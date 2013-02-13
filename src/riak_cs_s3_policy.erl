%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc policy utility functions

-module(riak_cs_s3_policy).

-behaviour(riak_cs_policy).

-include("riak_cs.hrl").
-include("s3_api.hrl").
-include_lib("webmachine/include/wm_reqdata.hrl").

%% Public API
-export([
         eval/2,
         check_policy/2,
         reqdata_to_access/3,
         policy_from_json/1,
         policy_to_json_term/1,
         supported_object_action/0,
         supported_bucket_action/0,
         log_supported_actions/0
        ]).

-ifdef(TEST).
-export([eval_all_ip_addr/2,
         eval_ip_address/2,
         eval_condition/2,
         eval_statement/2,
         my_split/4,
         parse_arns/1,
         print_arns/1,
         parse_ip/1,
         print_ip/1]).

-endif.

-type policy1() :: ?POLICY{}.

-export_type([policy1/0]).

-define(AMZ_DEFAULT_VERSION, <<"2008-10-17">>).

%% ===================================================================
%% Public API
%% ===================================================================

-spec eval(access(), policy() | undefined | binary() )-> boolean().
eval(_, undefined) -> undefined;
eval(Access, JSON) when is_binary(JSON) ->
    case policy_from_json(JSON) of
        {ok, Policy} ->  eval(Access, Policy);
        E -> E
    end;
eval(Access, ?POLICY{version=V, statement=Stmts}) ->
    case V of
        undefined ->        aggregate_evaluation(Access, Stmts);
        ?AMZ_DEFAULT_VERSION -> aggregate_evaluation(Access, Stmts);
        _ -> false
    end.

aggregate_evaluation(_, []) -> undefined;
aggregate_evaluation(Access, [Stmt|Stmts]) ->
    case eval_statement(Access, Stmt) of
        undefined -> aggregate_evaluation(Access, Stmts);
        true  -> true;
        false -> false
    end.


% @doc  semantic validation of policy
-spec check_policy(access(), policy()) -> ok | {error, atom()}.
check_policy(#access_v1{bucket=B} = _Access,
             Policy) ->
    
    case check_all_resources(B, Policy) of
        false -> {error, malformed_policy_resource};
        true ->
            case check_principals(Policy?POLICY.statement) of
                false -> {error, malformed_policy_principal};
                true -> 
                    case check_actions(Policy?POLICY.statement) of
                        false -> {error, malformed_policy_action};
                        true -> ok
                    end
            end
    end.

% @doc confirm if forbidden action included in policy
% s3:CreateBucket and s3:ListAllMyBuckets are prohibited at S3
check_actions([]) -> true;
check_actions([Stmt|Stmts]) ->
    case Stmt#statement.action of
        '*' -> check_actions(Stmts);
        Actions ->
            case not lists:member('s3:CreateBucket', Actions) of
                true ->
                    case not lists:member('s3:ListAllMyBuckets', Actions) of
                        true -> check_actions(Stmts);
                        false -> false
                    end;
                false -> false
            end
    end.

check_principals([]) -> false;
check_principals([Stmt|Stmts]) ->
    case check_principal(Stmt#statement.principal) of
        true -> true;
        false -> check_principals(Stmts)
    end.    

check_principal('*') ->
    true;
check_principal([]) ->
    false;
check_principal([{canonical_id, _Id}|_]) -> %% TODO: do we check all canonical ids exist?
    true;
check_principal([{aws, '*'}|_]) ->
    true;
check_principal([_|T]) ->
    check_principal(T).

% @doc check if the policy is set to proper bucket by checking arn
check_all_resources(BucketBin, ?POLICY{statement=Stmts} = _Policy) ->
    CheckFun = fun(Stmt) ->
                       check_all_resources(BucketBin, Stmt)
               end,
    lists:all(CheckFun, Stmts);
check_all_resources(BucketBin, #statement{resource=Resources} = _Stmt) ->
    CheckFun = fun(Resource) ->
                       check_all_resources(BucketBin, Resource)
               end,
    lists:all(CheckFun, Resources);
check_all_resources(BucketBin, #arn_v1{path=Path} = _Resource) ->
    [B|_] = string:tokens(Path, "/"),
    B =:= binary_to_list(BucketBin).

-spec reqdata_to_access(#wm_reqdata{}, Target::atom(), ID::binary()) -> {ok, policy()}.
reqdata_to_access(RD, Target, ID) ->
    Key = case wrq:path_info(object, RD) of
              undefined -> undefined;
              RawKey -> mochiweb_util:unquote(mochiweb_util:unquote(RawKey))
          end,
    #access_v1{
       method = wrq:method(RD), target = Target,
       id = ID, req = RD,
       bucket = list_to_binary(wrq:path_info(bucket, RD)),
       key    = Key
      }.

-spec policy_from_json(JSON::binary()) -> {ok, policy()} | {error, term()}.
policy_from_json(JSON)->
    case catch(mochijson2:decode(JSON)) of
        {struct, Pairs} ->
            Version = proplists:get_value(<<"Version">>, Pairs),
            ID      = proplists:get_value(<<"Id">>, Pairs),
            Stmts0  = proplists:get_value(<<"Statement">>, Pairs),
            Stmts = lists:map(fun({struct,S})->statement_from_pairs(S, #statement{}) end, Stmts0),
            {ok, ?POLICY{id=ID, version=Version, statement=Stmts}};
        {error, _Reason} = E ->
            E;
        {'EXIT', {{case_clause, B}, _}} when is_binary(B) -> %% mochiweb failed to parse the JSON
            {error, malformed_policy_json}
    end.

-spec policy_to_json_term(policy()) -> JSON::binary().
policy_to_json_term( ?POLICY{ version = V,
                                 id = ID, statement = Stmts0} ) when
      V =:= ?AMZ_DEFAULT_VERSION orelse V =:= undefined ->
    Stmts = lists:map(fun statement_to_pairs/1, Stmts0),
    % hope no unicode included
    Policy0 = [{"Id", ID}, {"Statement",Stmts}],
    Policy = case V of
                 undefined ->  Policy0;
                 ?AMZ_DEFAULT_VERSION -> [{"Version", V}|Policy0]
             end,
    list_to_binary(mochijson2:encode(Policy)).

-spec supported_object_action() -> [s3_object_action()].
supported_object_action() -> ?SUPPORTED_OBJECT_ACTION.

-spec supported_bucket_action() -> [s3_bucket_action()].
supported_bucket_action() -> ?SUPPORTED_BUCKET_ACTION.

% @doc put required atoms into atom table
% to make policy json parser safer by using erlang:binary_to_existing_atom/2.
-spec log_supported_actions() -> ok.
log_supported_actions()->
    _ = lager:info("supported object actions: ~p",
                   [lists:map(fun atom_to_list/1, supported_object_action())]),
    _ = lager:info("supported bucket actions: ~p",
                   [lists:map(fun atom_to_list/1, supported_bucket_action())]),
    ok.

%% ===================================================================
%% internal API

-spec resource_matches(binary(), binary(), #statement{}) -> boolean().
resource_matches(_, _, #statement{resource='*'} = _Stmt ) -> true;
resource_matches(BucketBin, KeyBin, #statement{resource=Resources}) ->
    Bucket = binary_to_list(BucketBin),
    % @TODO in case of using unicode object keys
    Path0 = case KeyBin of
                undefined -> Bucket;
                Key when is_list(KeyBin) ->
                    binary_to_list(BucketBin) ++ "/" ++ Key;
                _ when is_binary(KeyBin) ->
                    binary_to_list(<<BucketBin/binary, "/", KeyBin/binary>>)
            end,
    lists:any(fun(#arn_v1{path="*"}) ->    true;
                 (#arn_v1{path=Path}) ->
                      case Path of
                          Bucket -> true;

                          %% only prefix matching
                          Path ->
                              [B|_] = string:tokens(Path, "*"),
                              Len = length(B),
                              B =:= string:substr(Path0, 1, Len)
                      end;
                 (_) -> false
              end, Resources).
    


% functions to eval:
-spec eval_statement(access(), policy()) -> boolean() | undefined.
eval_statement(#access_v1{method=M, target=T, req=Req, bucket=B, key=K} = _Access,
               #statement{effect=E, condition_block=Conds, action=As} = Stmt) ->
    {ok, A} = make_action(M, T),
    ResourceMatch = resource_matches(B, K, Stmt),
    IsRelated = lists:member(A, As) orelse lists:member('*', As),
    case {IsRelated, ResourceMatch} of
        {false, _} -> undefined;
        {_, false} -> undefined;
        {true, true} ->
            Match = lists:all(fun(Cond) -> eval_condition(Req, Cond) end, Conds),
            case E of
                allow -> Match;
                deny -> not Match % @TODO: add test cases for Deny
            end
    end.

make_action(Method, Target) ->
    case {Method, Target} of
        {'PUT', object} ->     {ok, 's3:PutObject'};
        {'PUT', object_acl} -> {ok, 's3:PutObjectAcl'};
        {'PUT', bucket} ->
            {ok, 's3:CreateBucket'}; % 400 (MalformedPolicy): Policy has invalid action
        {'PUT', bucket_acl} -> {ok, 's3:PutBucketAcl'};
        {'PUT', bucket_policy} -> {ok, 's3:PutBucketPolicy'};
        
        {'GET', object} ->     {ok, 's3:GetObject'};
        {'GET', object_acl} -> {ok, 's3:GetObjectAcl'};
        {'GET', bucket} ->     {ok, 's3:ListBucket'};
        {'GET', no_bucket } -> {ok, 's3:ListAllMyBuckets'};
        {'GET', bucket_acl} -> {ok, 's3:GetBucketAcl'};
        {'GET', bucket_policy} -> {ok, 's3:GetBucketPolicy'};
        {'GET', bucket_location} -> {ok, 's3:GetBucketLocation'};
        
        {'DELETE', object} ->  {ok, 's3:DeleteObject'};
        {'DELETE', bucket} ->  {ok, 's3:DeleteBucket'};
        {'DELETE', bucket_policy} -> {ok, 's3:DeleteBucketPolicy'};
        
        {'HEAD', object} -> {ok, 's3:GetObject'}; % no HeadObject

        {'HEAD', _} ->
            {error, no_action}
    end.

eval_condition(Req, {AtomKey, Cond}) ->
    case AtomKey of
        'StringEquals' -> eval_string_eq(Req, Cond);
        streq          -> eval_string_eq(Req, Cond);
        'StringNotEquals' -> not eval_string_eq(Req, Cond);
        strneq            -> not eval_string_eq(Req, Cond);
        'StringEqualsIgnoreCase' -> eval_string_eq_ignore_case(Req, Cond);
        streqi                   -> eval_string_eq_ignore_case(Req, Cond);
        'StringNotEqualsIgnoreCase' -> not eval_string_eq_ignore_case(Req, Cond);
        streqni                     -> not eval_string_eq_ignore_case(Req, Cond);
        'StringLike' -> eval_string_like(Req, Cond);
        strl         -> eval_string_like(Req, Cond);
        'StringNotLike' -> not eval_string_like(Req, Cond);
        strnl  -> not eval_string_like(Req, Cond);

        'NumericEquals' -> eval_numeric_eq(Req, Cond);
        numeq           -> eval_numeric_eq(Req, Cond);
        'NumericNotEquals' -> not eval_numeric_eq(Req, Cond);
        numneq             -> not eval_numeric_eq(Req, Cond);
        'NumericLessThan'  -> eval_numeric_lt(Req, Cond);
        numlt              -> eval_numeric_lt(Req, Cond);
        'NumericLessThanEquals' -> eval_numeric_lte(Req, Cond);
        numlteq                 -> eval_numeric_lte(Req, Cond);
        'NumericGreaterThan' -> not eval_numeric_lte(Req, Cond);
        numgt                -> not eval_numeric_lte(Req, Cond);
        'NumericGreaterThanEquals' -> not eval_numeric_lt(Req, Cond);
        numgteq                    -> not eval_numeric_lt(Req, Cond);

        'DateEquals' -> eval_date_eq(Req, Cond);
        dateeq       -> eval_date_eq(Req, Cond);
        'DateNotEquals' -> not eval_date_eq(Req, Cond);
        dateneq         -> not eval_date_eq(Req, Cond);
        'DateLessThan' -> eval_date_lt(Req, Cond);
        datelt         -> eval_date_lt(Req, Cond);
        'DateLessThanEquals' -> eval_date_lte(Req, Cond);
        datelteq             -> eval_date_lte(Req, Cond);
        'DateGreaterThan' -> not eval_date_lte(Req, Cond);
        dategt            -> not eval_date_lte(Req, Cond);
        'DateGreaterThanEquals' -> not eval_date_lt(Req, Cond);
        dategteq                -> not eval_date_lt(Req, Cond);

        'Bool' -> eval_bool(Req, Cond);

        'IpAddress' ->    eval_ip_address(Req, Cond);
        'NotIpAddress' -> not eval_ip_address(Req, Cond)
    end.

-spec eval_string_eq(#wm_reqdata{}, [{'aws:UserAgent', binary()}] | [{'aws:Referer', binary()}] )-> boolean().
eval_string_eq(Req, Conds)->
    UA2be      = proplists:get_value('aws:UserAgent', Conds),
    Referer2be = proplists:get_value('aws:Referer', Conds),
    UA      = mochiweb_headers:get_value("User-Agent", Req#wm_reqdata.req_headers),
    Referer = mochiweb_headers:get_value("Referer", Req#wm_reqdata.req_headers),
    case {UA2be, Referer2be} of
        {undefined, undefined} -> false;
        {_, undefined} ->  (UA =:= UA2be);
        {undefined, _} ->  (Referer =:= Referer2be);
        _ -> (UA =:= UA2be) and (Referer =:= Referer2be)
    end.

eval_string_eq_ignore_case(_, _) -> throw(not_supported).
eval_string_like(_, _) -> throw(not_supported).

eval_numeric_eq(_, _) -> throw(not_supported).
eval_numeric_lt(_, _) -> throw(not_supported).
eval_numeric_lte(_, _) -> throw(not_supported).

eval_date_eq(_, _) -> throw(not_supported).
eval_date_lt(_, _) -> throw(not_supported).
eval_date_lte(_, _) -> throw(not_supported).

-spec eval_bool(#wm_reqdata{}, [{'aws:SecureTransport', boolean()}]) -> boolean().
eval_bool(_Req, []) ->  undefined;

eval_bool(#wm_reqdata{scheme=Scheme} = _Req, Conds) ->
    {'aws:SecureTransport', Boolean} = lists:last(Conds),
    case Scheme of
        https -> Boolean;
        http  -> not Boolean
    end.

-spec eval_ip_address(#wm_reqdata{}, [{'aws:SourceIp', binary()}]) -> boolean().
eval_ip_address(Req, Conds) ->
    {Peer, _} = parse_ip(Req#wm_reqdata.peer),
    IPConds = [ IPCond || {'aws:SourceIp', IPCond} <- Conds ],
    eval_all_ip_addr(IPConds, Peer).

eval_all_ip_addr([], _) -> false;
eval_all_ip_addr([{IP,Prefix}|T], Peer) ->
    case ipv4_eq(ipv4_band(IP, Prefix), ipv4_band(Peer, Prefix)) of
        true ->  true;
        false -> eval_all_ip_addr(T, Peer)
    end.

ipv4_band({A,B,C,D}, {E,F,G,H}) ->
    {A band E, B band F, C band G, D band H}.

ipv4_eq({A,B,C,D}, {E,F,G,H}) ->
    (A =:= E) andalso (B =:= F) andalso (C =:= G) andalso (D =:= H).

-type json_term() :: [{binary(), json_term()}] | integer() | float() | binary().

% ===========================================
% functions to convert policy record to JSON:

-spec statement_to_pairs(#statement{}) -> [{binary(), json_term()}].
statement_to_pairs(#statement{sid=Sid, effect=E, principal=P, action=A,
                              not_action=NA, resource=R, condition_block=Cs})->
    AtomE = case E of
                allow -> <<"Allow">>;
                deny  -> <<"Deny">>
            end,
    Conds = lists:map(fun condition_block_from_condition_pair/1, Cs),
    [{<<"Sid">>, Sid}, {<<"Effect">>, AtomE},
     {<<"Principal">>, print_principal(P)},
     {<<"Action">>, A}, {<<"NotAction">>, NA},
     {<<"Resource">>, print_arns(R)},
     {<<"Condition">>, Conds}].


-spec condition_block_from_condition_pair(condition_pair()) -> {binary(), json_term()}.
condition_block_from_condition_pair({AtomKey, Conds})->
    Fun = fun({'aws:SourceIp', IP}) -> {'aws:SourceIp', print_ip(IP)};
             (Cond) -> Cond
          end,
    {atom_to_binary(AtomKey, latin1),  lists:map(Fun, Conds)}.

% inverse of parse_ip/1
-spec print_ip({inet:ip_address(), inet:ip_address()}) -> binary().
print_ip({IP, Mask}) ->
    IPBin = list_to_binary(inet_parse:ntoa(IP)),
    case mask_to_prefix(Mask) of
        32 -> IPBin;
        I -> 
            Str = integer_to_list(I),
            <<IPBin/binary, <<"/">>/binary, (list_to_binary(Str))/binary>>
    end.

% {255,255,255,0} -> 24
mask_to_prefix({A,B,C,D})->
    case int_to_prefix(A) of
        8 ->
            case int_to_prefix(B) of
                0 -> 8;
                8 ->
                    case int_to_prefix(C) of
                        0 -> 16;
                        8 -> 24 + int_to_prefix(D);
                        I -> 16 + I
                    end;
                I -> 8 + I
            end;
        I -> I
    end.

int_to_prefix(2#11111111) -> 8;
int_to_prefix(2#11111110) -> 7;
int_to_prefix(2#11111100) -> 6;
int_to_prefix(2#11111000) -> 5;
int_to_prefix(2#11110000) -> 4;
int_to_prefix(2#11100000) -> 3;
int_to_prefix(2#11000000) -> 2;
int_to_prefix(2#10000000) -> 1;
int_to_prefix(0) -> 0.


% ===========================================================
% functions to convert (parse) JSON to create a policy record:

-spec statement_from_pairs(json_term(), #statement{})-> #statement{}.
statement_from_pairs([], Stmt) -> Stmt;
statement_from_pairs([{<<"Sid">>,Sid}      |T], Stmt) ->
    statement_from_pairs(T, Stmt#statement{sid=Sid});

statement_from_pairs([{<<"Effect">>,<<"Allow">>}|T], Stmt) ->
    statement_from_pairs(T, Stmt#statement{effect=allow});
statement_from_pairs([{<<"Effect">>,<<"Deny">>}|T], Stmt) ->
    statement_from_pairs(T, Stmt#statement{effect=deny});

statement_from_pairs([{<<"Principal">>,P}  |T], Stmt) ->
    Principal = parse_principal(P),
    statement_from_pairs(T, Stmt#statement{principal=Principal});

statement_from_pairs([{<<"Action">>,As}    |T], Stmt) ->
    Atoms = lists:map(fun binary_to_action/1, As),
    statement_from_pairs(T, Stmt#statement{action=Atoms});
statement_from_pairs([{<<"NotAction">>,As}  |T], Stmt) ->
    Atoms = lists:map(fun binary_to_action/1, As),
    statement_from_pairs(T, Stmt#statement{not_action=Atoms});

statement_from_pairs([{<<"Resource">>,R}   |T], Stmt) ->
    {ok, ARN} = parse_arns(R),
    statement_from_pairs(T, Stmt#statement{resource=ARN});

statement_from_pairs([{<<"Condition">>,{struct, Cs}}  |T], Stmt) ->
    Conditions = lists:map(fun condition_block_to_condition_pair/1, Cs),
    statement_from_pairs(T, Stmt#statement{condition_block=Conditions}).

-spec binary_to_action(binary()) -> s3_object_action() | s3_bucket_action().
binary_to_action(Bin)->
    binary_to_existing_atom(Bin, latin1).

% @TODO: error processing
-spec parse_principal(binary() | [binary()]) -> principal().
parse_principal(<<"*">>) -> '*';
parse_principal({struct, List}) when is_list(List) ->
    parse_principals(List, []).


parse_principals([], Principal) -> Principal;
parse_principals([{<<"AWS">>,[<<"*">>]}|TL], Principal) ->
    parse_principals(TL, [{aws, '*'}|Principal]);
parse_principals([{<<"AWS">>,<<"*">>}|TL], Principal) ->
    parse_principals(TL, [{aws, '*'}|Principal]).
%% TODO: CanonicalUser as principal is not yet supported,
%%  Because to use at Riak CS, key_id is better to specify user, because
%%  getting canonical ID from Riak is not enough efficient
%% parse_principals([{<<"CanonicalUser">>,CanonicalIds}|TL], Principal) ->
%%     case CanonicalIds of
%%         [H|_] when is_binary(H) ->
%%             %% in case of list of strings ["CAFEBABE...", "BA6DAAD..."]
%%             CanonicalUsers = lists:map(fun(CanonicalId) ->
%%                                                {canonical_id, binary_to_list(CanonicalId)}
%%                                        end,
%%                                        CanonicalIds),
%%             parse_principals(TL, CanonicalUsers ++ Principal);
%%         CanonicalId when is_binary(CanonicalId) ->
%%             %% in case of just a string ["CAFEBABE..."]
%%             parse_principals(TL, [{canonical_id, binary_to_list(CanonicalId)}|Principal])
%%     end.

print_principal('*') -> <<"*">>;
print_principal(List) ->
    PrintFun = fun({aws, '*'}) ->
                       {"AWS", <<"*">>};
                  ({canonical_id, Id}) ->
                       {"CanonicalUser", Id}
               end,
    lists:map(PrintFun, List).

-spec parse_arns(binary()|[binary()]) -> {ok, arn()} | {error, term()}.
parse_arns(<<"*">>) -> {ok, '*'};
parse_arns(Bin) when is_binary(Bin) ->
    Str = binary_to_list(Bin),
    case parse_arn(Str) of
        {ok, ARN} -> {ok, [ARN]};
        {error, _} = E -> E
    end;
parse_arns(List) when is_list(List) ->
    AccFun = fun(ARNBin, {ok, Acc0}) ->
                     case parse_arns(ARNBin) of
                         {ok, ARN} -> {ok, ARN ++ Acc0};
                         {error, bad_arn} -> {error, bad_arn}
                     end;
                ({error, bad_arn}, _)   ->  {error, bad_arn}
             end,
    case lists:foldl(AccFun, {ok, []}, List) of
        {ok, ARNs} -> {ok, lists:reverse(ARNs)};
        Error -> Error
    end.

parse_arn(Str) ->
    case my_split($:, Str, [], []) of
        ["arn", "aws", "s3", Region, ID, Path] ->
            {ok, #arn_v1{provider = aws,
                         service  = s3,
                         region   = Region,
                         id       = ID,
                         path     = Path}};
        _ ->
            {error, bad_arn}
    end.

-spec my_split(char(), string(), string(), [string()]) -> [string()].
my_split(_, [], [], L) -> lists:reverse(L);
my_split(Ch, [], Acc, L) ->
    my_split(Ch, [], [], [ lists:reverse(Acc) | L ]);
my_split(Ch, [Ch|TL], Acc, L) ->
    my_split(Ch, TL, [], [ lists:reverse(Acc) | L ]);
my_split(Ch, [Ch0|TL], Acc, L) ->
    my_split(Ch, TL, [Ch0|Acc], L).

-spec print_arns( '*'|[arn()]) -> [binary()] | binary().
print_arns('*') -> <<"*">>;
print_arns(ARNs) ->
    PrintARN =
        fun(#arn_v1{region=R, id=ID, path=Path} = _ARN) ->
                list_to_binary(string:join(["arn", "aws", "s3", R, ID, Path], ":"))
        end,
    lists:map(PrintARN, ARNs).

-spec condition_block_to_condition_pair({binary(), json_term()}) -> condition_pair().
condition_block_to_condition_pair({Key,{struct,Cond}}) ->
    % all key should be defined in stanchion.hrl
    AtomKey = binary_to_existing_atom(Key, latin1),
    {AtomKey, lists:map(fun condition_/1, Cond)}.

% TODO: more strict condition - currenttime only for date_condition, and so on
condition_({<<"aws:CurrentTime">>, Bin}) when is_binary(Bin) ->
    {'aws:CurrentTime', Bin};
condition_({<<"aws:EpochTime">>, Int}) when is_integer(Int) andalso Int >= 0 ->
    {'aws:EpochTime', Int};
condition_({<<"aws:SecureTransport">>, MaybeBool}) ->
    %% TODO: if this doesn't match return code like malformed condition -
    %% while AWS never returns error in failing. Needs decision.
    {ok, Bool} = parse_bool(MaybeBool),
    {'aws:SecureTransport', Bool};
condition_({<<"aws:SourceIp">>, Bin}) when is_binary(Bin)->
    IP = parse_ip(Bin),
    {'aws:SourceIp', IP};
condition_({<<"aws:UserAgent">>, Bin}) -> % TODO: check string condition
    {'aws:UserAgent', Bin};
condition_({<<"aws:Referer">>, Bin}) -> % TODO: check string condition
    {'aws:Referer', Bin}.


%% how S3 works:
%% "true" => true
%% "TRUE" => true
%% "True" => true
%% "tRUe" => true
%% "true   " => false
%% "trueboo" => false
%% "true boo" => false
%% "false" => false
%% "falsetrue" => false
%% "0" => false
%% "1" => false
%% "oopaa" => false
%% [] => []
%% ["true"] => true
%% ["false", "true"] => [false,true]
%% ["true", "false"] => [false,true]
%% ["true", "false", "true"] => [false,true]
%% [[[[[[["true"]]]]]]]  => true
%% [[[[[[["true"], "false"]]]]]] => [false, true] => evaluaged as true
%% To be honest, we can't imitate S3 any more.
-spec parse_bool(term()) -> {ok, boolean()} | {error, notbool}.
parse_bool(Bool) when is_boolean(Bool) -> {ok, Bool};
parse_bool(<<"true">>)  -> {ok, true};
parse_bool(<<"false">>) -> {ok, false};
parse_bool(MaybeBool) when is_binary(MaybeBool) ->
    case string:to_lower(binary_to_list(MaybeBool)) of
        "true" ->  {ok, true};
        "false" -> {ok, false};
        _ ->       {error, notbool}
    end;
parse_bool(_) -> {error, notbool}.

% TODO: IPv6
% <<"10.1.2.3/24">> -> {{10,1,2,3}, {255,255,255,0}}
% "10.1.2.3/24 -> {{10,1,2,3}, {255,255,255,0}}
-spec parse_ip(binary() | string()) -> {inet:ip_address(), inet:ip_address()}.
parse_ip(Bin) when is_binary(Bin) ->
    Str = binary_to_list(Bin),
    parse_ip(Str);
parse_ip(Str) when is_list(Str) ->
    case inet_parse:ipv4_address(Str) of
        {error, _} ->
            [IPStr, Bits] = string:tokens(Str, "/"),
            Prefix0 = (16#FFFFFFFF bsl (32-list_to_integer(Bits))) band 16#FFFFFFFF,
            Prefix = { Prefix0 band 16#FF000000 bsr 24,
                       Prefix0 band 16#FF0000 bsr 16,
                       Prefix0 band 16#FF00 bsr 8,
                       Prefix0 band 16#FF },
            {ok, IP} =inet_parse:ipv4_address(IPStr),
            {IP, Prefix};
        {ok, IP} -> {IP, {255,255,255,255}}
    end;
parse_ip(T) when is_tuple(T)-> T.
