%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% ---------------------------------------------------------------------

%% @doc policy utility functions
%% A (principal) is/isn't allowed (effect) to do B (action) to C (resource) where D (condition) applies

-module(riak_cs_s3_policy).

-behaviour(riak_cs_policy).

-include("riak_cs.hrl").
-include("s3_api.hrl").
-include_lib("webmachine/include/wm_reqdata.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").

%% Public API
-export([
         bucket_policy/2,
         bucket_policy_from_contents/2,
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

-include_lib("eunit/include/eunit.hrl").
-export([eval_all_ip_addr/2,
         eval_ip_address/2,
         eval_condition/2,
         eval_statement/2,
         my_split/4,
         parse_arns/1,
         print_arns/1,
         parse_ip/1,
         print_ip/1,

         statement_eq/2  %% for test use
        ]).

-endif.

-type policy1() :: ?POLICY{}.

-export_type([policy1/0]).

-define(AMZ_DEFAULT_VERSION, <<"2008-10-17">>).
-define(POLICY_UNDEF, {error, policy_undefined}).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc evaluates the policy and returns the policy allows, denies or
%% not says nothing about this access. Usually in case of undefined,
%% the owner access must be accepted and others must be refused.
-spec eval(access(), policy1() | undefined | binary() ) -> boolean() | undefined.
eval(_, undefined) -> undefined;
eval(Access, JSON) when is_binary(JSON) ->
    case policy_from_json(JSON) of
        {ok, Policy} ->  eval(Access, Policy);
        {error, _} = E -> E
    end;
eval(Access, ?POLICY{version=V, statement=Stmts}) ->
    case V of
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
-spec check_policy(access(), policy1()) -> ok | {error, atom()}.
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
-spec check_actions([#statement{}]) -> boolean().
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

-spec check_principals([#statement{}]) -> boolean().
check_principals([]) -> false;
check_principals([Stmt|Stmts]) ->
    case check_principal(Stmt#statement.principal) of
        true -> true;
        false -> check_principals(Stmts)
    end.

-spec check_principal(any()) -> boolean().
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

-spec reqdata_to_access(#wm_reqdata{}, Target::atom(), ID::binary()|undefined) -> access().
reqdata_to_access(RD, Target, ID) ->
    Key = case wrq:path_info(object, RD) of
              undefined -> undefined;
              RawKey -> list_to_binary(mochiweb_util:unquote(mochiweb_util:unquote(RawKey)))
          end,
    #access_v1{
            method = wrq:method(RD),
            target = Target,
            id = ID, req = RD,
            bucket = list_to_binary(wrq:path_info(bucket, RD)),
            key    = Key
           }.

-spec policy_from_json(JSON::binary()) -> {ok, policy1()} | {error, term()}.
policy_from_json(JSON) ->
    %% TODO: stop using exception and start some monadic validation and parsing.
    case catch(mochijson2:decode(JSON)) of
        {struct, Pairs} ->
            Version = proplists:get_value(<<"Version">>, Pairs),
            ID      = proplists:get_value(<<"Id">>, Pairs),
            Stmts0  = proplists:get_value(<<"Statement">>, Pairs),

            case catch(lists:map(fun({struct,S})->
                                         statement_from_pairs(S, #statement{})
                                 end, Stmts0)) of

                      {error, _Reason} = E ->
                         E;
                      [] ->
                         {error, malformed_policy_missing};

                      Stmts ->
                         case {Version, ID} of
                             {undefined, <<"undefined">>} ->
                                 {ok, ?POLICY{statement=Stmts}};
                             {undefined, _} ->
                                 {ok, ?POLICY{id=ID, statement=Stmts}};
                             {_, <<"undefined">>} ->
                                 {ok, ?POLICY{version=Version, statement=Stmts}};
                             _ ->
                                 {ok, ?POLICY{id=ID, version=Version, statement=Stmts}}
                         end
                 end;
        {error, _Reason} = E ->
            E;
        {'EXIT', {{case_clause, B}, _}} when is_binary(B) ->
            %% mochiweb failed to parse the JSON
            _ = lager:debug("~p", [B]),
            {error, malformed_policy_json}
    end.

-spec policy_to_json_term(policy1()) -> JSON::binary().
policy_to_json_term( ?POLICY{ version = ?AMZ_DEFAULT_VERSION,
                              id = ID, statement = Stmts0}) ->
    Stmts = lists:map(fun statement_to_pairs/1, Stmts0),
    % hope no unicode included
    Policy = [{"Version",?AMZ_DEFAULT_VERSION},{"Id", ID},{"Statement",Stmts}],
    unicode:characters_to_binary(mochijson2:encode(Policy), unicode).


-spec supported_object_action() -> [s3_object_action()].
supported_object_action() -> ?SUPPORTED_OBJECT_ACTION.

-spec supported_bucket_action() -> [s3_bucket_action()].
supported_bucket_action() -> ?SUPPORTED_BUCKET_ACTION.

%% @doc put required atoms into atom table
%% to make policy json parser safer by using erlang:binary_to_existing_atom/2.
-spec log_supported_actions() -> ok.
log_supported_actions()->
    _ = lager:info("supported object actions: ~p",
                   [lists:map(fun atom_to_list/1, supported_object_action())]),
    _ = lager:info("supported bucket actions: ~p",
                   [lists:map(fun atom_to_list/1, supported_bucket_action())]),
    ok.

%% @doc Get the policy for a bucket
-type policy_from_meta_result() :: {'ok', policy()} | {'error', 'policy_undefined'}.
-type bucket_policy_result() :: policy_from_meta_result() |
                                {'error', 'notfound'} |
                                {'error', 'multiple_bucket_owners'}.
-spec bucket_policy(binary(), pid()) -> bucket_policy_result().
bucket_policy(Bucket, RiakPid) ->
    case riak_cs_utils:check_bucket_exists(Bucket, RiakPid) of
        {ok, Obj} ->
            %% For buckets there should not be siblings, but in rare
            %% cases it may happen so check for them and attempt to
            %% resolve if possible.
            Contents = riakc_obj:get_contents(Obj),
            bucket_policy_from_contents(Bucket, Contents);
        {error, Reason} ->
            _ = lager:debug("Failed to fetch policy. Bucket ~p "
                            " does not exist. Reason: ~p",
                            [Bucket, Reason]),
            {error, notfound}
    end.

%% @doc Attempt to resolve a policy for the bucket based on the contents.
%% We attempt resolution, but intentionally do not write back a resolved
%% value. Instead the fact that the bucket has siblings is logged, but the
%% condition should be rare so we avoid updating the value at this time.
-spec bucket_policy_from_contents(binary(), riakc_obj:contents()) ->
                                         bucket_policy_result().
bucket_policy_from_contents(_, [{MD, _}]) ->
    MetaVals = dict:fetch(?MD_USERMETA, MD),
    policy_from_meta(MetaVals);
bucket_policy_from_contents(Bucket, Contents) ->
    {Metas, Vals} = lists:unzip(Contents),
    UniqueVals = lists:usort(Vals),
    UserMetas = [dict:fetch(?MD_USERMETA, MD) || MD <- Metas],
    riak_cs_utils:maybe_log_bucket_owner_error(Bucket, UniqueVals),
    resolve_bucket_metadata(UserMetas, UniqueVals).

-spec resolve_bucket_metadata(list(riakc_obj:metadata()),
                               list(riakc_obj:value())) -> bucket_policy_result().
resolve_bucket_metadata(Metas, [_Val]) ->
    Policies = [policy_from_meta(M) || M <- Metas],
    resolve_bucket_policies(Policies);
resolve_bucket_metadata(_Metas, _) ->
    {error, multiple_bucket_owners}.

-spec resolve_bucket_policies(list(policy_from_meta_result())) -> policy_from_meta_result().
resolve_bucket_policies([Policy]) ->
    Policy;
resolve_bucket_policies(Policies) ->
    lists:foldl(fun newer_policy/2, ?POLICY_UNDEF, Policies).

-spec newer_policy(policy_from_meta_result(), policy_from_meta_result()) ->
                       policy_from_meta_result().
newer_policy(Policy1, ?POLICY_UNDEF) ->
    Policy1;
newer_policy({ok, Policy1}, {ok, Policy2})
  when Policy1?POLICY.creation_time >= Policy2?POLICY.creation_time ->
    {ok, Policy1};
newer_policy(_, Policy2) ->
    Policy2.

%% @doc Find the policy in a list of metadata values and
%% convert it to an erlang term representation. Return
%% an error tuple if a policy is not found.
-spec policy_from_meta([{string(), term()}]) -> policy_from_meta_result().
policy_from_meta([]) ->
    ?POLICY_UNDEF;
policy_from_meta([{?MD_POLICY, Policy} | _]) ->
    {ok, binary_to_term(Policy)};
policy_from_meta([_ | RestMD]) ->
    policy_from_meta(RestMD).

%% ===================================================================
%% internal API

-spec resource_matches(binary(), binary()|undefined|list(), #statement{}) -> boolean().
resource_matches(_, _, #statement{resource='*'} = _Stmt ) -> true;
resource_matches(BucketBin, KeyBin, #statement{resource=Resources})
  when KeyBin =:= undefined orelse is_binary(KeyBin) ->
    Bucket = binary_to_list(BucketBin),
    % @TODO in case of using unicode object keys
    Path0 = case KeyBin of
                undefined ->
                    Bucket;
                _ when is_binary(KeyBin) ->
                    unicode:characters_to_list(<<BucketBin/binary, "/", KeyBin/binary>>, unicode)
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
-spec eval_statement(access(), #statement{}) -> boolean() | undefined.
eval_statement(#access_v1{method=M, target=T, req=Req, bucket=B, key=K} = _Access,
               #statement{effect=E, condition_block=Conds, action=As} = Stmt) ->
    {ok, A} = make_action(M, T),
    ResourceMatch = resource_matches(B, K, Stmt),
    IsRelated = (As =:= '*')
        orelse (A =:= As)
        orelse (is_list(As) andalso lists:member(A, As)),
    case {IsRelated, ResourceMatch} of
        {false, _} -> undefined;
        {_, false} -> undefined;
        {true, true} ->
            Match = lists:all(fun(Cond) -> eval_condition(Req, Cond) end, Conds),
            case {E, Match} of
                {allow, true} -> true;
                {deny, true} -> false;
                {_, false} -> undefined %% matches nothing
            end
    end.

make_action(Method, Target) ->
    case {Method, Target} of
        {'PUT', object} ->     {ok, 's3:PutObject'};
        {'PUT', object_part} -> {ok, 's3:PutObject'};
        {'PUT', object_acl} -> {ok, 's3:PutObjectAcl'};
        {'PUT', bucket_acl} -> {ok, 's3:PutBucketAcl'};
        {'PUT', bucket_policy} -> {ok, 's3:PutBucketPolicy'};

        {'GET', object} ->     {ok, 's3:GetObject'};
        {'GET', object_part} -> {ok, 's3:ListMultipartUploadParts'};
        {'GET', object_acl} -> {ok, 's3:GetObjectAcl'};
        {'GET', bucket} ->     {ok, 's3:ListBucket'};
        {'GET', no_bucket } -> {ok, 's3:ListAllMyBuckets'};
        {'GET', bucket_acl} -> {ok, 's3:GetBucketAcl'};
        {'GET', bucket_policy} -> {ok, 's3:GetBucketPolicy'};
        {'GET', bucket_location} -> {ok, 's3:GetBucketLocation'};
        {'GET', bucket_uploads} -> {ok, 's3:ListBucketMultipartUploads'};

        {'DELETE', object} ->  {ok, 's3:DeleteObject'};
        {'DELETE', object_part} -> {ok, 's3:AbortMultipartUpload'};
        {'DELETE', bucket} ->  {ok, 's3:DeleteBucket'};
        {'DELETE', bucket_policy} -> {ok, 's3:DeleteBucketPolicy'};

        {'HEAD', object} -> {ok, 's3:GetObject'}; % no HeadObject

        %% PUT Object includes POST Object,
        %% including Initiate Multipart Upload, Upload Part, Complete Multipart Upload
        {'POST', object} -> {ok, 's3:PutObject'};
        {'POST', object_part} -> {ok, 's3:PutObject'};

        %% same as {'GET' bucket}
        {'HEAD', bucket} -> {ok, 's3:ListBucket'};

        %% 400 (MalformedPolicy): Policy has invalid action
        {'PUT', bucket} ->  {ok, 's3:CreateBucket'};

        {'HEAD', _} -> {error, no_action}
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
        strneqi                     -> not eval_string_eq_ignore_case(Req, Cond);
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
    case parse_ip(Req#wm_reqdata.peer) of
        {error, _} ->
            false;
        {Peer, _} ->
            IPConds = [ IPCond || {'aws:SourceIp', IPCond} <- Conds ],
            eval_all_ip_addr(IPConds, Peer)
    end.

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

-spec statement_to_pairs(#statement{}) -> [{binary(), any()}].
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

-spec condition_block_from_condition_pair(condition_pair()) -> {binary(), list()}.
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

-spec statement_from_pairs(list(), #statement{})-> #statement{}.
statement_from_pairs([], Stmt) ->
    case Stmt#statement.principal of
        [] ->
            %% TODO: there're a lot to do: S3 describes the
            %% details of error, in xml. with <Code>, <Message> and <Detail>
            throw({error, malformed_policy_missing});
        _ ->
            Stmt
    end;

statement_from_pairs([{<<"Sid">>, <<>>}      |_], _) ->
    throw({error, malformed_policy_resource});

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
    Atoms = case As of
                As when is_list(As) ->
                    lists:map(fun binary_to_action/1, As);
                Bin when is_binary(Bin) ->
                    binary_to_existing_atom(Bin, latin1)
            end,
    statement_from_pairs(T, Stmt#statement{action=Atoms});
statement_from_pairs([{<<"NotAction">>,As}  |T], Stmt) when is_list(As) ->
    Atoms = lists:map(fun binary_to_action/1, As),
    statement_from_pairs(T, Stmt#statement{not_action=Atoms});

statement_from_pairs([{<<"Resource">>,R}   |T], Stmt) ->
    case parse_arns(R) of
        {ok, ARN} ->
            statement_from_pairs(T, Stmt#statement{resource=ARN});
        {error, _} ->
            throw({error, malformed_policy_resource})
    end;

statement_from_pairs([{<<"Condition">>,[]}  |T], Stmt) ->
    %% empty conditions
    statement_from_pairs(T, Stmt#statement{condition_block=[]});

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
    parse_principals(List, []);
parse_principal([{struct, List}]) when is_list(List) ->
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
print_principal({aws, '*'}) ->
    [{<<"AWS">>, <<"*">>}];
%%print_principal({canonical_id, Id}) ->
%%    {"CanonicalUser", Id};
print_principal(Principals) when is_list(Principals) ->
    PrintFun = fun(Principal) -> print_principal(Principal) end,
    lists:map(PrintFun, Principals).

-spec parse_arns(binary()|[binary()]) -> {ok, arn()} | {error, bad_arn}.
parse_arns(<<"*">>) -> {ok, '*'};
parse_arns(Bin) when is_binary(Bin) ->
    Str = binary_to_list(Bin),
    case parse_arn(Str) of
        {ok, ARN} -> {ok, [ARN]};
        {error, bad_arn} = E -> E
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

-spec parse_arn(string()) -> {ok, arn()} | {error, bad_arn}.
parse_arn(Str) ->
    case my_split($:, Str, [], []) of
        ["arn", "aws", "s3", Region, ID, Path] ->
            {ok, #arn_v1{provider = aws,
                         service  = s3,
                         region   = Region,
                         id       = list_to_binary(ID),
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
print_arns(#arn_v1{region=R, id=ID, path=Path} = _ARN) ->
    StringPath = unicode:characters_to_list(Path),
    StringID   = binary_to_list(ID),
    list_to_binary(string:join(["arn", "aws", "s3", R, StringID, StringPath], ":"));

print_arns(ARNs) when is_list(ARNs)->
    PrintARN = fun(ARN) -> print_arns(ARN) end,
    lists:map(PrintARN, ARNs).

-spec condition_block_to_condition_pair({binary(), {struct, json_term()}}) -> condition_pair().
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
    case parse_bool(MaybeBool) of
        {error, _} ->
            throw({error, malformed_policy_condition});
        {ok, Bool} ->
            {'aws:SecureTransport', Bool}
    end;
condition_({<<"aws:SourceIp">>, Bin}) when is_binary(Bin)->
    case parse_ip(Bin) of
        {error, _} ->
            throw({error, malformed_policy_condition});
        IP ->
            {'aws:SourceIp', IP}
    end;
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
%% NOTE: Returns false on a bad ip
-spec parse_ip(binary() | string()) -> {inet:ip_address(), inet:ip_address()} | {error, term()}.
parse_ip(Bin) when is_binary(Bin) ->
    Str = binary_to_list(Bin),
    parse_ip(Str);
parse_ip(Str) when is_list(Str) ->
    {IPStr, Netmask} = parse_tokenized_ip(string:tokens(Str, "/")),
    case inet_parse:ipv4strict_address(IPStr) of
        {ok, IP} ->
            {IP, Netmask};
        Error ->
            Error
    end.

-spec parse_tokenized_ip([string()]) -> {string(), inet:ip_address()}.
parse_tokenized_ip([IP]) ->
    {IP, {255, 255, 255, 255}};
parse_tokenized_ip([IP, PrefixSize]) ->
    Bits = list_to_integer(lists:flatten(PrefixSize)),
    Prefix0 = (16#FFFFFFFF bsl (32-Bits)) band 16#FFFFFFFF,
    Netmask = { Prefix0 band 16#FF000000 bsr 24,
               Prefix0 band 16#FF0000 bsr 16,
               Prefix0 band 16#FF00 bsr 8,
               Prefix0 band 16#FF },
    {IP, Netmask}.

-ifdef(TEST).

principal_eq({aws, H}, [{aws, H}]) -> true;
principal_eq([{aws, H}], {aws, H}) -> true;
principal_eq(LHS, RHS) ->   LHS =:= RHS.

resource_eq(?ARN{} = LHS, [?ARN{} = LHS]) -> true;
resource_eq([?ARN{} = LHS], ?ARN{} = LHS) -> true;
resource_eq(LHS, RHS) ->  LHS =:= RHS.

statement_eq(LHS, RHS) ->
    (LHS#statement.sid =:= RHS#statement.sid)
        andalso
    (LHS#statement.effect =:= RHS#statement.effect)
        andalso
    principal_eq(LHS#statement.principal, RHS#statement.principal)
        andalso
    (LHS#statement.action =:= RHS#statement.action)
        andalso
    (LHS#statement.not_action =:= RHS#statement.not_action)
        andalso
    resource_eq(LHS#statement.resource, RHS#statement.resource)
        andalso
    (LHS#statement.condition_block =:= RHS#statement.condition_block).

-endif.
