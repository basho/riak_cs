%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_wm_user).

-export([init/1,
         service_available/2,
         forbidden/2,
         content_types_provided/2,
         content_types_accepted/2,
         accept_json/2,
         accept_xml/2,
         allowed_methods/2,
         produce_json/2,
         produce_xml/2,
         process_post/2,
         finish_request/2]).

-include("riak_moss.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("xmerl/include/xmerl.hrl").

%% -------------------------------------------------------------------
%% Webmachine callbacks
%% -------------------------------------------------------------------

init(Config) ->
    dt_entry(<<"init">>),
    %% Check if authentication is disabled and
    %% set that in the context.
    AuthBypass = proplists:get_value(auth_bypass, Config),
    {ok, #context{auth_bypass=AuthBypass}}.

-spec service_available(term(), term()) -> {true, term(), term()}.
service_available(RD, Ctx) ->
    dt_entry(<<"service_available">>),
    riak_moss_wm_utils:service_available(RD, Ctx).

-spec allowed_methods(term(), term()) -> {[atom()], term(), term()}.
allowed_methods(RD, Ctx) ->
    dt_entry(<<"allowed_methods">>),
    {['GET', 'HEAD', 'POST', 'PUT'], RD, Ctx}.

forbidden(RD, Ctx) ->
    dt_entry(<<"forbidden">>),
    case wrq:method(RD) of
        'POST' ->
            dt_return(<<"forbidden">>, [], [<<"POST method">>]),
            {false, RD, Ctx};
        _ ->
            Next = fun(NewRD, NewCtx=#context{user=User}) ->
                           AccessRD = riak_moss_access_logger:set_user(User, NewRD),
                           forbidden(AccessRD, NewCtx, User)
                   end,
            case riak_moss_wm_utils:find_and_auth_user(RD, Ctx, Next) of
                {false, _RD2, Ctx2} = FalseRet ->
                    dt_return(<<"forbidden">>, [], [extract_name(Ctx2#context.user), <<"false">>]),
                    FalseRet;
                {Rsn, _RD2, Ctx2} = Ret ->
                    Reason = case Rsn of
                                 {halt, Code} -> Code;
                                 _            -> -1
                             end,
                    dt_return(<<"forbidden">>, [Reason], [extract_name(Ctx2#context.user), <<"true">>]),
                    Ret
            end
    end.

-spec content_types_accepted(term(), term()) ->
    {[{string(), atom()}], term(), term()}.
content_types_accepted(RD, Ctx) ->
    dt_entry(<<"content_types_accepted">>),
    {[{?XML_TYPE, accept_xml}, {?JSON_TYPE, accept_json}], RD, Ctx}.

content_types_provided(RD, Ctx) ->
    dt_entry(<<"content_types_provided">>),
    {[{?XML_TYPE, produce_xml}, {?JSON_TYPE, produce_json}], RD, Ctx}.

-spec accept_json(term(), term()) ->
    {boolean() | {halt, term()}, term(), term()}.
accept_json(RD, Ctx) ->
    dt_entry(<<"accept_json">>),
    Body = wrq:req_body(RD),
    case catch mochijson2:decode(Body) of
        {struct, UserItems} ->
            UpdateItems = lists:foldl(fun user_json_filter/2, [], UserItems),
            update_user(UpdateItems, RD, Ctx);
        {'EXIT', _} ->
            riak_moss_s3_response:api_error(invalid_user_update, RD, Ctx)
    end.

-spec accept_xml(term(), term()) ->
    {boolean() | {halt, term()}, term(), term()}.
accept_xml(RD, Ctx) ->
    dt_entry(<<"accept_xml">>),
    Body = binary_to_list(wrq:req_body(RD)),
    case catch xmerl_scan:string(Body, []) of
        {'EXIT', _} ->
            riak_moss_s3_response:api_error(invalid_user_update, RD, Ctx);
        {ParsedData, _Rest} ->
            UpdateItems = lists:foldl(fun user_xml_filter/2,
                                      [],
                                      ParsedData#xmlElement.content),
            update_user(UpdateItems, RD, Ctx)
    end.

produce_json(RD, #context{user=User}=Ctx) ->
    dt_entry(<<"produce_json">>),
    MJ = {struct, riak_moss_wm_utils:user_record_to_proplist(User)},
    Body = mochijson2:encode(MJ),
    Etag = etag(Body),
    RD2 = wrq:set_resp_header("ETag", Etag, RD),
    {Body, RD2, Ctx}.

produce_xml(RD, #context{user=User}=Ctx) ->
    dt_entry(<<"produce_xml">>),
    XmlUserRec =
        [{Key, [binary_to_list(Value)]} ||
            {Key, Value} <- riak_moss_wm_utils:user_record_to_proplist(User)],
    Doc = [{'User', XmlUserRec}],
    Body = riak_moss_s3_response:export_xml(Doc),
    Etag = etag(Body),
    RD2 = wrq:set_resp_header("ETag", Etag, RD),
    {Body, RD2, Ctx}.

%% @doc Create a user from a POST.
%%      and return the user object
%%      as JSON
-spec process_post(term(), term()) -> {true, term(), term}.
process_post(RD, Ctx) ->
    dt_entry(<<"process_post">>),
    Body = wrq:req_body(RD),
    ParsedBody = mochiweb_util:parse_qs(binary_to_list(Body)),
    UserName = proplists:get_value("name", ParsedBody, ""),
    Email= proplists:get_value("email", ParsedBody, ""),
    case riak_moss_utils:create_user(UserName, Email) of
        {ok, UserRecord} ->
            PropListUser = riak_moss_wm_utils:user_record_to_proplist(UserRecord),
            CTypeWritten = wrq:set_resp_header("Content-Type", ?JSON_TYPE, RD),
            WrittenRD = wrq:set_resp_body(list_to_binary(
                                            mochijson2:encode(PropListUser)),
                                          CTypeWritten),
            {true, WrittenRD, Ctx};
        {error, Reason} ->
            riak_moss_s3_response:api_error(Reason, RD, Ctx)
    end.

finish_request(RD, Ctx=#context{riakc_pid=undefined}) ->
    dt_entry(<<"finish_request">>, [0], []),
    {true, RD, Ctx};
finish_request(RD, Ctx=#context{riakc_pid=RiakPid}) ->
    dt_entry(<<"finish_request">>, [1], []),
    riak_moss_utils:close_riak_connection(RiakPid),
    dt_return(<<"finish_request">>, [1], []),
    {true, RD, Ctx#context{riakc_pid=undefined}}.

%% -------------------------------------------------------------------
%% Internal functions
%% -------------------------------------------------------------------

%% @doc Calculate the etag of a response body
etag(Body) ->
    webmachine_util:quoted_string(
      riak_moss_utils:binary_to_hexlist(
        crypto:md5(Body))).

forbidden(RD, Ctx, undefined) ->
    %% anonymous access disallowed
    riak_moss_wm_utils:deny_access(RD, Ctx);
forbidden(RD, Ctx, User) ->
    UserKeyId = User?MOSS_USER.key_id,
    UserPathKey = user_key(RD),
    case UserPathKey of
        [] ->
            %% user is accessing own account
            %% @TODO Determine if logging this is appropriate
            %% and if we need to classify it differently.
            AccessRD = riak_moss_access_logger:set_user(User, RD),
            {false, AccessRD, Ctx};
         UserKeyId ->
            %% user is accessing own account
            %% @TODO Determine if logging this is appropriate
            %% and if we need to classify it differently.
            AccessRD = riak_moss_access_logger:set_user(User, RD),
            {false, AccessRD, Ctx};
        _ ->
            case riak_moss_utils:get_admin_creds() of
                {ok, {Admin, _}} when Admin == UserKeyId ->
                    %% admin can access any account
                    case riak_moss_utils:get_user(UserPathKey, Ctx#context.riakc_pid) of
                        {ok, {ReqUser, _}} ->
                            {false, RD, Ctx#context{user=ReqUser}};
                        {error, Reason} ->
                            _ = lager:warning("Failed to fetch user record. KeyId: ~p"
                                          " Reason: ~p", [UserPathKey, Reason]),
                            riak_moss_s3_response:api_error(invalid_access_key_id, RD, Ctx)
                    end;
                _ ->
                    %% no one else is allowed
                    riak_moss_wm_utils:deny_access(RD, Ctx)
            end
    end.

-spec update_user([{binary(), binary()}], term(), term()) ->
    {boolean() | {halt, term()}, term(), term()}.
update_user(UpdateItems, RD, Ctx=#context{user=User,
                                      user_vclock=VClock,
                                      riakc_pid=RiakPid}) ->
    dt_entry(<<"update_user">>),
    case lists:keyfind(status, 1, UpdateItems) of
        {status, Status} ->
            case Status =:= User?MOSS_USER.status of
                true ->
                    {{halt, 200}, RD, Ctx};
                false ->
                    case riak_moss_utils:save_user(User?MOSS_USER{status=Status},
                                                   VClock,
                                                   RiakPid) of
                        ok ->
                            {{halt, 200}, RD, Ctx};
                        {error, Reason} ->
                            riak_moss_s3_response:api_error(Reason, RD, Ctx)
                    end
            end;
        false ->
            riak_moss_s3_response:api_error(invalid_user_update, RD, Ctx)
    end.

-spec user_json_filter({binary(), binary()}, [{atom(), term()}]) -> [{atom(), term()}].
user_json_filter({ItemKey, ItemValue}, Acc) ->
    case ItemKey of
        <<"email">> ->
            Acc;
        <<"name">> ->
            Acc;
        <<"status">> ->
            case ItemValue of
                <<"enabled">> ->
                    [{status, enabled} | Acc];
                <<"disabled">> ->
                    [{status, disabled} | Acc];
                _ ->
                    Acc
            end;
        _ ->
            Acc
    end.

user_key(RD) ->
    case wrq:path_tokens(RD) of
        [KeyId|_] -> mochiweb_util:unquote(KeyId);
        _         -> []
    end.

-spec user_xml_filter(#xmlText{} | #xmlElement{}, [{atom(), term()}]) -> [{atom(), term()}].
user_xml_filter(#xmlText{}, Acc) ->
    Acc;
user_xml_filter(Element, Acc) ->
    case Element#xmlElement.name of
        'Email' ->
            Acc;
        'Name' ->
            Acc;
        'Status' ->
            [Content | _] = Element#xmlElement.content,
            case is_record(Content, xmlText) of
                true ->
                    case Content#xmlText.value of
                        "enabled" ->
                            [{status, enabled} | Acc];
                        "disabled" ->
                            [{status, disabled} | Acc];
                        _ ->
                            Acc
                    end;
                false ->
                    Acc
            end;
        _ ->
            Acc
    end.

extract_name(X) ->
    riak_moss_wm_utils:extract_name(X).

dt_entry(Func) ->
    dt_entry(Func, [], []).

dt_entry(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, Ints, ?MODULE, Func, Strings).

dt_return(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 2, Ints, ?MODULE, Func, Strings).
