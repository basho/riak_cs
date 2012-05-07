%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_wm_service).

-export([init/1,
         service_available/2,
         forbidden/2,
         content_types_provided/2,
         malformed_request/2,
         to_xml/2,
         allowed_methods/2,
         finish_request/2]).

-include("riak_moss.hrl").
-include_lib("webmachine/include/webmachine.hrl").

init(Config) ->
    dt_entry(<<"init">>),
    %% Check if authentication is disabled and
    %% set that in the context.
    AuthBypass = proplists:get_value(auth_bypass, Config),
    {ok, #context{start_time=now(), auth_bypass=AuthBypass}}.

-spec service_available(term(), term()) -> {true, term(), term()}.
service_available(RD, Ctx) ->
    dt_entry(<<"service_available">>),
    riak_moss_wm_utils:service_available(RD, Ctx).

-spec malformed_request(term(), term()) -> {false, term(), term()}.
malformed_request(RD, Ctx) ->
    dt_entry(<<"malformed_request">>),
    {false, RD, Ctx}.

%% @doc Check to see if the user is
%%      authenticated. Normally with HTTP
%%      we'd use the `authorized` callback,
%%      but this is how S3 does things.
forbidden(RD, Ctx=#context{auth_bypass=AuthBypass,
                           riakc_pid=RiakPid}) ->
    dt_entry(<<"forbidden">>),
    AuthHeader = wrq:get_req_header("authorization", RD),
    {AuthMod, KeyId, Signature} =
        riak_moss_wm_utils:parse_auth_header(AuthHeader, AuthBypass),
    case riak_moss_utils:get_user(KeyId, RiakPid) of
        {ok, {User, _}} ->
            case AuthMod:authenticate(RD, User?MOSS_USER.key_secret, Signature) of
                ok ->
                    %% Authentication succeeded
                    AccessRD = riak_moss_access_logger:set_user(User, RD),
                    dt_return(<<"forbidden">>, [], [extract_name(User), <<"false">>]),
                    {false, AccessRD, Ctx#context{user=User}};
                {error, _Reason} ->
                    %% Authentication failed, deny access
                    dt_return(<<"forbidden">>, [403], [extract_name(User), <<"true">>]),
                    riak_moss_s3_response:api_error(access_denied, RD, Ctx)
            end;
        {error, no_user_key} ->
            %% Anonymous access not allowed, deny access
            dt_return(<<"forbidden">>, [403], [extract_name(KeyId), <<"true">>]),
            riak_moss_s3_response:api_error(access_denied, RD, Ctx);
        {error, notfound} ->
            %% Access not allowed, deny access
            dt_return(<<"forbidden">>, [403], [extract_name(KeyId), <<"true">>]),
            riak_moss_s3_response:api_error(invalid_access_key_id, RD, Ctx);
        {error, Reason} ->
            %% Access not allowed, deny access and log the reason
            _ = lager:error("Retrieval of user record for ~p failed. Reason: ~p", [KeyId, Reason]),
            Code = riak_moss_s3_response:status_code(Reason),
            dt_return(<<"forbidden">>, [Code], [extract_name(KeyId), <<"true">>]),
            riak_moss_s3_response:api_error(invalid_access_key_id, RD, Ctx)
    end.

%% @doc Get the list of methods this resource supports.
-spec allowed_methods(term(), term()) -> {[atom()], term(), term()}.
allowed_methods(RD, Ctx) ->
    dt_entry(<<"allowed_methods">>),
    %% TODO:
    %% The docs seem to suggest GET is the only
    %% allowed method. It's worth checking to see
    %% if HEAD is supported too.
    {['GET'], RD, Ctx}.

-spec content_types_provided(term(), term()) ->
    {[{string(), atom()}], term(), term()}.
content_types_provided(RD, Ctx) ->
    dt_entry(<<"content_types_provided">>),
    %% TODO:
    %% This needs to be xml soon, but for now
    %% let's do json.
    {[{"application/xml", to_xml}], RD, Ctx}.


%% TODO:
%% This spec will need to be updated
%% when we change this to allow streaming
%% bodies.
-spec to_xml(term(), term()) ->
    {{'halt', term()}, term(), #context{}}.
to_xml(RD, Ctx=#context{start_time=StartTime,user=User}) ->
    dt_entry(<<"to_xml">>),
    Res = riak_moss_s3_response:list_all_my_buckets_response(User, RD, Ctx),
    ok = riak_cs_stats:update_with_start(service_get_buckets, StartTime),
    dt_entry(<<"to_xml">>, [], [extract_name(User), <<"service_get_buckets">>]),
    Res.

finish_request(RD, Ctx=#context{riakc_pid=undefined}) ->
    dt_entry(<<"finish_request">>, [0], []),
    {true, RD, Ctx};
finish_request(RD, Ctx=#context{riakc_pid=RiakPid}) ->
    dt_entry(<<"finish_request">>, [1], []),
    riak_moss_utils:close_riak_connection(RiakPid),
    dt_return(<<"finish_request">>, [1], []),
    {true, RD, Ctx#context{riakc_pid=undefined}}.

extract_name(X) ->
    riak_moss_wm_utils:extract_name(X).

dt_entry(Func) ->
    dt_entry(Func, [], []).

dt_entry(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, Ints, ?MODULE, Func, Strings).

dt_return(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 2, Ints, ?MODULE, Func, Strings).
