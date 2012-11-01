%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_wm_buckets).

-export([init/1,
         service_available/2,
         forbidden/2,
         content_types_provided/2,
         malformed_request/2,
         to_xml/2,
         allowed_methods/2,
         finish_request/2]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").

init(Config) ->
    dt_entry(<<"init">>),
    %% Check if authentication is disabled and
    %% set that in the context.
    AuthBypass = proplists:get_value(auth_bypass, Config),
    {ok, #context{start_time=os:timestamp(), auth_bypass=AuthBypass}}.

-spec service_available(term(), term()) -> {true, term(), term()}.
service_available(RD, Ctx) ->
    dt_entry(<<"service_available">>),
    riak_cs_wm_utils:service_available(RD, Ctx).

-spec malformed_request(term(), term()) -> {false, term(), term()}.
malformed_request(RD, Ctx) ->
    dt_entry(<<"malformed_request">>),
    {false, RD, Ctx}.

%% @doc Check to see if the user is
%%      authenticated. Normally with HTTP
%%      we'd use the `authorized` callback,
%%      but this is how S3 does things.
forbidden(RD, Ctx) ->
    dt_entry(<<"forbidden">>),
    case riak_cs_wm_utils:find_and_auth_user(RD, Ctx, fun auth_complete/2, false) of
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
    end.

%% @doc This function will be called by
%% `riak_cs_wm_utils:find_and_auth_user' if the user is successfully
%% autenticated. ACLs are not applicaable to service-level requests so
%% we just return a tuple indicating that the request may proceed.
-spec auth_complete(term(), term()) -> {false, term(), term()}.
auth_complete(RD, Ctx) ->
    {false, RD, Ctx}.

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
    dt_entry_service(<<"to_xml">>),
    Res = riak_cs_s3_response:list_all_my_buckets_response(User, RD, Ctx),
    ok = riak_cs_stats:update_with_start(service_get_buckets, StartTime),
    dt_return(<<"to_xml">>, [], [extract_name(User), <<"service_get_buckets">>]),
    dt_return_service(<<"service_get_buckets">>, [], [extract_name(User)]),
    Res.

finish_request(RD, Ctx=#context{riakc_pid=undefined}) ->
    dt_entry(<<"finish_request">>, [0], []),
    {true, RD, Ctx};
finish_request(RD, Ctx=#context{riakc_pid=RiakPid}) ->
    dt_entry(<<"finish_request">>, [1], []),
    riak_cs_utils:close_riak_connection(RiakPid),
    dt_return(<<"finish_request">>, [1], []),
    {true, RD, Ctx#context{riakc_pid=undefined}}.

extract_name(X) ->
    riak_cs_wm_utils:extract_name(X).

dt_entry(Func) ->
    dt_entry(Func, [], []).

dt_entry(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, Ints, ?MODULE, Func, Strings).

dt_entry_service(Func) ->
    dt_entry_service(Func, [], []).

dt_entry_service(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_SERVICE_OP, 1, Ints, ?MODULE, Func, Strings).

dt_return(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 2, Ints, ?MODULE, Func, Strings).

dt_return_service(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_SERVICE_OP, 2, Ints, ?MODULE, Func, Strings).
