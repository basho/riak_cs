%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_wm_common).

-export([init/1,
         service_available/2,
         service_available/3,
         forbidden/2,
         content_types_accepted/2,
         content_types_provided/2,
         malformed_request/2,
         to_xml/2,
         allowed_methods/2,
         finish_request/2]).

-export([default_allowed_methods/1,
         default_content_types/1,
         default_finish_request/2,
         default_init/1,
         default_malformed_request/2]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").

%% ===================================================================
%% Webmachine callbacks
%% ===================================================================

init(Config) ->
    Mod = proplists:get_value(submodule, Config),
    dt_entry(Mod, <<"init">>),
    %% Check if authentication is disabled and set that in the context.
    AuthBypass = proplists:get_value(auth_bypass, Config),
    AuthModule = proplists:get_value(auth_module, Config),
    Exports = orddict:from_list(Mod:module_info(exports)),
    ExportsFun = exports_fun(Exports),
    Ctx = #context{auth_bypass=AuthBypass,
                   auth_module=AuthModule,
                   exports_fun=ExportsFun,
                   start_time=os:timestamp(),
                   submodule=Mod},
    resource_call(Mod, init, [Ctx], ExportsFun(init)).


-spec service_available(term(), term()) -> {true, term(), term()}.
service_available(RD, KeyCtx=#key_context{context=Ctx}) ->
    Mod = Ctx#context.submodule,
    dt_entry(Mod, <<"service_available">>),
    case service_available(RD, Ctx) of
        {true, UpdRD, UpdCtx} ->
            {true, UpdRD, KeyCtx#key_context{context=UpdCtx}};
        {false, _, _} ->
            {false, RD, KeyCtx}
    end;
service_available(RD, Ctx=#context{submodule=Mod}) ->
    dt_entry(Mod, <<"service_available">>),
    case riak_cs_utils:riak_connection() of
        {ok, Pid} ->
            {true, RD, Ctx#context{riakc_pid=Pid}};
        {error, _Reason} ->
            {false, RD, Ctx}
    end.

service_available(Pool, RD, Ctx) ->
    case riak_cs_utils:riak_connection(Pool) of
        {ok, Pid} ->
            {true, RD, Ctx#context{riakc_pid=Pid}};
        {error, _Reason} ->
            {false, RD, Ctx}
    end.

-spec malformed_request(term(), term()) -> {false, term(), term()}.
malformed_request(RD, Ctx=#context{submodule=Mod,
                                   exports_fun=ExportsFun}) ->
    dt_entry(Mod, <<"malformed_request">>),
    resource_call(Mod,
                  malformed_request,
                  [RD, Ctx],
                  ExportsFun(malformed_request)).

forbidden(RD, Ctx=#context{auth_module=AuthMod, submodule=Mod}) ->
    dt_entry(Mod, <<"forbidden">>),
    _Authenticated = AuthMod:authenticate(RD, Ctx),
    %% @TODO Use resource callback to check for special `Fun'
    case riak_cs_wm_utils:xfind_and_auth_user(RD, Ctx, fun auth_complete/2) of
        {false, _RD2, Ctx2} = FalseRet ->
            dt_return(Mod, <<"forbidden">>, [], [riak_cs_wm_utils:extract_name(Ctx2#context.user), <<"false">>]),
            FalseRet;
        {Rsn, _RD2, Ctx2} = Ret ->
            Reason =
                case Rsn of
                    {halt, Code} -> Code;
                    _            -> -1
                end,
            dt_return(Mod, <<"forbidden">>, [Reason], [riak_cs_wm_utils:extract_name(Ctx2#context.user), <<"true">>]),
            Ret
    end.

%% @doc Get the list of methods a resource supports.
-spec allowed_methods(term(), term()) -> {[atom()], term(), term()}.
allowed_methods(RD, Ctx=#context{submodule=Mod,
                                 exports_fun=ExportsFun}) ->
    dt_entry(Mod, <<"allowed_methods">>),
    Methods = resource_call(Mod,
                            allowed_methods,
                            [],
                            ExportsFun(allowed_methods)),
    {Methods, RD, Ctx}.

-spec content_types_accepted(term(), term()) ->
    {[{string(), atom()}], term(), term()}.
content_types_accepted(RD, Ctx=#context{submodule=Mod,
                                        exports_fun=ExportsFun}) ->
    dt_entry(Mod, <<"content_types_accepted">>),
    ContentTypes = resource_call(Mod,
                                 content_types_accepted,
                                 [],
                                 ExportsFun(content_types_accepted)),
    {ContentTypes, RD, Ctx}.

-spec content_types_provided(term(), term()) ->
    {[{string(), atom()}], term(), term()}.
content_types_provided(RD, Ctx=#context{submodule=Mod,
                                        exports_fun=ExportsFun}) ->
    dt_entry(Mod, <<"content_types_provided">>),
    ContentTypes = resource_call(Mod,
                                 content_types_provided,
                                 [],
                                 ExportsFun(content_types_provided)),
    {ContentTypes, RD, Ctx}.

-spec to_xml(term(), term()) ->
    {{'halt', term()}, term(), #context{}}.
to_xml(RD, Ctx=#context{user=User,
                        submodule=Mod,
                        exports_fun=ExportsFun}) ->
    dt_entry(Mod, <<"to_xml">>),
    dt_entry_service(Mod, <<"service_get_buckets">>),
    Res = resource_call(Mod,
                        to_xml,
                        [RD, Ctx],
                        ExportsFun(to_xml)),
    dt_return(Mod, <<"to_xml">>, [], [riak_cs_wm_utils:extract_name(User), <<"service_get_buckets">>]),
    dt_return_service(Mod, <<"service_get_buckets">>, [], [riak_cs_wm_utils:extract_name(User)]),
    Res.

finish_request(RD, Ctx=#context{riakc_pid=undefined,
                                submodule=Mod,
                                exports_fun=ExportsFun}) ->
    dt_entry(Mod, <<"finish_request">>, [0], []),
    Res = resource_call(Mod,
                        finish_request,
                        [RD, Ctx],
                        ExportsFun(finish_request)),
    dt_return(Mod, <<"finish_request">>, [0], []),
    Res;
finish_request(RD, Ctx=#context{submodule=Mod,
                                exports_fun=ExportsFun}) ->
    dt_entry(Mod, <<"finish_request">>, [1], []),
    Res = resource_call(Mod,
                        finish_request,
                        [RD, Ctx],
                        ExportsFun(finish_request)),
    dt_return(Mod, <<"finish_request">>, [1], []),
    Res.

%% ===================================================================
%% Helper functions
%% ===================================================================

-spec exports_fun(orddict:new()) -> function().
exports_fun(Exports) ->
    fun(Function) ->
            orddict:is_key(Function, Exports)
    end.

resource_call(Mod, Fun, Args, true) ->
    erlang:apply(Mod, Fun, Args);
resource_call(_Mod, Fun, Args, false) ->
    erlang:apply(?MODULE, default(Fun), Args).


%% @doc This function will be called by
%% `riak_cs_wm_utils:find_and_auth_user' if the user is successfully
%% autenticated. ACLs are not applicaable to service-level requests so
%% we just return a tuple indicating that the request may proceed.
-spec auth_complete(term(), term()) -> {false, term(), term()}.
auth_complete(RD, Ctx) ->
    {false, RD, Ctx}.

%% ===================================================================
%% Resource function defaults
%% ===================================================================

default(init) ->
    default_init;
default(allowed_methods) ->
    default_allowed_methods;
default(content_types_accepted) ->
    default_content_types;
default(content_types_provided) ->
    default_content_types;
default(malformed_request) ->
    default_malformed_request;
default(_) ->
    undefined.

default_init(Ctx) ->
    {ok, Ctx}.

default_malformed_request(RD, Ctx) ->
    {false, RD, Ctx}.

default_content_types(_) ->
    [].

%% @doc Mapping of resource module to allowed methods
-spec default_allowed_methods(atom()) -> [atom()].
default_allowed_methods(_) ->
    [].

default_finish_request(RD, Ctx=#context{riakc_pid=undefined}) ->
    {true, RD, Ctx};
default_finish_request(RD, Ctx=#context{riakc_pid=RiakPid}) ->
    riak_cs_utils:close_riak_connection(RiakPid),
    {true, RD, Ctx#context{riakc_pid=undefined}}.

%% ===================================================================
%% DTrace functions
%% ===================================================================

dt_entry(Mod, Func) ->
    dt_entry(Mod, Func, [], []).

dt_entry(Mod, Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, Ints, Mod, Func, Strings).

dt_entry_service(Mod, Func) ->
    dt_entry_service(Mod, Func, [], []).

dt_entry_service(Mod, Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_SERVICE_OP, 1, Ints, Mod, Func, Strings).

dt_return(Mod, Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 2, Ints, Mod, Func, Strings).

dt_return_service(Mod, Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_SERVICE_OP, 2, Ints, Mod, Func, Strings).
