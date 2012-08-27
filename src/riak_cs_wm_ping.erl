%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_wm_ping).

-export([init/1,
         service_available/2,
         allowed_methods/2,
         to_html/2,
         finish_request/2]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-record(ping_context, {pool_pid=true :: boolean(),
                  riakc_pid :: pid()}).

%% -------------------------------------------------------------------
%% Webmachine callbacks
%% -------------------------------------------------------------------

init(_Config) ->
    dt_entry(<<"init">>),
    {ok, #ping_context{}}.

-spec service_available(term(), term()) -> {boolean(), term(), term()}.
service_available(RD, Ctx) ->
    dt_entry(<<"service_available">>),
    case poolboy:checkout(request_pool, true, ping_timeout()) of
        full ->
            case riak_cs_riakc_pool_worker:start_link([]) of
                {ok, Pid} ->
                    UpdCtx = Ctx#ping_context{riakc_pid=Pid,
                                              pool_pid=false};
                {error, _} ->
                    Pid = undefined,
                    UpdCtx = Ctx
            end;
        Pid ->
            UpdCtx = Ctx#ping_context{riakc_pid=Pid}
    end,
    case Pid of
        undefined ->
            Available = false;
        _ ->
            case riakc_pb_socket:ping(Pid, ping_timeout()) of
                pong ->
                    Available = true;
                 _ ->
                    Available = false
            end
    end,
    {Available, RD, UpdCtx}.

-spec allowed_methods(term(), term()) -> {[atom()], term(), term()}.
allowed_methods(RD, Ctx) ->
    dt_entry(<<"allowed_methods">>),
    {['GET', 'HEAD'], RD, Ctx}.

to_html(ReqData, Ctx) ->
    {"OK", ReqData, Ctx}.

finish_request(RD, Ctx=#ping_context{riakc_pid=undefined}) ->
    dt_entry(<<"finish_request">>, [0], []),
    {true, RD, Ctx};
finish_request(RD, Ctx=#ping_context{riakc_pid=RiakPid,
                                pool_pid=PoolPid}) ->
    dt_entry(<<"finish_request">>, [1], []),
    case PoolPid of
        true ->
            riak_cs_utils:close_riak_connection(RiakPid);
        false ->
            riak_cs_riakc_pool_worker:stop(RiakPid)
    end,
    dt_return(<<"finish_request">>, [1], []),
    {true, RD, Ctx#ping_context{riakc_pid=undefined}}.

%% -------------------------------------------------------------------
%% Internal functions
%% -------------------------------------------------------------------

dt_entry(Func) ->
    dt_entry(Func, [], []).

dt_entry(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, Ints, ?MODULE, Func, Strings).

dt_return(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 2, Ints, ?MODULE, Func, Strings).

%% @doc Return the configured ping timeout. Default is 5 seconds.  The
%% timeout is used in call to `poolboy:checkout' and if that fails in
%% the call to `riakc_pb_socket:ping' so the effective cumulative
%% timeout could be up to 2 * `ping_timeout()'.
-spec ping_timeout() -> pos_integer().
ping_timeout() ->
    case application:get_env(riak_moss, ping_timeout) of
        undefined ->
            ?DEFAULT_PING_TIMEOUT;
        {ok, Timeout} ->
            Timeout
    end.
