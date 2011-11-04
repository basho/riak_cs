%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(basho_bench_driver_moss).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(url, {host, port}).
-record(state, { client_id }).

%% ====================================================================
%% API
%% ====================================================================

-spec new(integer()) -> {ok, term()}.
new(ID) ->
    application:start(ibrowse),
    {ok, #state{client_id=ID}}.

-spec run(atom(), fun(), fun(), term()) -> {ok, term()}.
run(_Operation, _KeyGen, _ValueGen, State) ->
    {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% This is ripped from basho_bench_driver_http_raw.erl
connect(Url) ->
    case erlang:get({ibrowse_pid, Url#url.host}) of
        undefined ->
            {ok, Pid} = ibrowse_http_client:start({Url#url.host, Url#url.port}),
            erlang:put({ibrowse_pid, Url#url.host}, Pid),
            Pid;
        Pid ->
            case is_process_alive(Pid) of
                true ->
                    Pid;
                false ->
                    erlang:erase({ibrowse_pid, Url#url.host}),
                    connect(Url)
            end
    end.


%% This is ripped from basho_bench_driver_http_raw.erl
disconnect(Url) ->
    case erlang:get({ibrowse_pid, Url#url.host}) of
        undefined ->
            ok;
        OldPid ->
            catch(ibrowse_http_client:stop(OldPid))
    end,
    erlang:erase({ibrowse_pid, Url#url.host}),
    ok.
