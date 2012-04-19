%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Entry point for poolboy to pool Riak connections

-module(riak_moss_riakc_pool_worker).

-export([riak_host_port/0,
         start_link/1,
         stop/1]).

riak_host_port() ->
    case application:get_env(riak_moss, riak_ip) of
        {ok, Host} ->
            ok;
        undefined ->
            Host = "127.0.0.1"
    end,
    case application:get_env(riak_moss, riak_pb_port) of
        {ok, Port} ->
            ok;
        undefined ->
            Port = 8087
    end,
    {Host, Port}.

start_link(_Args) ->
    {Host, Port} = riak_host_port(),
    riakc_pb_socket:start_link(Host, Port).

stop(Worker) ->
    riakc_pb_socket:stop(Worker).
