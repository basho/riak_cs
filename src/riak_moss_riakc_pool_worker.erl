%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Entry point for poolboy to pool Riak connections

-module(riak_moss_riakc_pool_worker).

-include("riak_moss.hrl").

-export([start_link/1,
         stop/1]).

start_link(_Args) ->
    case application:get_env(?RIAKCS, riak_ip) of
        {ok, Host} ->
            ok;
        undefined ->
            Host = "127.0.0.1"
    end,
    case application:get_env(?RIAKCS, riak_pb_port) of
        {ok, Port} ->
            ok;
        undefined ->
            Port = 8087
    end,
    riakc_pb_socket:start_link(Host, Port).

stop(Worker) ->
    riakc_pb_socket:stop(Worker).
