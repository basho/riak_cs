%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc This supervisor monitors the access archiver and its Riak
%% client.  The supervisor is setup for `rest_for_one` restart, such
%% that the archiver exiting also tears down its client.  The client
%% is a temporary worker, however, so it is never restarted by the
%% supervisor, and should instead be monitored (and restarted, if
%% necessary) by the archiver.
-module(riak_moss_access_archiver_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         start_client/0,
         stop_client/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

client_child_name() ->
    access_archiver_riak_client.

client_child_spec() ->
    {Host, Port} = riak_moss_riakc_pool_worker:riak_host_port(),
    {client_child_name(),
     {riakc_pb_socket, start_link,
      [Host, Port, [queue_if_disconnected, {connect_timeout, 5000}]]},
     temporary, brutal_kill, worker,
     [riak_moss_riakc_pool_worker, riakc_pb_socket]}.

start_client() ->
    supervisor:start_child(?SERVER, client_child_spec()).

stop_client() ->
    %% should this use the pid instead?
    supervisor:terminate_child(?SERVER, client_child_name()).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([]) ->
    RestartStrategy = rest_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    Archiver = {riak_moss_access_archiver,
                {riak_moss_access_archiver, start_link, []},
                permanent, 5000, worker,
                [riak_moss_access_archiver]},

    {ok, {SupFlags, [Archiver]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
