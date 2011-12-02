%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_get_fsm_test).

-include_lib("eunit/include/eunit.hrl").

setup() ->
    application:load(sasl),
    application:load(riak_moss),
    application:set_env(sasl, sasl_error_logger, {file, "moss_get_fsm_sasl.log"}),
    error_logger:tty(false),
    application:start(lager),
    lager:set_loglevel(lager_console_backend, info).

%% TODO:
%% Implement this
teardown(_) ->
    application:stop(riak_moss),
    application:stop(sasl).

get_fsm_test_() ->
    {setup,
     fun setup/0,
     fun teardown/1,
     [
      fun receives_metadata/0,
      fun receives_2_chunks/0,
      fun receives_100_chunks/0,
      fun receives_1000_chunks/0
     ]}.

receives_metadata() ->
    {ok, Pid} = riak_moss_get_fsm:test_link(self(), <<"bucket">>, <<"key">>),
    ?assertMatch({metadata, _}, receive_with_timeout(1000)),
    riak_moss_get_fsm:stop(Pid).

receives_2_chunks() ->
    application:set_env(riak_moss, lfs_block_size, 5000),
    {ok, Pid} = riak_moss_get_fsm:test_link(self(), <<"bucket">>, <<"key">>),
    ?assertMatch({metadata, _}, receive_with_timeout(1000)),
    riak_moss_get_fsm:continue(Pid),
    expect_n_chunks(2),
    riak_moss_get_fsm:stop(Pid).

receives_100_chunks() ->
    application:set_env(riak_moss, lfs_block_size, 100),
    {ok, Pid} = riak_moss_get_fsm:test_link(self(), <<"bucket">>, <<"key">>),
    ?assertMatch({metadata, _}, receive_with_timeout(1000)),
    riak_moss_get_fsm:continue(Pid),
    expect_n_chunks(100),
    riak_moss_get_fsm:stop(Pid).

receives_1000_chunks() ->
    application:set_env(riak_moss, lfs_block_size, 10),
    {ok, Pid} = riak_moss_get_fsm:test_link(self(), <<"bucket">>, <<"key">>),
    ?assertMatch({metadata, _}, receive_with_timeout(1000)),
    riak_moss_get_fsm:continue(Pid),
    expect_n_chunks(1000),
    riak_moss_get_fsm:stop(Pid).

%% ===================================================================
%% Helper Funcs
%% ===================================================================

%% @doc Just returns whatever it receives, or
%%      `timeout` after `Timeout`
receive_with_timeout(Timeout) ->
    receive
        Anything ->
            Anything
    after
        Timeout -> timeout
    end.

expect_n_chunks(N) ->
    lists:foreach(fun (_) ->
                         ?assertMatch({chunk, _}, receive_with_timeout(100)) end,
                  lists:seq(1,N-1)),  %% subtract 1 from N because we'll
                                      %% end with an expectation of `done`
    ?assertMatch({done, _}, receive_with_timeout(1000)).
