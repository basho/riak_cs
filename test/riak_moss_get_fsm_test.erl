%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_get_fsm_test).

-include_lib("eunit/include/eunit.hrl").

setup() ->
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, {file, "moss_get_fsm_sasl.log"}),
    error_logger:tty(false),
    application:start(lager),
    lager:set_loglevel(lager_console_backend, info).

%% TODO:
%% Implement this
teardown(_) ->
    ok.

get_fsm_test_() ->
    {setup,
     fun setup/0,
     fun teardown/1,
     [
      fun receives_metadata/0
     ]}.

receives_metadata() ->
    {ok, _Pid} = riak_moss_get_fsm:test_link(self(), <<"bucket">>, <<"key">>),
    ?assertMatch({metadata, _}, receive_with_timeout(1000)).

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
