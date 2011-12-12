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
      [test_n_chunks_builder(X) || X <- [1,2,5,9,100,1000]]
     ]}.

calc_block_size(ContentLength, NumBlocks) ->
    Quotient = ContentLength div NumBlocks,
    case ContentLength rem NumBlocks of
        0 ->
            Quotient;
        _ ->
            Quotient + 1
    end.

test_n_chunks_builder(N) ->
    fun () ->
        BlockSize = calc_block_size(10000, N),
        application:set_env(riak_moss, lfs_block_size, BlockSize),
        {ok, Pid} = riak_moss_get_fsm:test_link(<<"bucket">>, <<"key">>),
        ?assertEqual(dict:new(), riak_moss_get_fsm:get_metadata(Pid)),
        riak_moss_get_fsm:continue(Pid),
        expect_n_chunks(Pid, N)
    end.

receives_metadata() ->
    {ok, Pid} = riak_moss_get_fsm:test_link(<<"bucket">>, <<"key">>),
    ?assertEqual(dict:new(), riak_moss_get_fsm:get_metadata(Pid)),
    riak_moss_get_fsm:stop(Pid).

%% ===================================================================
%% Helper Funcs
%% ===================================================================

expect_n_chunks(FsmPid, N) ->
    %% subtract 1 from N because the last
    %% chunk will have a different pattern
    [?assertMatch({chunk, _}, riak_moss_get_fsm:get_next_chunk(FsmPid)) || _ <- lists:seq(1, N-1)],
    ?assertMatch({done, _}, riak_moss_get_fsm:get_next_chunk(FsmPid)).
