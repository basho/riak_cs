%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_get_fsm_test).

-include("riak_cs.hrl").
-include_lib("eunit/include/eunit.hrl").

setup() ->
%    TestNode = list_to_atom("testnode" ++ integer_to_list(element(3, now())) ++
%                                "@localhost"),
%    {ok, _} = net_kernel:start([TestNode, longnames]),
    application:load(sasl),
    application:load(riak_cs),
    application:set_env(sasl, sasl_error_logger, {file, "cs_get_fsm_sasl.log"}),
    error_logger:tty(false),
    application:start(lager).

%% TODO:
%% Implement this
teardown(_) ->
    application:stop(riak_cs),
    application:stop(sasl),
    net_kernel:stop().

get_fsm_test_() ->
    {setup,
     fun setup/0,
     fun teardown/1,
     [
      fun receives_manifest/0,
      [{timeout, 30, test_n_chunks_builder(X)} || X <- [1,2,5,9,100,1000]]
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
            ContentLength = 10000,
            BlockSize = calc_block_size(ContentLength, N),
            application:set_env(riak_moss, lfs_block_size, BlockSize),
            {ok, Pid} = riak_cs_get_fsm:test_link(<<"bucket">>, <<"key">>, ContentLength, BlockSize),
            Manifest = riak_cs_get_fsm:get_manifest(Pid),
            ?assertEqual(ContentLength, Manifest?MANIFEST.content_length),
            riak_cs_get_fsm:continue(Pid),
            expect_n_chunks(Pid, N)
    end.

receives_manifest() ->
    {ok, Pid} = riak_cs_get_fsm:test_link(<<"bucket">>, <<"key">>, 100, 10),
    Manifest = riak_cs_get_fsm:get_manifest(Pid),
    ?assertEqual(100, Manifest?MANIFEST.content_length),
    riak_cs_get_fsm:stop(Pid).

%% ===================================================================
%% Helper Funcs
%% ===================================================================

expect_n_chunks(FsmPid, N) ->
    %% subtract 1 from N because the last
    %% chunk will have a different pattern
    [?assertMatch({chunk, _}, riak_cs_get_fsm:get_next_chunk(FsmPid)) || _ <- lists:seq(1, N-1)],
    ?assertMatch({done, _}, riak_cs_get_fsm:get_next_chunk(FsmPid)).
