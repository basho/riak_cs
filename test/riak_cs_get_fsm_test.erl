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

get_fsm_should_never_fail_intermittently_test_() ->
    {setup,
     fun setup/0,
     fun teardown/1,
     [
      fun receives_manifest/0,
      %% In a perfect testing world, we would use chunks of
      %% various sizes.  However, due to limitations of
      %% using riak_cs_get_fsm:test_link() which uses multiple
      %% dummy reader procs, and the dummy readers are too dumb
      %% to be able to create chunks of variable size, we must
      %% choose chunk sizes where the dummy readers can give
      %% the get FSM decent chunk sizes.
      %%
      %% Thus we must use values that evenly divide the
      %% ContentLength = 10000, e.g. 1,2,5,100,1000.
      [{timeout, 300, fun() -> [ok = (test_n_chunks_builder(X))() ||
                                   _ <- lists:seq(1, Iters)] end} ||
          {X, Iters} <- [{1, 500}, {2, 500}, {5, 500},
                         {100, 300}, {1000, 30}]]
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
            application:set_env(riak_cs, lfs_block_size, BlockSize),
            {ok, Pid} = riak_cs_get_fsm:test_link(<<"bucket">>, <<"key">>, ContentLength, BlockSize),
            Manifest = riak_cs_get_fsm:get_manifest(Pid),
            ?assertEqual(ContentLength, Manifest?MANIFEST.content_length),
            riak_cs_get_fsm:continue(Pid),
            try
                expect_n_bytes(Pid, N, ContentLength)
            after
                riak_cs_get_fsm:stop(Pid)
            end
    end.

receives_manifest() ->
    {ok, Pid} = riak_cs_get_fsm:test_link(<<"bucket">>, <<"key">>, 100, 10),
    Manifest = riak_cs_get_fsm:get_manifest(Pid),
    ?assertEqual(100, Manifest?MANIFEST.content_length),
    riak_cs_get_fsm:stop(Pid).

%% ===================================================================
%% Helper Funcs
%% ===================================================================

%% expect_n_bytes(FsmPid, N) ->
expect_n_bytes(FsmPid, N, Bytes) ->
    {done, Res} = lists:foldl(
                    fun(_, {done, _} = Acc) ->
                            Acc;
                       (_, {working, L}) ->
                            case riak_cs_get_fsm:get_next_chunk(FsmPid) of
                                {chunk, X} ->
                                    {working, [X|L]};
                                {done, <<>>} ->
                                    {done, L}
                            end
                    end, {working, []}, lists:seq(1, Bytes)),
    ?assertMatch({N, Bytes}, {N, byte_size(iolist_to_binary(Res))}),
    %% dummy reader uses little endian to encode the sequence number
    %% in each chunk ... pull that seq num out, then check that usort
    %% yields the same thing.
    FirstBytes = lists:reverse([begin <<X:32/little, _/binary>> = Bin, X end ||
                                   Bin <- Res]),
    USorted = lists:usort(FirstBytes),
    ?assertMatch(FirstBytes, USorted).
