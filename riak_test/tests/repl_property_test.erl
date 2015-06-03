-module(repl_property_test).

-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, "riak-test-bucket").

%% @doc This test ensures validity of replication and garbage
%% collection, where all deleted data should be deleted under any type
%% of failure or race condition. This test may run for serveral or
%% infinite iterations (and also has a knob to notify otherwise)
%% 
%% Basic idea to verify the system is:
%%
%% 0. Set up 2x2 repl environment, unidirection (or should be bi?)
%% 1. run some workloads, including
%%   - Garbage collection batch
%%   - Replication fullsync, realtime
%%   - Fault or delay injection via intercepter or kill
%% 2. stop all workloads
%% 3. verify no orphan blocks remaining and all live data consistent
%% 4. back to 1.
%%
%% Configuration items for this test, in riak_test.config:
%%
%% {repl_property_test,
%%   [{iteration, non_neg_integer()|forever},
%%    {fault_injection, ??},
%%    {rtq_drop, ??}
%%    {notify, 'where to go?'},
%%    {bidirectional, boolean()}]},
%%
%% any other knobs described in code below;

-record(repl_property_test_config, {
          iteration = 1 :: non_neg_integer() | forever,
          fault_injection = false :: boolean(),
          rtq_drop_percentage = 0 :: non_neg_integer() ,
          notify = undefined :: undefined,
          bidirectional = false :: boolean()}).

repl_property_test_config() ->
    Config = rt_config:get(repl_property_test, []),
    lists:foldl(fun({iteration, I}, Prop) ->  Prop#repl_property_test_config{iteration=I};
                   (_, Prop) -> Prop end, #repl_property_test_config{}, Config).                         

confirm() ->
    case rtcs:longrun_duration() of
        undefined ->
            lager:info("Won't run ~s as RIAK_CS_LONGRUN_DURATION is not defined.",
                       [?MODULE]),
            rtcs:pass();
        Other ->
            confirm(Other, os:timestamp())
    end.

confirm(DurationMinutes, StartTime) ->
    %% Config = [{stanchion, rtcs:stanchion_config()},
    %%           {cs, rtcs:cs_config([{gc_interval, infinity}])}],
    {AdminConfig, {RiakNodes, CSNodes, Stanchion}} = rtcs:setup2x2(),
    % lager:info("UserConfig = ~p", [UserConfig]),
    [A,B,C,D] = RiakNodes,
    [CSSource, _, CSSink, _] = CSNodes,
    rt:setup_log_capture(CSSink),
    rt:setup_log_capture(CSSource),

    TestConfig = repl_property_test_config(),
    lager:info("repl_property_test configuraiton: ~p", [TestConfig]),

    SourceNodes = [A,B],
    SinkNodes = [C,D],
    lager:info("SourceNodes: ~p", [SourceNodes]),
    lager:info("SinkNodes: ~p", [SinkNodes]),

    [E, F, G, H] = CSNodes,
    _SourceCSNodes = [E, F],
    _SinkCSNodes = [G, H],

    SourceFirst = hd(SourceNodes),
    SinkFirst = hd(SinkNodes),

    SourceFirst = hd(SourceNodes),
    SinkFirst = hd(SinkNodes),

    %% Setup All replication here
    {ok, {SourceLeader, SinkLeader}} =
        repl_helpers:configure_all_repl(SourceNodes, "source",
                                        SinkNodes, "sink"),

    History = 
        [{cs_suites, set_node1_version, [current]},
         {cs_suites, run,["1st"]}],

    {ok, InitialState} = cs_suites:new({AdminConfig,
                                        {RiakNodes, CSNodes, Stanchion}}),
    {ok, EvolvedState} = cs_suites:fold_with_state(InitialState, History),

    %% LeaderA = SourceLeader, _LeaderB = SinkLeader,
    maybe_repeat(EvolvedState, {SourceLeader, SinkLeader}, DurationMinutes, StartTime, 0).
           %% TestConfig#repl_property_test_config.iteration, 1).

maybe_repeat(State, Leaders, forever, StartTime, Count) ->
    {ok, NextState} = repeat(State, Leaders, Count),
    maybe_repeat(NextState, Leaders, forever, StartTime, Count + 1);
    
maybe_repeat(State, Leaders, DurationMinutes, StartTime, Count) ->
    Now = os:timestamp(),
    DurationSecs = DurationMinutes * 60,
    case timer:now_diff(Now, StartTime) of
        Diff when Diff > DurationSecs * 1000000 ->
            lager:info("done."),
            {ok, _FinalState}  = cs_suites:cleanup(State),
            rtcs:pass();
        _ ->
            {ok, NextState} = repeat(State, Leaders, Count),
            maybe_repeat(NextState, Leaders, DurationMinutes, StartTime, Count + 1)
    end.

repeat(State, _Leaders = {_SourceLeader, _SinkLeader}, Count) ->

    Str = io_lib:format("~p-th turn", [Count]),
    Repeat =
        [{cs_suites, run,[Str]},
         %%{?MODULE, fullsync, [SourceLeader]},
         %%{?MODULE, run_gc, []},
         {?MODULE, verify_iteration, []}],

    cs_suites:fold_with_state(State, Repeat).

fullsync(SourceLeader, State) ->
    repl_helpers:start_and_wait_until_fullsync_complete13(SourceLeader),
    {ok, State}.

run_gc(State) ->
    %% _UserConfig = cs_suites:admin_config(State),
    [SourceCSNode, _B,SinkCSNode, _D] = cs_suites:nodes_of(cs, State),
    %% %% wait for leeway
    timer:sleep(5000),
    %% Run GC in Sink side
    start_and_wait_for_gc(SourceCSNode),
    start_and_wait_for_gc(SinkCSNode),
    {ok, State}.

verify_iteration(State) ->
    _UserConfig = cs_suites:admin_config(State),
    [A, B, C, D] = cs_suites:nodes_of(riak, State),
    SourceNodes = [A, B],
    _SinkNodes = [C, D],
    %% Make sure no blocks are in Source side
    {ok, Keys0} = rc_helper:list_keys(SourceNodes, blocks, ?TEST_BUCKET),
    ?assertEqual([],
                 rc_helper:filter_tombstones(SourceNodes, blocks,
                                             ?TEST_BUCKET, Keys0)),
    
    %% Boom!!!!
    %% Make sure no blocks are in Sink side: will fail in Riak CS 2.0
    %% or older. Or turn on {delete_mode, keep}
    %% {ok, Keys1} = rc_helper:list_keys(SinkNodes, blocks, ?TEST_BUCKET),
    %% ?assertEqual([],
    %%              rc_helper:filter_tombstones(SinkNodes, blocks,
    %%                                          ?TEST_BUCKET, Keys1)),

    Home = rtcs:riakcs_home(rtcs:get_rt_config(cs, current), 1),
    os:cmd("rm -rf " ++ filename:join([Home, "maybe-orphaned-blocks"])),
    os:cmd("rm -rf " ++ filename:join([Home, "actual-orphaned-blocks"])),
    Res1 = rtcs:exec_priv_escript(1, "internal/block_audit.erl",
                                  "-h 127.0.0.1 -p 10017 -dd"),
    lager:debug("block_audit.erl log:\n~s", [Res1]),
    lager:debug("block_audit.erl log:============= END"),
    %% fake_false_orphans(RiakNodes, FalseOrphans1 ++ FalseOrphans2),
    Res2 = rtcs:exec_priv_escript(1, "internal/ensure_orphan_blocks.erl",
                                  "-h 127.0.0.1 -p 10017 -dd"),
    lager:debug("ensure_orphan_blocks.erl log:\n~s", [Res2]),
    lager:debug("ensure_orphan_blocks.erl log:============= END"),

    {ok, Dir} = file:list_dir(filename:join([Home, "actual-orphaned-blocks"])),
    lager:info("~p", [Dir]),
    ?assertEqual([], Dir),
    %% OutFile1 = filename:join([Home, "actual-orphaned-blocks", Bucket]),
    %% {ok, Bin} = file:read_file(OutFile1),
    %% lager:info("~p", [Bin]),
    %% ?assertEqual(<<>>, Bin),

    {ok, State}.


start_and_wait_for_gc(CSNode) ->
    rtcs:gc(rtcs:node_id(CSNode), "batch 1"),
    true = rt:expect_in_log(CSNode,
                            "Finished garbage collection: \\d+ seconds, "
                            "\\d+ batch_count, 0 batch_skips, "
                            "\\d+ manif_count, \\d+ block_count").

    %% lager:info("UserConfig ~p", [U1C1Config]),
    %% ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(U1C1Config)),

    %% lager:info("creating bucket ~p", [?TEST_BUCKET]),
    %% ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, U1C1Config)),

    %% ?assertMatch([{buckets, [[{name, ?TEST_BUCKET}, _]]}],
    %%              erlcloud_s3:list_buckets(U1C1Config)),

    %% Object1 = crypto:rand_bytes(4194304),

    %% erlcloud_s3:put_object(?TEST_BUCKET, "object_one", Object1, U1C1Config),

    %% ObjList2 = erlcloud_s3:list_objects(?TEST_BUCKET, U1C1Config),
    %% ?assertEqual(["object_one"],
    %%              [proplists:get_value(key, O) ||
    %%                  O <- proplists:get_value(contents, ObjList2)]),
    
    %% Obj = erlcloud_s3:get_object(?TEST_BUCKET, "object_one", U1C1Config),
    %% ?assertEqual(Object1, proplists:get_value(content, Obj)),


    %% lager:info("User 1 has the test bucket on the secondary cluster now"),
    %% ?assertMatch([{buckets, [[{name, ?TEST_BUCKET}, _]]}],
    %%              erlcloud_s3:list_buckets(U1C2Config)),

    %% lager:info("Object written on primary cluster is readable from Source"),
    %% Obj2 = erlcloud_s3:get_object(?TEST_BUCKET, "object_one", U1C1Config),
    %% ?assertEqual(Object1, proplists:get_value(content, Obj2)),

    %% lager:info("check we can still read the fullsynced object"),
    %% Obj3 = erlcloud_s3:get_object(?TEST_BUCKET, "object_one", U1C2Config),
    %% ?assertEqual(Object1, proplists:get_value(content, Obj3)),

    %% lager:info("delete object_one in Source"),
    %% erlcloud_s3:delete_object(?TEST_BUCKET, "object_one", U1C1Config),

    %% %%lager:info("object_one is still visible on secondary cluster"),
    %% %%Obj9 = erlcloud_s3:get_object(?TEST_BUCKET, "object_one", U1C2Config),
    %% %%?assertEqual(Object1, proplists:get_value(content, Obj9)),


    %% %% Propagate deleted manifests
    %% repl_helpers:start_and_wait_until_fullsync_complete13(LeaderA),

    %% lager:info("object_one is invisible in Sink"),
    %% ?assertError({aws_error, _},
    %%              erlcloud_s3:get_object(?TEST_BUCKET, "object_one", U1C2Config)),

    %% lager:info("object_one is invisible in Source"),
    %% ?assertError({aws_error, _},
    %%              erlcloud_s3:get_object(?TEST_BUCKET, "object_one", U1C1Config)),

    %% lager:info("secondary cluster now has 'A' version of object four"),


    %% Verify no blocks are in sink and source

