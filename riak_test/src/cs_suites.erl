%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% ---------------------------------------------------------------------

%% CS test suites

%% TODO: Can this be rt/rtcs-independent?

%% A suite is composed of several circles and single cleanup.
%%
%% Single circle run includes:
%% - creation of 5 buckets and 5 objects for each
%% - deletion of one bucket including objects in it
%% - PUT/GET/List/DELETE operations for pre-created buckets
%% - Access stats flush and storage stat calculation
%% - GC batch completion
%%
%% Cleanup includes:
%% - deletion of all objects and buckets ever created
%% - GC batch completion
%% - bitcask merge & delete
%% - confirmation that all data files become small enough

-module(cs_suites).

-export([new/1, fold_with_state/2, run/2, cleanup/1]).
-export([nodes_of/2,
         set_node1_version/2,
         admin_credential/1]).
-export([ops/0, reduced_ops/0]).

-export_type([state/0, op/0, tag/0]).
-type op() :: atom().
-type tag() :: string().

-include_lib("kernel/include/file.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("erlcloud/include/erlcloud_aws.hrl").

-record(state,
        {
          begin_at               :: string(),
          bucket_count = 5       :: pos_integer(),
          key_count    = 5       :: pos_integer(),
          ops          = ops()   :: [atom()],
          node1_cs_vsn = current :: previous | current,
          riak_vsn     = current :: current,
          riak_nodes             :: list(),
          cs_nodes               :: list(),
          stanchion_nodes        :: list(),
          admin_config           :: term(),
          prefix = "t-"          :: string(),
          circles = []           :: [circle()]
        }).
-opaque state() :: #state{}.

-record(bucket,
        {name      :: string(),
         count = 0 :: non_neg_integer()}).
-type bucket() :: #bucket{}.

-record(circle,
        {tag          :: string(),
          user_config  :: term(),
          buckets = [] :: [bucket()]}).
-type circle() :: #circle{}.

-define(GC_LEEWAY, 1).

-spec ops() -> [op()].
ops() -> [put_buckets,
          put_objects,
          put_objects_old,
          get_objects,
          get_objects_old,
          list_objects,
          list_objects_old,
          delete_bucket,
          delete_bucket_old,
          stats_access,
          stats_storage,
          gc,
          end_of_op].

-spec reduced_ops() -> [op()].
reduced_ops() -> [put_buckets,
                  put_objects,
                  end_of_op].

-spec cleanup_ops() -> [op()].
cleanup_ops() ->
    [delete_all,
     gc,
     merge,
     end_of_cleanup_op].

%% Create configuration state for subsequennt runs.
-spec new(term()) -> {ok, state()}.
new({AdminConfig, {RiakNodes, CSNodes, StanchionNode}}) ->
    new({AdminConfig, {RiakNodes, CSNodes, StanchionNode}}, ops()).

-spec new(term(), [op()]) -> {ok, state()}.
new({AdminConfig, {RiakNodes, CSNodes, StanchionNode}}, Ops) ->
    rt:setup_log_capture(hd(CSNodes)),
    rtcs:gc(1, "set-interval infinity"),
    Begin = rtcs:datetime(),
    %% FIXME: workaround for riak_cs#766
    timer:sleep(timer:seconds(1)),
    {ok, #state{begin_at = Begin,
                ops = Ops,
                riak_nodes = RiakNodes,
                cs_nodes = CSNodes,
                stanchion_nodes = [StanchionNode],
                admin_config = AdminConfig}}.

%% Utility functions to avoid many `StateN''s by appplying MFA lists sequentially
-spec fold_with_state(state(), [{module(), atom(), [term()]}]) -> {ok, state()}.
fold_with_state(State, []) ->
    {ok, State};
fold_with_state(State, [{M, F, A} | Rest]) ->
    {ok, NewState} = apply(M, F, A ++ [State]),
    fold_with_state(NewState, Rest).

-spec run(tag(), state()) -> {ok, state()}.
run(Tag, #state{ops=Ops} = State) ->
    run(Tag, Ops, State).

-spec cleanup(state()) -> {ok, state()}.
cleanup(State) ->
    Tag = "cleanup",
    run(Tag, cleanup_ops(), State).

run(Tag, Ops, State) ->
    lager:info("[~s] BEGIN", [Tag]),
    Circle = init_circle(Tag, State),
    {USec, {ok, NewState}} =
        timer:tc(fun() -> apply_operations(Circle, State, Ops) end),
    lager:info("[~s] END: ~B [msec]", [Tag, USec div 1000]),
    {ok, NewState}.

set_node1_version(Vsn, State) when Vsn =:= previous orelse Vsn =:= current ->
    {ok, State#state{node1_cs_vsn=Vsn}}.

admin_credential(#state{admin_config=AdminConfig}) ->
    {AdminConfig#aws_config.access_key_id,
     AdminConfig#aws_config.secret_access_key}.

-spec nodes_of(riak | stanchion | riak, state()) -> [node()].
nodes_of(riak,      State) -> State#state.riak_nodes;
nodes_of(stanchion, State) -> State#state.stanchion_nodes;
nodes_of(cs,        State) -> State#state.cs_nodes.

init_circle(Tag, #state{admin_config=AdminConfig, riak_nodes = [RiakNode|_]} = _State) ->
    Port = rtcs:cs_port(RiakNode),
    Name = concat("user-", Tag),
    Email = concat(Name, "@example.com"),
    {AccessKeyId, SecretAccessKey, _Id} =
        rtcs:create_user(Port, AdminConfig, Email, Name),
    UserConfig = rtcs:config(AccessKeyId, SecretAccessKey, Port),
    #circle{tag = Tag, user_config = UserConfig}.

-spec apply_operations(circle(), state(), [op()]) -> {ok, state()}.
apply_operations(_Circle, State, []) ->
    {ok, State};
apply_operations(#circle{tag=Tag} = Circle, State, [Op | Rest]) ->
    lager:info("[~s] Applying operation ~w ...", [Tag, Op]),
    {USec, {ok, NewCircle, NewState}} =
        timer:tc(fun() -> apply_operation(Op, Circle, State) end),
    lager:info("[~s] Finished operation ~w in ~B [msec]", [Tag, Op, USec div 1000]),
    apply_operations(NewCircle, NewState, Rest).

-spec apply_operation(op(), circle(), state()) -> {ok, circle(), state()}.
apply_operation(put_buckets, #circle{tag=Tag, user_config=UserConfig} = Circle,
                #state{bucket_count = Count, prefix=Prefix} = State) ->
    Buckets = [#bucket{name = concat(Prefix, Tag, to_str(Suffix))}
               || Suffix <- lists:seq(1, Count)],
    [?assertEqual(ok, erlcloud_s3:create_bucket(B#bucket.name, UserConfig)) ||
        B <- Buckets],
    {ok, Circle#circle{buckets = Buckets}, State};
apply_operation(put_objects, Circle,
                #state{key_count=KeyCount} = State) ->
    NewCircle = put_objects_to_every_bucket(KeyCount, Circle),
    {ok, NewCircle, State};
apply_operation(put_objects_old, CurrentCircle,
                #state{key_count=KeyCount, circles=Circles} = State) ->
    NewCircles = [put_objects_to_every_bucket(KeyCount, Circle) || Circle <- Circles],
    {ok, CurrentCircle, State#state{circles=NewCircles}};
apply_operation(get_objects, Circle, State) ->
    NewCircle = get_objects_from_every_bucket(Circle),
    {ok, NewCircle, State};
apply_operation(get_objects_old, CurrentCircle,
                #state{circles=Circles} = State) ->
    NewCircles = [get_objects_from_every_bucket(Circle) || Circle <- Circles],
    {ok, CurrentCircle, State#state{circles=NewCircles}};
apply_operation(list_objects, Circle, State) ->
    NewCircle = list_objects_from_every_bucket(Circle),
    {ok, NewCircle, State};
apply_operation(list_objects_old, CurrentCircle, #state{circles=Circles} = State) ->
    NewCircles = [list_objects_from_every_bucket(Circle) || Circle <- Circles],
    {ok, CurrentCircle, State#state{circles=NewCircles}};
apply_operation(delete_bucket, Circle, State) ->
    NewCircle = delete_first_bucket(Circle),
    {ok, NewCircle, State};
apply_operation(delete_bucket_old, CurrentCircle, #state{circles=Circles} = State) ->
    NewCircles = [delete_first_bucket(Circle) || Circle <- Circles],
    {ok, CurrentCircle, State#state{circles=NewCircles}};
apply_operation(stats_access, Circle, State) ->
    Res = rtcs:flush_access(1),
    lager:info("riak-cs-access flush result: ~s", [Res]),
    ExpectRegexp = "All access logs were flushed.\n$",
    ?assertMatch({match, _}, re:run(Res, ExpectRegexp)),
    %% TODO
    %% - Get access stats and assert them with real access generated so far
    {ok, Circle, State};
apply_operation(stats_storage, CurrentCircle,
                #state{admin_config=AdminConfig, begin_at=Begin,
                       cs_nodes=[CSNode|_], circles=Circles} = State) ->
    Res = rtcs:calculate_storage(1),
    lager:info("riak-cs-storage batch result: ~s", [Res]),
    ExpectRegexp = "Batch storage calculation started.\n$",
    ?assertMatch({match, _}, re:run(Res, ExpectRegexp)),
    true = rt:expect_in_log(CSNode, "Finished storage calculation"),
    %% FIXME: workaround for riak_cs#766
    timer:sleep(timer:seconds(2)),
    End = rtcs:datetime(),
    [get_storage_stats(AdminConfig, Begin, End, Circle) || Circle <- [CurrentCircle|Circles]],
    {ok, CurrentCircle, State};
apply_operation(gc, Circle, #state{cs_nodes=[CSNode|_]} = State) ->
    timer:sleep(timer:seconds(?GC_LEEWAY + 1)),
    rtcs:gc(1, "batch 1"),
    ok = rt:wait_until(
           CSNode,
           fun(_N) ->
                   Res = rtcs:gc(1, "status"),
                   ExpectSubstr = "There is no garbage collection in progress",
                   case string:str(Res, ExpectSubstr) of
                       0 ->
                           {match, Captured} =
                               re:run(Res, "Elapsed time of current run: [0-9]*\n",
                                      [{capture, first, binary}]),
                           lager:debug("riak-cs-gc status: ~s", [Captured]),
                           false;
                       _ ->
                           lager:debug("GC completed"),
                           true
                   end
           end),
    %% TODO: Calculate manif_count and block_count and assert them specifically
    %% true = rt:expect_in_log(CSNode,
    %%                         "Finished garbage collection: \\d+ seconds, "
    %%                         "\\d+ batch_count, \\d+ batch_skips, "
    %%                         "\\d+ manif_count, \\d+ block_count"),
    {ok, Circle, State};
apply_operation(end_of_op, Circle,
                #state{node1_cs_vsn=Vsn, circles=SoFar} = State) ->
    rtcs:assert_error_log_empty(Vsn, 1),
    {ok, Circle, State#state{circles=[Circle|SoFar]}};

apply_operation(delete_all, CurrentCircle, #state{circles=Circles} = State) ->
    NewCircles = [delete_all_buckets(Circle) || Circle <- Circles],
    {ok, CurrentCircle, State#state{circles=NewCircles}};
apply_operation(merge, CurrentCircle, State) ->
    merge_all_bitcask(State),
    {ok, CurrentCircle, State};
apply_operation(end_of_cleanup_op, CurrentCircle, State) ->
    {ok, CurrentCircle, State}.

-spec put_objects_to_every_bucket(non_neg_integer(), circle()) -> circle().
put_objects_to_every_bucket(KeyCount,
                            #circle{user_config=UserConfig,
                                      buckets=Buckets} = Circle) ->
    NewBuckets = [put_objects(KeyCount, UserConfig, B) || B <- Buckets],
    Circle#circle{buckets=NewBuckets}.

-spec put_objects(non_neg_integer(), #aws_config{}, bucket()) -> bucket().
put_objects(KeyCount, UserConfig, #bucket{name=B, count=Before} = Bucket) ->
    [case bk_to_body(B, K) of
         Body when is_binary(Body) ->
             erlcloud_s3:put_object(B, to_str(K), Body, UserConfig);
         Parts when is_list(Parts) ->
             multipart_upload(B, to_str(K), Parts, UserConfig);
         {copy, {SrcB, SrcK}} ->
             ?assertEqual([{copy_source_version_id, "false"},
                           {version_id, "null"}],
                          erlcloud_s3:copy_object(B, to_str(K), SrcB, to_str(SrcK), UserConfig))
     end ||
        K <- lists:seq(Before + 1, Before + KeyCount) ],
    Bucket#bucket{count = Before + KeyCount}.

multipart_upload(Bucket, Key, Parts, Config) ->
    InitRes = erlcloud_s3_multipart:initiate_upload(
                Bucket, Key, "text/plain", [], Config),
    UploadId = erlcloud_xml:get_text(
                 "/InitiateMultipartUploadResult/UploadId", InitRes),
    upload_parts(Bucket, Key, UploadId, Config, 1, Parts, []).

upload_parts(Bucket, Key, UploadId, UserConfig, _PartCount, [], PartEtags) ->
    ?assertEqual(ok, erlcloud_s3_multipart:complete_upload(
                       Bucket, Key, UploadId, lists:reverse(PartEtags), UserConfig)),
    ok;
upload_parts(Bucket, Key, UploadId, UserConfig, PartCount, [Part | Parts], PartEtags) ->
    {RespHeaders, _UploadRes} = erlcloud_s3_multipart:upload_part(
                                  Bucket, Key, UploadId, PartCount, Part, UserConfig),
    PartEtag = proplists:get_value("ETag", RespHeaders),
    upload_parts(Bucket, Key, UploadId, UserConfig, PartCount + 1,
                 Parts, [{PartCount, PartEtag} | PartEtags]).


-spec get_objects_from_every_bucket(circle()) -> circle().
get_objects_from_every_bucket(#circle{user_config=UserConfig,
                                        buckets=Buckets} = Circle) ->
    NewBuckets = [get_objects(UserConfig, Bucket)|| Bucket <- Buckets],
    Circle#circle{buckets=NewBuckets}.

%% TODO: More variants, e.g. range, conditional
-spec get_objects(#aws_config{}, bucket()) -> bucket().
get_objects(UserConfig, #bucket{name=B, count=KeyCount} = Bucket) ->
    [begin
         GetResponse = erlcloud_s3:get_object(B, to_str(K), UserConfig),
         Expected = case bk_to_body(B, K) of
                        Body when is_binary(Body) -> Body;
                        Parts when is_list(Parts) -> to_bin(Parts);
                        {copy, {SrcB, SrcK}}      -> bk_to_body(SrcB, SrcK)
                    end,
         case proplists:get_value(content, GetResponse) of
             Expected -> ok;
             Other    ->
                 lager:error("Unexpected contents for bucket=~s, key=~s", [B, K]),
                 lager:error("First 100 bytes of expected content~n~s",
                             [first_n_bytes(Expected, 100)]),
                 lager:error("First 100 bytes of actual content~n~s",
                             [first_n_bytes(Other, 100)]),
                 error({content_unmatched, B, K})
         end
     end ||
        K <- lists:seq(1, KeyCount)],
    Bucket.

%% TODO: repeating GET Bucket calls by small (<5) `max-keys' param
list_objects_from_every_bucket(#circle{user_config=UserConfig,
                                         buckets=Buckets} = Circle) ->
    Opts = [],
    [begin
         KeyCount = Bucket#bucket.count,
         Response = erlcloud_s3:list_objects(Bucket#bucket.name, Opts, UserConfig),
         Result = proplists:get_value(contents, Response),
         ?assertEqual(KeyCount, length(Result))
     end ||
        Bucket <- Buckets],
    Circle.

%% Delete objects in the first bucket if exists, leave untouched the rest.
-spec delete_first_bucket(circle()) -> circle().
delete_first_bucket(#circle{buckets=[]} = Circle) ->
    Circle;
delete_first_bucket(#circle{user_config=UserConfig,
                              buckets=[Bucket|UntouchedBucket]} = Circle) ->
    delete_bucket(UserConfig, Bucket),
    Circle#circle{buckets=UntouchedBucket}.

delete_all_buckets(#circle{buckets=[]} = Circle) ->
    Circle;
delete_all_buckets(#circle{user_config=UserConfig,
                              buckets=[Bucket|RestBucket]} = Circle) ->
    delete_bucket(UserConfig, Bucket),
    delete_all_buckets(Circle#circle{buckets=RestBucket}).

delete_bucket(UserConfig, Bucket) ->
    B = Bucket#bucket.name,
    KeyCount = Bucket#bucket.count,
    [erlcloud_s3:delete_object(B, to_str(K), UserConfig) ||
        K <- lists:seq(1, KeyCount)],
    ListResponse = erlcloud_s3:list_objects(B, [], UserConfig),
    Contents = proplists:get_value(contents, ListResponse),
    ?assertEqual(0, length(Contents)),
    ?assertEqual(ok, erlcloud_s3:delete_bucket(B, UserConfig)).

get_storage_stats(AdminConfig, Begin, End,
                  #circle{user_config=UserConfig, buckets=Buckets} = Circle) ->
    lager:debug("storage stats for user ~s , ~s/~s",
                [UserConfig#aws_config.access_key_id, Begin, End]),
    Expected = lists:sort(
                 [{B, {Count, bucket_bytes(B, Count)}} ||
                     #bucket{name=B, count=Count} <- Buckets]),
    Actual = rtcs_admin:storage_stats_json_request(AdminConfig, UserConfig,
                                                   Begin, End),
    ?assertEqual(Expected, Actual),
    Circle.

first_n_bytes(Binary, ByteSize) ->
    binary:part(Binary, 0, math:max(byte_size(Binary), ByteSize)).

-spec bk_to_body(string(), integer()) -> binary() | [binary()].
bk_to_body(B, K) ->
    case os:getenv("CS_RT_DEBUG") of
        "true" ->
            %% Trick to make duration/stacktrace smaller for debugging (/ω・＼)
            bk_to_body_debug(B, K);
        _ ->
            %% This branch should be used normally
            bk_to_body_actual(B, K)
    end.

%% Generate object body by modulo 5 of `K::integer()':
%% 1    : multiple blocks  (3 MB)
%% 2    : Multipart Upload (2 parts * 5 MB/part)
%% 3    : Put Copy of `1'  (3 MB)
%% 4, 0 : small objects    (~ 10 KB)
-spec bk_to_body_actual(string(), integer()) ->
                               binary() | [binary()] | {copy, {string(), integer()}}.
bk_to_body_actual(_B, K) when (K rem 5) =:= 1 -> binary:copy(<<"A">>, mb(3));
bk_to_body_actual(_B, K) when (K rem 5) =:= 2 -> lists:duplicate(2, binary:copy(<<"A">>, mb(5)));
bk_to_body_actual(B, K)  when (K rem 5) =:= 3 -> {copy, {B, 1}};
bk_to_body_actual(B, K)                       -> binary:copy(lead(B, K), 1024).

-spec bk_to_body_debug(string(), integer()) -> binary() | [binary()].
bk_to_body_debug(B, K) ->
    lead(B, K).

lead(B, K) ->
    to_bin([B, $:, to_str(K), $\n]).

%% Calculate total byte size for the bucket
-spec bucket_bytes(string(), non_neg_integer()) -> non_neg_integer().
bucket_bytes(B, Count) ->
    lists:sum([obj_bytes(B, K) || K <- lists:seq(1, Count)]).

%% Returns byte size of the binary that will be returned by
%% `bk_to_body(B, K)'.
-spec obj_bytes(string(), integer()) -> non_neg_integer().
obj_bytes(B, K) ->
    case os:getenv("CS_RT_DEBUG") of
        "true" ->
            %% Use OS env var to restrict object body to small binary
            %% for debugging. no multi-MB obj, no Multipart, no PUT Copy.
            byte_size(bk_to_body_debug(B, K));
        _ ->
            obj_bytes_actual(B, K)
    end.

obj_bytes_actual(_B, K) when (K rem 5) =:= 1 -> mb(3);
obj_bytes_actual(_B, K) when (K rem 5) =:= 2 -> 2 * mb(5);
obj_bytes_actual(B, K)  when (K rem 5) =:= 3 -> obj_bytes_actual(B, 1);
obj_bytes_actual(B, K)                       -> kb(byte_size(lead(B, K))).

kb(KB) -> KB * 1024.

mb(MB) -> MB * 1024 * 1024.

merge_all_bitcask(#state{riak_nodes=Nodes, riak_vsn=Vsn} = _State) ->
    wait_until_merge_worker_idle(Nodes),
    [trigger_bitcask_merge(N, Vsn) || N <- Nodes],
    [ok = rt:wait_until(N,
                        fun(Node) ->
                                check_data_files_are_small_enough(Node, Vsn)
                        end) || N <- Nodes],
    ok.

wait_until_merge_worker_idle(Nodes) ->
    [ok = rt:wait_until(
            N,
            fun(Node) ->
                    Status = rpc:call(Node, bitcask_merge_worker, status, []),
                    lager:debug("Wait util bitcask_merge_worker to finish on ~p:"
                                " status=~p~n", [Node, Status]),
                    Status =:= {0, undefined}
            end) || N <- Nodes],
    ok.

trigger_bitcask_merge(Node, Vsn) ->
    lager:debug("Trigger bitcask merger for node ~s (version: ~s)~n", [Node, Vsn]),
    VnodeNames = bitcask_vnode_names(Node, Vsn),
    [begin
         VnodeDir = filename:join(bitcask_data_root(), VnodeName),
         DataFiles = bitcask_data_files(Node, Vsn, VnodeName, rel),
         ok = rpc:call(Node, bitcask_merge_worker, merge,
                       [VnodeDir, [], {DataFiles, []}])
     end ||
        VnodeName <- VnodeNames],
    ok.

bitcask_data_root() -> "./data/bitcask".

%% Returns vnode dir name,
%% For example: "548063113999088594326381812268606132370974703616"
-spec bitcask_vnode_names(atom(), previous | current) -> [string()].
bitcask_vnode_names(Node, Vsn) ->
    Prefix = rtcs:get_rt_config(riak, Vsn),
    BitcaskAbsRoot = rtcs:riak_bitcaskroot(Prefix, rt_cs_dev:node_id(Node)),
    {ok, VnodeDirNames} = file:list_dir(BitcaskAbsRoot),
    VnodeDirNames.

%% Data file names should start bitcask's `data_root' for `bitcask_merge_worker'.
%% This function returns a list proper for the aim, e.g.
%% ["./data/bitcask/548063113999088594326381812268606132370974703616/5.bitcask.data",
%%  "./data/bitcask/548063113999088594326381812268606132370974703616/6.bitcask.data"]
-spec bitcask_data_files(node(), previous | current, string(), abs | rel) ->
                                [string()].
bitcask_data_files(Node, Vsn, VnodeName, AbsOrRel) ->
    Prefix = rtcs:get_rt_config(riak, Vsn),
    BitcaskAbsRoot = rtcs:riak_bitcaskroot(Prefix, rt_cs_dev:node_id(Node)),
    VnodeAbsPath = filename:join(BitcaskAbsRoot, VnodeName),
    {ok, Fs0} = file:list_dir(VnodeAbsPath),
    [case AbsOrRel of
         rel -> filename:join([bitcask_data_root(), VnodeName, F]);
         abs -> filename:join([BitcaskAbsRoot, VnodeName, F])
     end ||
        F <- Fs0, filename:extension(F) =:= ".data"].

%% Assert bitcask data is "small"
%% 1) The number of *.data files should be =< 2
%% 2) Each *.data file size should be < 32 KiB
check_data_files_are_small_enough(Node, Vsn) ->
    VnodeNames = bitcask_vnode_names(Node, Vsn),
    check_data_files_are_small_enough(Node, Vsn, VnodeNames).

check_data_files_are_small_enough(_Node, _Vsn, []) ->
    true;
check_data_files_are_small_enough(Node, Vsn, [VnodeName|Rest]) ->
    DataFiles = bitcask_data_files(Node, Vsn, VnodeName, abs),
    FileSizes = [begin
                     {ok, #file_info{size=S}} = file:read_file_info(F),
                     {F, S}
                 end || F <- DataFiles],
    lager:debug("FileSizes (~p): ~p~n", [{Node, VnodeName}, FileSizes]),
    TotalSize = lists:sum([S || {_, S} <- FileSizes]),
    case {length(FileSizes) =< 2, TotalSize < 32*1024} of
        {true, true} ->
            lager:info("bitcask data file check OK for ~p ~p",
                       [Node, VnodeName]),
            check_data_files_are_small_enough(Node, Vsn, Rest);
        {false, _} ->
            lager:info("bitcask data file check failed, count(files)=~p for ~p ~p",
                       [Node, VnodeName, length(FileSizes)]),
            false;
        {_, false} ->
            lager:info("bitcask data file check failed, sum(file size)=~p for ~p ~p",
                       [Node, VnodeName, TotalSize]),
            false
    end.

%% Misc utilities

concat(IoList1, IoList2) ->
    concat([IoList1, IoList2]).

concat(IoList1, IoList2, IoList3) ->
    concat([IoList1, IoList2, IoList3]).

concat(IoList) ->
    lists:flatten(IoList).

to_str(Int) when is_integer(Int) ->
    integer_to_list(Int);
to_str(String) when is_list(String) ->
    String;
to_str(Bin) when is_binary(Bin) ->
    binary_to_list(Bin).

to_bin(Int) when is_integer(Int) ->
    to_bin(integer_to_list(Int));
to_bin(String) when is_list(String) ->
    iolist_to_binary(String);
to_bin(Bin) when is_binary(Bin) ->
    Bin.
