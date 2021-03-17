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

%% @doc MapReduce functions for storage calculation

-module(riak_cs_storage_mr).

-include("riak_cs.hrl").

%% Utility for M/R client
-export([empty_summary/0]).
-export([bucket_summary_map/3,
         bucket_summary_reduce/2]).
-export([object_size_map/3,
         object_size_reduce/2]).

-export([objs_bytes_and_blocks/1]).

-ifdef(TEST).
-export([object_size/1,
         count_multipart_parts/1]).
-endif.

%% Record for summary information, each 3-tuple represents object
%% count, total bytes and total blocks.  Some of them are estimated
%% value, maybe (over|under)-estimated.
%% @see objs_bytes_and_blocks/1 for details of calculation/estimation
-record(sum,
        {
          %% User accessible objects, which includes active and
          %% writing of multipart.
          user = {0, 0, 0},
          %% Most recent active's (double count with `user')
          active = {0, 0, 0},
          %% active's those are invisible, older than user accessible
          active_invisible = {0, 0, 0},
          %% MP writing (double count with `user')
          writing_multipart = {0, 0, 0},
          %% non-MP writing, divided by <> leeway
          writing_new = {0, 0, 0},
          writing_old = {0, 0, 0},
          %% pending_delete, divided by <> leeway
          pending_delete_new = {0, 0, 0},
          pending_delete_old = {0, 0, 0},
          %% scheduled_delete, divided by <> leeway
          scheduled_delete_new = {0, 0, 0},
          scheduled_delete_old = {0, 0, 0}
        }).

-type sum() :: #sum{}.

-spec empty_summary() -> proplists:proplist().
empty_summary() ->
    summary_to_list(#sum{}).

bucket_summary_map({error, notfound}, _, _Args) ->
    [];
bucket_summary_map(Object, _, Args) ->
    LeewayEdge = proplists:get_value(leeway_edge, Args),
    Summary = riak_cs_utils:maybe_process_resolved(
                Object, fun(History) -> sum_objs(LeewayEdge, History) end, #sum{}),
    Res = summary_to_list(Summary),
    [Res].

object_size_map({error, notfound}, _, _) ->
    [];
object_size_map(Object, _, _) ->
    Handler = fun(Resolved) -> object_size(Resolved) end,
    riak_cs_utils:maybe_process_resolved(Object, Handler, []).

object_size_reduce(Sizes, _) ->
    {Objects,Bytes} = lists:unzip(Sizes),
    [{lists:sum(Objects),lists:sum(Bytes)}].

%% Internal

-spec sum_objs(erlang:timestamp(), [cs_uuid_and_manifest()]) -> sum().
sum_objs(LeewayEdge, History) ->
    case riak_cs_manifest_utils:active_manifest(History) of
        {ok, Active} ->
            {_, _, OBB} = objs_bytes_and_blocks(Active),
            Sum0 = add_to(#sum{}, #sum.user, OBB),
            Sum = add_to(Sum0, #sum.active, OBB),
            NonActiveHistory = lists:keydelete(Active?MANIFEST.uuid, 1, History),
            sum_objs(LeewayEdge, Sum, NonActiveHistory);
        _ ->
            sum_objs(LeewayEdge, #sum{}, History)
    end.

sum_objs(_LeewayEdge, Sum, []) ->
    Sum;
sum_objs(LeewayEdge, Sum, [{_UUID, M} | Rest]) ->
    NewSum = case objs_bytes_and_blocks(M) of
                 {_, active, OBB} ->
                     %% Because user accessible active manifest had
                     %% been removed in `sum_objs/2', active's here are
                     %% invisible for users.
                     add_to(Sum, #sum.active_invisible, OBB);
                 {mp, writing, OBB} ->
                     %% MP writing is visible for user. Also add
                     %% to MP writing counters.
                     Sum1 = add_to(Sum, #sum.user, OBB),
                     add_to(Sum1, #sum.writing_multipart, OBB);
                 {non_mp, writing, OBB} ->
                     case new_or_old(LeewayEdge, M?MANIFEST.write_start_time) of
                         new ->
                             add_to(Sum, #sum.writing_new, OBB);
                         old ->
                             add_to(Sum, #sum.writing_old, OBB)
                     end;
                 {_, pending_delete, OBB} ->
                     case new_or_old(LeewayEdge, M?MANIFEST.delete_marked_time) of
                         new ->
                             add_to(Sum, #sum.pending_delete_new, OBB);
                         old ->
                             add_to(Sum, #sum.pending_delete_old, OBB)
                     end;
                 {_, scheduled_delete, OBB} ->
                     case new_or_old(LeewayEdge, M?MANIFEST.delete_marked_time) of
                         new ->
                             add_to(Sum, #sum.scheduled_delete_new, OBB);
                         old ->
                             add_to(Sum, #sum.scheduled_delete_old, OBB)
                     end
             end,
    sum_objs(LeewayEdge, NewSum, Rest).

%% @doc count objects, bytes and blocks for manifests
-spec objs_bytes_and_blocks(lfs_manifest()) ->
                                   {non_mp | mp, State::atom(),
                                    {Objects::non_neg_integer(),
                                     Bytes::non_neg_integer(),
                                     Blocks::non_neg_integer()}}.
objs_bytes_and_blocks(?MANIFEST{props=Props} = M) when is_list(Props) ->
    case proplists:get_value(multipart, Props) of
        ?MULTIPART_MANIFEST{} = MpM -> objs_bytes_and_blocks_mp(M, MpM);
        _ -> objs_bytes_and_blocks_non_mp(M)
    end;
objs_bytes_and_blocks(M) ->
    objs_bytes_and_blocks_non_mp(M).

%% @doc count objects, bytes and blocks, non-MP version
objs_bytes_and_blocks_non_mp(?MANIFEST{state=State, content_length=CL, block_size=BS})
  when is_integer(CL) andalso is_integer(BS) ->
    BlockCount = riak_cs_lfs_utils:block_count(CL, BS),
    {non_mp, State, {1, CL, BlockCount}};
objs_bytes_and_blocks_non_mp(?MANIFEST{state=State} = _M) ->
    logger:debug("Strange manifest: ~p~n", [_M]),
    %% The branch above is for content_length is properly set.  This
    %% is true for non-MP v2 auth case but not always true for v4 of
    %% streaming sha256 check of writing. To avoid error, ignore
    %% this objects. Can this be guessed better from write_blocks_remaining?
    {non_mp, State, {1, 0, 0}}.

%% @doc counting parts, bytes and blocks, multipart version
%%
%% There are possibility of understimatation and overestimation for
%% Multipart cases.
%%
%% In writing state, there are two active fields in multipart
%% manifests, `parts' and `done_parts'. To count bytes and blocks,
%% `parts' is used here, these counts may be overestimate because
%% `parts' includes unfinished blocks. We could use `done_parts'
%% instead, then counts may be underestimate because it does not
%% include unfinished ones.
%%
%% Once MP turned into active state, unused parts had already been
%% gone to GC bucket. These UUIDs are remaining in cleanup_parts, but
%% we can't know whether correspoinding blocks have been GC'ed or not,
%% because dummy manifests for `cleanup_parts' have been inserted to
%% GC bucket directly and no object in manifest buckets.  We don't
%% count cleanup_parts here.
objs_bytes_and_blocks_mp(?MANIFEST{state=writing},
                    ?MULTIPART_MANIFEST{}=MpM) ->
    {mp, writing, {part_count(MpM), bytes_mp_parts(MpM), blocks_mp_parts(MpM)}};
objs_bytes_and_blocks_mp(?MANIFEST{state=State, content_length=CL},
                    ?MULTIPART_MANIFEST{}=MpM)
  when State =:= active andalso is_integer(CL) ->
    {mp, State, {1, CL, blocks_mp_parts(MpM)}};
objs_bytes_and_blocks_mp(?MANIFEST{state=State},
                    ?MULTIPART_MANIFEST{}=MpM) ->
    {mp, State, {1, bytes_mp_parts(MpM), blocks_mp_parts(MpM)}}.

part_count(?MULTIPART_MANIFEST{parts=PartMs}) ->
    length(PartMs).

bytes_mp_parts(?MULTIPART_MANIFEST{parts=PartMs}) ->
    lists:sum([P?PART_MANIFEST.content_length || P <- PartMs]).

blocks_mp_parts(?MULTIPART_MANIFEST{parts=PartMs}) ->
    lists:sum([riak_cs_lfs_utils:block_count(
                 P?PART_MANIFEST.content_length,
                 P?PART_MANIFEST.block_size) || P <- PartMs]).

% @doc Returns `new' if Timestamp is 3-tuple and greater than `LeewayEdge',
% otherwise `old'.
-spec new_or_old(erlang:timestamp(), erlang:timestamp()) -> new | old.
new_or_old(LeewayEdge, {_,_,_} = Timestamp) when LeewayEdge < Timestamp -> new;
new_or_old(_, _) -> old.

-spec add_to(sum(), pos_integer(),
             {non_neg_integer(), non_neg_integer(), non_neg_integer()}) -> sum().
add_to(Sum, Pos, {Count, Bytes, Blocks}) ->
    {C0, By0, Bl0} = element(Pos, Sum),
    setelement(Pos, Sum, {C0 + Count, By0 + Bytes, Bl0 + Blocks}).

%% @doc Convert `sum()' record to list.
-spec summary_to_list(sum()) -> [{term(), non_neg_integer()}].
summary_to_list(Sum) ->
    [_RecName | Triples] = tuple_to_list(Sum),
    summary_to_list(record_info(fields, sum), Triples, []).

summary_to_list([], _, Acc) ->
    Acc;
summary_to_list([F|Fields], [{C, By, Bl}|Triples], Acc) ->
    summary_to_list(Fields, Triples,
                    [{{F, objects}, C}, {{F, bytes}, By}, {{F, blocks}, Bl} | Acc]).

object_size(Resolved) ->
    {MPparts, MPbytes} = count_multipart_parts(Resolved),
    case riak_cs_manifest_utils:active_manifest(Resolved) of
        {ok, ?MANIFEST{content_length=Length}} ->
            [{1 + MPparts, Length + MPbytes}];
        _ ->
            [{MPparts, MPbytes}]
    end.

-spec count_multipart_parts([{cs_uuid(), lfs_manifest()}]) ->
                                   {non_neg_integer(), non_neg_integer()}.
count_multipart_parts(Resolved) ->
    lists:foldl(fun count_multipart_parts/2, {0, 0}, Resolved).

-spec count_multipart_parts({cs_uuid(), lfs_manifest()},
                            {non_neg_integer(), non_neg_integer()}) ->
                                   {non_neg_integer(), non_neg_integer()}.
count_multipart_parts({_UUID, ?MANIFEST{props=Props, state=writing} = M},
                      {MPparts, MPbytes} = Acc)
  when is_list(Props) ->
    case proplists:get_value(multipart, Props) of
        ?MULTIPART_MANIFEST{parts=Ps} = _  ->
            {MPparts + length(Ps),
             MPbytes + lists:sum([P?PART_MANIFEST.content_length ||
                                     P <- Ps])};
        undefined ->
            %% Maybe not a multipart
            Acc;
        Other ->
            %% strange thing happened
            logger:warning("strange writing multipart manifest detected at ~p: ~p",
                           [M?MANIFEST.bkey, Other]),
            Acc
    end;
count_multipart_parts(_, Acc) ->
    %% Other state than writing, won't be counted
    %% active manifests will be counted later
    Acc.

bucket_summary_reduce(Sums, _) ->
    InitialCounters = orddict:new(),
    [bucket_summary_fold(Sums, InitialCounters)].

bucket_summary_fold([], Counters) ->
    Counters;
bucket_summary_fold([Sum | Sums], Counters) ->
    NewCounters = lists:foldl(
                    fun({K, Num}, Cs) -> orddict:update_counter(K, Num, Cs) end,
                    Counters, Sum),
    bucket_summary_fold(Sums, NewCounters).
