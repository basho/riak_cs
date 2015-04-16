%% #!/usr/bin/env escript

%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.
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

-module(select_gc_bucket).

-compile(export_all).

-include_lib("riak_cs/include/riak_cs.hrl").

-record(state,
        {logger :: pid(),
         ring_size = 64 :: non_neg_integer(),
         threshold :: non_neg_integer() | undefined}).

main([Host, Port0, RingSize0, MinSize0, OutputFile|_Rest]) ->
    Port = case Port0 of
               "rel" -> 8087;
               "dev1" -> 10018;
               _ -> list_to_integer(Port0)
           end,
    RingSize = list_to_integer(RingSize0),
    MinSize = list_to_integer(MinSize0),
    %% TODO: make this configurable, take leeway into account
    StartKey = <<"0">>,
    EndKey = integer_to_list(riak_cs_gc:timestamp()),
    io:format(standard_error, "Connecting ~p:~p~n", [Host, Port]),
    Opts = [{max_results, 100},
            {start_key, StartKey},
            {end_key, EndKey},
            {timeout, 6000}],
    {ok, Pid} = riakc_pb_socket:start_link(Host, Port),
    Options = [write, delayed_write], %, compressed],
    {ok, File} = file:open(OutputFile, Options),
    State = #state{logger = File, ring_size = RingSize, threshold = MinSize},
    try
        {ok, ReqID} = riakc_pb_socket:cs_bucket_fold(Pid, ?GC_BUCKET, Opts),
        handle_fold_results(Pid, ReqID, [], State),
        io:format(standard_error,
                  "Finished!~n"
                  "Next action is to run offline delete. Use Riak command like this:~n"
                  "$ riak escript /usr/lib/riak-cs/lib/riak-cs_2.0.0/priv/tools/internal/offline_delete.erl --dry-run /var/lib/riak/bitcask ~s~n",
                  [OutputFile])
    after
        riakc_pb_socket:stop(Pid),
        file:close(File)
    end;
main(_) ->
    io:format(standard_error, "options: <host> <port> <ring_size> <threshold-bytes> <output-file>~n", []).


handle_fold_results(Pid, ReqID, Objs0, State) ->
    [begin
         ManifestSet = riak_cs_gc:decode_and_merge_siblings(
                         Obj, twop_set:new()),
         ManifestList = twop_set:to_list(ManifestSet),
         %% io:format(standard_error, "~p manifests found.~n", [length(ManifestList)]),
         lists:foreach(fun(Manifest) -> handle_manifest(Manifest, State) end,
                       ManifestList)
     end
     || Obj <- Objs0],
    receive
        {ReqID, {ok, Objs}} ->
            handle_fold_results(Pid, ReqID, Objs, State);
        {ReqID, {done, Other}} when is_list(Other) ->
            handle_fold_results(Pid, ReqID, Other, State);
        {ReqID, {done, _}} ->
            done
    end.

handle_manifest({_UUID,
                 ?MANIFEST{content_length=ContentLength,
                           state=pending_delete} = _Manifest},
                #state{threshold=Threshold} = _State)
  when ContentLength < Threshold -> ignore;
handle_manifest({_UUID, ?MANIFEST{bkey=_BK, props=Props} = M}, State) ->
    case proplists:get_value(multipart, Props) of
        ?MULTIPART_MANIFEST{} = MpM ->
            %% This is Multipart
            PartMs = MpM?MULTIPART_MANIFEST.parts,
            lists:foreach(fun(Part) ->
                                  handle_mp_part(Part, State)
                          end, PartMs);
        undefined ->
            ?MANIFEST{bkey=BKey,
                      uuid=UUID,
                      block_size=BlockSize,
                      content_length=ContentLength,
                      state=pending_delete} = M,
            %% This is normal manifest
            #state{logger=File, ring_size=RingSize, threshold=_Threshold} = State,
            io:format(standard_error, "~p (~p) ~p~n", [BKey, mochihex:to_hex(UUID), ContentLength]),
            %% io:format(standard_error, "vnodes:~p ~p ~p~n", vnode_ids(BKey, RingSize, 3)),
            NumBlocks = ContentLength div BlockSize,
            {Bucket, _Key}= BKey,
            output_blocks(File, Bucket, UUID, NumBlocks, RingSize)
    end.

handle_mp_part(?PART_MANIFEST{content_length=ContentLength,
                              block_size=BlockSize,
                              part_id=PartId,
                              bucket=Bucket},
               #state{logger=File,
                      ring_size=RingSize}) ->
    NumBlocks = ContentLength div BlockSize,
    output_blocks(File, Bucket, PartId, NumBlocks, RingSize).

output_blocks(File, Bucket, UUID, NumBlocks, RingSize) ->
    lists:foreach(fun(SeqNo) ->
                          BK = {B,K} = full_bkey(Bucket, dummy, UUID, SeqNo),
                          VNodes = [[integer_to_list(VNode), " "] || VNode <- vnode_ids(BK, RingSize, 3)],
                          %% Partitions, UUID, SeqNo
                          file:write(File, [VNodes, $\t,
                                            mochihex:to_hex(B), $\t,
                                            mochihex:to_hex(K), $\t,
                                            mochihex:to_hex(UUID), $\t,
                                            integer_to_list(SeqNo), $\n])
                  end, lists:seq(0, NumBlocks-1)).

full_bkey(Bucket, Key, UUID, BlockId) ->
    PrefixedBucket = riak_cs_utils:to_bucket_name(blocks, Bucket),
    FullKey = riak_cs_lfs_utils:block_name(Key, UUID, BlockId),
    {PrefixedBucket, FullKey}.


-define(RINGTOP, trunc(math:pow(2,160)-1)).  % SHA-1 space

%% (hash({B,K}) div Inc)

vnode_id(BKey, RingSize) ->
    <<HashKey:160/integer>> = key_of(BKey),
    Inc = ?RINGTOP div RingSize,
    %% io:format(standard_error, "RingSize ~p, RINGTOP ~p Inc ~p ~n", [RingSize, ?RINGTOP, Inc]),
    PartitionId = ((HashKey div Inc) + 1) rem RingSize,
    PartitionId * Inc.

vnode_ids(BKey, RingSize, NVal) ->
    <<HashKey:160/integer>> = key_of(BKey),
    Inc = ?RINGTOP div RingSize,
    %% io:format(standard_error, "RingSize ~p, RINGTOP ~p Inc ~p ~n", [RingSize, ?RINGTOP, Inc]),
    PartitionId = ((HashKey div Inc) + 1) rem RingSize,
    [((PartitionId+N) rem RingSize) * Inc  || N <- lists:seq(0, NVal-1)].

sha(Bin) ->
    crypto:hash(sha, Bin).

key_of(ObjectName) ->
    sha(term_to_binary(ObjectName)).
