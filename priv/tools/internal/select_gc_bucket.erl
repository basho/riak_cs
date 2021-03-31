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
-mode(compile).

-include_lib("riak_cs/include/riak_cs.hrl").

-record(state,
        {logger :: file:io_device(),
         threshold :: non_neg_integer() | undefined,
         keycount = 0 :: non_neg_integer(),
         manifestcount = 0 :: non_neg_integer(),
         total_manifestcount = 0 :: non_neg_integer(),
         blockcount = 0 :: non_neg_integer()
        }).

options() ->
    [{host, $h, "host", {string, "localhost"}, "Address of Riak"},
     {port, $p, "port", {integer, 8087}, "Port number of Riak PB"},
     {threshold, $t, "threshold", {integer, 5*1024*1024}, "Threshold"},
     {start, $s, "start", {string, "19700101"}, "Start of the seek period"},
     {'end', $e, "end", {string, "yesterday"}, "End of the seek period"},
     {output, $o, "output", {string, "/tmp/tmp.txt"},
      "Output file (absolute path)"},
     {timeout, $w, "timeout", {integer, 6}, "Timeout in seconds"}].

pgv(Key,Proplist) ->
    case proplists:get_value(Key, Proplist) of
        undefined -> getopt:usage(options(), "riak-cs escript me"), halt(-1);
        Value -> Value
    end.

maybe_date("today") ->
    list_to_binary(integer_to_list(riak_cs_gc:timestamp()));
maybe_date("yesterday") ->
    list_to_binary(integer_to_list(riak_cs_gc:timestamp() - 86400));
maybe_date([Y0,Y1,Y2,Y3,M0,M1,D0,D1]) ->
    DateTime = {{list_to_integer([Y0,Y1,Y2,Y3]),
                 list_to_integer([M0,M1]),
                 list_to_integer([D0,D1])},
                {0,0,0}},
    Sec = calendar:datetime_to_gregorian_seconds(DateTime) - 62167219200,
    list_to_binary(integer_to_list(Sec)).

main(Args) ->
    case getopt:parse(options(), Args) of
        {ok, {Options, _}} ->
            Host = pgv(host, Options),
            Port = pgv(port, Options),
            Threshold = pgv(threshold, Options),
            %% TODO: make this configurable, take leeway into account
            StartKey = maybe_date(pgv(start, Options)),
            EndKey = maybe_date(pgv('end', Options)),
            OutputFile = pgv(output, Options),
            Timeout = pgv(timeout, Options) * 1000,
            State = #state{threshold = Threshold},
            work(Host, Port, StartKey, EndKey, Timeout, OutputFile, State);
        _E ->
            getopt:usage(options(), "select_gc_bucket.erl")
    end.

work(Host, Port, StartKey, EndKey, Timeout, OutputFile, State0) ->
    io:format(standard_error, "Connecting ~p:~p~n", [Host, Port]),
    Opts = [%% {max_results, 1000},
            {start_key, StartKey},
            {end_key, EndKey},
            {timeout, Timeout}],
    {ok, Pid} = riakc_pb_socket:start_link(Host, Port),
    Options = [write, delayed_write], %, compressed],
    {ok, File} = file:open(OutputFile, Options),
    State = State0#state{logger=File},
    try
        {ok, ReqID} = riakc_pb_socket:cs_bucket_fold(Pid, ?GC_BUCKET, Opts),
        handle_fold_results(Pid, ReqID, State),
        io:format(standard_error,
                  "Finished!~n"
                  "Next action is to run offline delete. Use Riak command like this:~n"
                  "$ riak escript /usr/lib/riak-cs/lib/riak-cs_2.0.0/priv/tools/internal/offline_delete.erl --dry-run /var/lib/riak/bitcask ~s~n",
                  [OutputFile])
    after
        riakc_pb_socket:stop(Pid),
        file:close(File)
    end.

handle_fold_results(Pid, ReqID, State = #state{total_manifestcount=TMC,
                                               keycount=KC,
                                               manifestcount=MC,
                                               blockcount=BC}) ->
    receive
        {ReqID, {ok, Objs}} ->
            Nums = [begin
                        ManifestSet = riak_cs_gc:decode_and_merge_siblings(
                                        Obj, twop_set:new()),
                        ManifestList = twop_set:to_list(ManifestSet),
                        {MK, MB} = lists:foldl(fun(Manifest, {MK0, MB0}) ->
                                                       {MK1, MB1} = handle_manifest(Manifest, State),
                                                       {MK0+MK1, MB0+MB1}
                                               end, {0, 0}, ManifestList),
                        {length(ManifestList), MK, MB}
                    end
                    || Obj <- Objs],
            %% io:format(standard_error, "============================== ~p gc keys found.~n", [length(Objs0)]),
            {A,B,C} = lists:foldl(fun({A0,B0,C0},{A1,B1,C1}) -> {A0+A1, B0+B1, C0+C1} end, {0, 0, 0}, Nums),
            handle_fold_results(Pid, ReqID,
                                State#state{total_manifestcount=A+TMC,
                                            keycount=KC+length(Objs),
                                            manifestcount=MC+B,
                                            blockcount=BC+C});
        %% {ReqID, {done, Other}} when is_list(Other) ->
        %%     handle_fold_results(Pid, ReqID, Other, State);
        {ReqID, {done, _}} ->
            io:format(standard_error,
                      "keycount: ~p, total_manifestcount ~p, manifestcount ~p, blockcount ~p~n",
                      [KC, TMC, MC, BC]),
            done;
        Other ->
            io:format(standard_error, "Boom!!! Other; ~p", [Other]),
            error
    end.

%% => {matched_keys, matched_blocks}
handle_manifest({_UUID,
                 ?MANIFEST{content_length=ContentLength} = _Manifest},
                #state{threshold=Threshold} = _State)
  when ContentLength < Threshold ->
    {0, 0};
handle_manifest({_UUID, ?MANIFEST{bkey=BKey={CSBucket, CSKey},
                                  uuid=UUID,
                                  content_length=ContentLength,
                                  state=pending_delete} = M},
                _State = #state{logger=File}) ->
    io:format(standard_error, "~p (~p) ~p~n", [BKey, mochihex:to_hex(UUID), ContentLength]),
    BlockSequences = riak_cs_lfs_utils:block_sequences_for_manifest(M),
    Count = ordsets:fold(fun({UUID1, SeqNo}, Count0) ->
                                 {B,K} = full_bkey(CSBucket, dummy, UUID1, SeqNo),
                                 file:write(File, [mochihex:to_hex(B), $\t,
                                                   mochihex:to_hex(K), $\t,
                                                   CSBucket, $\t,
                                                   mochihex:to_hex(CSKey), $\t,
                                                   mochihex:to_hex(UUID1), $\t,
                                                   integer_to_list(SeqNo), $\n]),
                                 Count0 + 1
                         end, 0, BlockSequences),
    {1, Count}.

%% From riak_cs
full_bkey(Bucket, Key, UUID, BlockId) ->
    PrefixedBucket = riak_cs_utils:to_bucket_name(blocks, Bucket),
    FullKey = riak_cs_lfs_utils:block_name(Key, UUID, BlockId),
    {PrefixedBucket, FullKey}.
