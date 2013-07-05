%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_cs_block_server).

-behaviour(gen_server).

-ifdef(PULSE).
-include_lib("pulse/include/pulse.hrl").
-compile({parse_transform, pulse_instrument}).
-compile({pulse_replace_module,[{gen_server,pulse_gen_server}]}).
-endif.

-include("riak_cs.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").

%% API
-export([start_link/0,
         start_link/1,
         start_block_servers/2,
         get_block/5, get_block/6,
         put_block/6,
         delete_block/5,
         maybe_stop_block_servers/1,
         stop/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {riakc_pid :: pid(),
                close_riak_connection=true :: boolean()}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link(?MODULE, [], []).

start_link(RiakPid) ->
    gen_server:start_link(?MODULE, [RiakPid], []).

%% @doc Start (up to) 'MaxNumServers'
%% riak_cs_block_server procs.
%% 'RiakcPid' must be a Pid you already
%% have for a riakc_pb_socket proc. If the
%% poolboy boy returns full, you will be given
%% a list of less than 'MaxNumServers'.

%% TODO: this doesn't guarantee any minimum
%% number of workers. I could also imagine
%% this function looking something
%% like:
%% start_block_servers(RiakcPid, MinWorkers, MaxWorkers, MinWorkerTimeout)
%% Where the function works something like:
%% Give me between MinWorkers and MaxWorkers,
%% waiting up to MinWorkerTimeout to get at least
%% MinWorkers. If the timeout occurs, this function
%% could return an error, or the pids it has
%% so far (which might be less than MinWorkers).
-spec start_block_servers(pid(), pos_integer()) -> [pid()].
start_block_servers(RiakcPid, 1) ->
    {ok, Pid} = start_link(RiakcPid),
    [Pid];
start_block_servers(RiakcPid, MaxNumServers) ->
    case start_link() of
        {ok, Pid} ->
            [Pid | start_block_servers(RiakcPid, (MaxNumServers - 1))];
        {error, normal} ->
            start_block_servers(RiakcPid, 1)
    end.

-spec get_block(pid(), binary(), binary(), binary(), pos_integer()) -> ok.
get_block(Pid, Bucket, Key, UUID, BlockNumber) ->
    gen_server:cast(Pid, {get_block, self(), Bucket, Key, undefined, UUID, BlockNumber}).

%% @doc get a block which is know to have originated on cluster ClusterID.
%% If it's not found locally, it might get returned from the replication
%% cluster if a connection exists to that cluster. This is proxy-get().
-spec get_block(pid(), binary(), binary(), binary(), binary(), pos_integer()) -> ok.
get_block(Pid, Bucket, Key, ClusterID, UUID, BlockNumber) ->
    gen_server:cast(Pid, {get_block, self(), Bucket, Key, ClusterID, UUID, BlockNumber}).

-spec put_block(pid(), binary(), binary(), binary(), pos_integer(), binary()) -> ok.
put_block(Pid, Bucket, Key, UUID, BlockNumber, Value) ->
    gen_server:cast(Pid, {put_block, self(), Bucket, Key, UUID, BlockNumber, Value}).

-spec delete_block(pid(), binary(), binary(), binary(), pos_integer()) -> ok.
delete_block(Pid, Bucket, Key, UUID, BlockNumber) ->
    gen_server:cast(Pid, {delete_block, self(), Bucket, Key, UUID, BlockNumber}).

-spec maybe_stop_block_servers(undefined | [pid()]) -> ok.
maybe_stop_block_servers(undefined) ->
    ok;
maybe_stop_block_servers(BlockServerPids) ->
    _ = [stop(P) || P <- BlockServerPids],
    ok.

stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([RiakPid]) ->
    process_flag(trap_exit, true),
    {ok, #state{riakc_pid=RiakPid,
                close_riak_connection=false}};
init([]) ->
    process_flag(trap_exit, true),
    case riak_cs_utils:riak_connection() of
        {ok, RiakPid} ->
            {ok, #state{riakc_pid=RiakPid}};
        {error, all_workers_busy} ->
            {stop, normal}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------

handle_cast({get_block, ReplyPid, Bucket, Key, ClusterID, UUID, BlockNumber}, State=#state{riakc_pid=RiakcPid}) ->
    dt_entry(<<"get_block">>, [BlockNumber], [Bucket, Key]),
    {FullBucket, FullKey} = full_bkey(Bucket, Key, UUID, BlockNumber),
    StartTime = os:timestamp(),
    GetOptions = [{r, 1}, {notfound_ok, false}, {basic_quorum, false}],
    LocalClusterID = riak_cs_config:cluster_id(RiakcPid),
    %% don't use proxy get if it's a local get
    %% or proxy get is disabled
    UseProxyGet = ClusterID /= undefined
                    andalso riak_cs_config:proxy_get_active()
                    andalso LocalClusterID /= ClusterID,
    Object =
        case UseProxyGet of
            false ->
                riakc_pb_socket:get(RiakcPid, FullBucket, FullKey, GetOptions);
            true ->
                riak_repl_pb_api:get(RiakcPid, FullBucket, FullKey, ClusterID, GetOptions)
        end,
    ChunkValue = case Object of
        {ok, RiakObject} ->
            {ok, riakc_obj:get_value(RiakObject)};
        {error, notfound}=NotFound ->
            NotFound
    end,
    ok = riak_cs_stats:update_with_start(block_get, StartTime),
    ok = riak_cs_get_fsm:chunk(ReplyPid, {UUID, BlockNumber}, ChunkValue),
    dt_return(<<"get_block">>, [BlockNumber], [Bucket, Key]),
    {noreply, State};
handle_cast({put_block, ReplyPid, Bucket, Key, UUID, BlockNumber, Value}, State=#state{riakc_pid=RiakcPid}) ->
    dt_entry(<<"put_block">>, [BlockNumber], [Bucket, Key]),
    {FullBucket, FullKey} = full_bkey(Bucket, Key, UUID, BlockNumber),
    RiakObject0 = riakc_obj:new(FullBucket, FullKey, Value),
    MD = dict:from_list([{?MD_USERMETA, [{"RCS-bucket", Bucket},
                                         {"RCS-key", Key}]}]),
    _ = lager:debug("put_block: Bucket ~p Key ~p UUID ~p", [Bucket, Key, UUID]),
    _ = lager:debug("put_block: FullBucket: ~p FullKey: ~p", [FullBucket, FullKey]),
    RiakObject = riakc_obj:update_metadata(RiakObject0, MD),
    StartTime = os:timestamp(),
    ok = riakc_pb_socket:put(RiakcPid, RiakObject),
    ok = riak_cs_stats:update_with_start(block_put, StartTime),
    riak_cs_put_fsm:block_written(ReplyPid, BlockNumber),
    dt_return(<<"put_block">>, [BlockNumber], [Bucket, Key]),
    {noreply, State};
handle_cast({delete_block, ReplyPid, Bucket, Key, UUID, BlockNumber}, State=#state{riakc_pid=RiakcPid}) ->
    dt_entry(<<"delete_block">>, [BlockNumber], [Bucket, Key]),
    {FullBucket, FullKey} = full_bkey(Bucket, Key, UUID, BlockNumber),
    StartTime = os:timestamp(),

    %% do a get first to get the vclock (only do a head request though)
    GetOptions = [{r, 1}, {notfound_ok, false}, {basic_quorum, false}, head],
    _ = case riakc_pb_socket:get(RiakcPid, FullBucket, FullKey, GetOptions) of
            {ok, RiakObject} ->
                ok = delete_block(RiakcPid, ReplyPid, RiakObject, {UUID, BlockNumber});
        {error, notfound} ->
            %% If the block isn't found, assume it's been
            %% previously deleted by another delete FSM, and
            %% move on to the next block.
            riak_cs_delete_fsm:block_deleted(ReplyPid, {ok, {UUID, BlockNumber}})
    end,
    ok = riak_cs_stats:update_with_start(block_delete, StartTime),
    dt_return(<<"delete_block">>, [BlockNumber], [Bucket, Key]),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

delete_block(RiakcPid, ReplyPid, RiakObject, BlockId) ->
    Result = constrained_delete(RiakcPid, RiakObject, BlockId),
    _ = secondary_delete_check(Result, RiakcPid, RiakObject),
    riak_cs_delete_fsm:block_deleted(ReplyPid, Result),
    ok.

constrained_delete(RiakcPid, RiakObject, BlockId) ->
    DeleteOptions = [{r, all}, {pr, all}, {w, all}, {pw, all}],
    format_delete_result(
      riakc_pb_socket:delete_obj(RiakcPid, RiakObject, DeleteOptions),
      BlockId).

secondary_delete_check({error, {unsatisfied_constraint, _, _}}, RiakcPid, RiakObject) ->
    riakc_pb_socket:delete_obj(RiakcPid, RiakObject);
secondary_delete_check(_, _, _) ->
    ok.

format_delete_result(ok, BlockId) ->
    {ok, BlockId};
format_delete_result({error, Reason}, BlockId) when is_binary(Reason) ->
    %% Riak client sends back oddly formatted errors
    format_delete_result({error, binary_to_list(Reason)}, BlockId);
format_delete_result({error, "{r_val_unsatisfied," ++ _}, BlockId) ->
    {error, {unsatisfied_constraint, r, BlockId}};
format_delete_result({error, "{w_val_unsatisfied," ++ _}, BlockId) ->
    {error, {unsatisfied_constraint, w, BlockId}};
format_delete_result({error, "{pr_val_unsatisfied," ++ _}, BlockId) ->
    {error, {unsatisfied_constraint, pr, BlockId}};
format_delete_result({error, "{pw_val_unsatisfied," ++ _}, BlockId) ->
    {error, {unsatisfied_constraint, pw, BlockId}};
format_delete_result(Result, _) ->
    Result.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, #state{riakc_pid=RiakcPid,
                          close_riak_connection=CloseConn}) ->
    case CloseConn of
        true ->
            riak_cs_utils:close_riak_connection(RiakcPid),
            ok;
        false ->
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec full_bkey(binary(), binary(), binary(), pos_integer()) -> {binary(), binary()}.
%% @private
full_bkey(Bucket, Key, UUID, BlockId) ->
    PrefixedBucket = riak_cs_utils:to_bucket_name(blocks, Bucket),
    FullKey = riak_cs_lfs_utils:block_name(Key, UUID, BlockId),
    {PrefixedBucket, FullKey}.

dt_entry(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_BLOCK_OP, 1, Ints, ?MODULE, Func, Strings).

dt_return(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_BLOCK_OP, 2, Ints, ?MODULE, Func, Strings).
