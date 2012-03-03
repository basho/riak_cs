%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_block_server).

-behaviour(gen_server).

%% API
-export([start_link/0,
         get_block/5,
         put_block/6,
         delete_block/5]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {riakc_pid :: pid()}).

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

-spec get_block(pid(), binary(), binary(), binary(), pos_integer()) -> ok.
get_block(Pid, Bucket, Key, UUID, BlockNumber) ->
    gen_server:cast(Pid, {get_block, self(), Bucket, Key, UUID, BlockNumber}).

-spec put_block(pid(), binary(), binary(), binary(), pos_integer(), binary()) -> ok.
put_block(Pid, Bucket, Key, UUID, BlockNumber, Value) ->
    gen_server:cast(Pid, {put_block, self(), Bucket, Key, UUID, BlockNumber, Value}).

-spec delete_block(pid(), binary(), binary(), binary(), pos_integer()) -> ok.
delete_block(Pid, Bucket, Key, UUID, BlockNumber) ->
    gen_server:cast(Pid, {delete_block, self(), Bucket, Key, UUID, BlockNumber}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    case riak_moss_utils:riak_connection() of
        {ok, RiakPid} ->
            {ok, #state{riakc_pid=RiakPid}};
        {error, Reason} ->
            lager:error("Failed to establish connection to Riak. Reason: ~p",
                        [Reason]),
            {stop, riak_connect_failed}
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
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

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
handle_cast({get_block, ReplyPid, Bucket, Key, UUID, BlockNumber}, State=#state{riakc_pid=RiakcPid}) ->
    {FullBucket, FullKey} = full_bkey(Bucket, Key, UUID, BlockNumber),
    ChunkValue = case riakc_pb_socket:get(RiakcPid, FullBucket, FullKey, [{r, 1}]) of
        {ok, RiakObject} ->
            {ok, riakc_obj:get_value(RiakObject)};
        {error, notfound}=NotFound ->
            NotFound
    end,
    riak_moss_get_fsm:chunk(ReplyPid, BlockNumber, ChunkValue),
    {noreply, State};
handle_cast({put_block, ReplyPid, Bucket, Key, UUID, BlockNumber, Value}, State=#state{riakc_pid=RiakcPid}) ->
    {FullBucket, FullKey} = full_bkey(Bucket, Key, UUID, BlockNumber),
    RiakObject = riakc_obj:new(FullBucket, FullKey, Value),
    %% TODO: note the return value
    %% of this put call
    riakc_pb_socket:put(RiakcPid, RiakObject),

    riak_moss_put_fsm:block_written(ReplyPid, BlockNumber),
    {noreply, State};
handle_cast({delete_block, _ReplyPid, Bucket, Key, UUID, BlockNumber}, State=#state{riakc_pid=RiakcPid}) ->
    {FullBucket, FullKey} = full_bkey(Bucket, Key, UUID, BlockNumber),
    %% TODO: note the return
    %% value of this delete call
    riakc_pb_socket:delete(RiakcPid, FullBucket, FullKey),
    %% TODO:
    %% add a public func to riak_moss_delete_fsm 
    %% to send messages back to the fsm
    %% saying that the block was deleted
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

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
terminate(_Reason, _State) ->
    ok.

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
full_bkey(Bucket, Key, UUID, BlockNumber) ->
    PrefixedBucket = riak_moss_utils:to_bucket_name(blocks, Bucket),
    FullKey = riak_moss_lfs_utils:block_name(Key, UUID, BlockNumber),
    {PrefixedBucket, FullKey}.
