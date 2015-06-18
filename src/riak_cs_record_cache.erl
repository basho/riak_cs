%%%-------------------------------------------------------------------
%%% @author UENISHI Kota <kota@basho.com>
%%% @copyright (C) 2014, UENISHI Kota
%%% @doc
%%%  We can adopt much smarter cache design, but so far this is it.
%%% @end
%%% Created :  8 Aug 2014 by UENISHI Kota <kota@basho.com>
%%%-------------------------------------------------------------------
-module(riak_cs_record_cache).

-behaviour(gen_server).

%% API
-export([start_link/2, get/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("riak_cs.hrl").
-define(SERVER, ?MODULE).
-define(TTL, 4096000). %% microseconds
-define(COMPACTION_INTERVAL, 1024000). %% milliseconds

-type refresher_fun() :: fun((Key::binary(), riak_client()) -> {ok, Data::term()} | {error, term()}).

-record(state, {
          name :: atom(),
          refresher_fun = fun(_, _) -> {error, none} end :: refresher_fun()
         }).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Starts the server
-spec start_link(atom(), refresher_fun()) -> {ok, pid()} | ignore | {error, Error::term()}.
start_link(Name, RefresherFun) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name, RefresherFun], []).

%% @doc opaque proxy
-spec get(atom(), User::binary()) -> {ok,rcs_user()}.
get(Name, AccessKey) ->
    case ets:lookup(Name, AccessKey) of
        [] ->
            Now = erlang:now(),
            gen_server:call(Name, {get, AccessKey, Now});
        [{AccessKey, RcsUser, TimeCached}] ->
            Now = erlang:now(),
            case timer:now_diff(Now, TimeCached) > ?TTL of
                true -> %% cache expired
                    lager:info("cache expired for ~p", [AccessKey]),
                    gen_server:call(Name, {get, AccessKey, Now});
                false -> %% cache hit
                    {ok,RcsUser}
            end
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
%% @doc Initializes the server
-spec init(list()) -> {ok, #state{}} |
                      {ok, #state{}, non_neg_integer()} |
                      ignore |
                      {stop, Reason::atom()}.
init([Name, RefresherFun]) ->
    Name = ets:new(Name, [public,named_table,set,{read_concurrency,true}]),
    erlang:send_after(?COMPACTION_INTERVAL, self(), compact),
    {ok, #state{name=Name, refresher_fun=RefresherFun}}.

%% @private
%% @doc Handling call messages
handle_call({get,AccessKey,Now}, _From, State = #state{name=Name, refresher_fun=RefresherFun}) ->
    case ets:lookup(Name, AccessKey) of
        [] ->
            Reply = refresh(AccessKey, Name, RefresherFun),
            {reply,Reply,State};
        [{AccessKey, RcsUser, TimeCached}] ->
            case timer:now_diff(Now, TimeCached) > ?TTL of
                true -> %% cache expired
                    Reply = refresh(AccessKey, Name, RefresherFun),
                    {reply,Reply,State};
                false -> %% cache hit
                    {reply,{ok,RcsUser},State}
            end
    end.

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
handle_info(compact, State = #state{name=Name}) ->
    ets:delete_all_objects(Name),
    lager:debug("~s compaction at ~p cache", [?MODULE, Name]),
    erlang:send_after(?COMPACTION_INTERVAL, self(), compact),
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
terminate(_Reason, #state{name=Name} = _State) ->
    ets:delete(Name),
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

refresh(Key, Name, RefresherFun) when is_binary(Key) ->
    {ok, RcPid} = riak_cs_utils:riak_connection(request_pool),
    Now = erlang:now(),
    try
        %% Reply = riak_cs_user:get_user(AccessKey, RcPid),
        Reply = RefresherFun(Key, RcPid),
        case Reply of
            {ok, Data} ->
                %% TODO: storing User only might be sufficient and space saving
                true = ets:insert(Name, {Key, Data, Now}),
                lager:info("cache updated for ~p at ~p", [Key, Name]);
            _ ->
                false
        end,
        Reply
    after
            ok = riak_cs_utils:close_riak_connection(request_pool, RcPid)
    end.
