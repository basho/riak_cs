-module(riak_cs_diags).
-behaviour(gen_server).


%% API
-export([start_link/0,
         print_manifests/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {}).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec print_manifests(binary(), binary()) -> string().
print_manifests(Bucket, Key) ->
    gen_server:call(?MODULE, {print_manifests, Bucket, Key}).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([]) ->
    {ok, #state{}}.

handle_call({print_manifests, Bucket, Key}, _From, State) ->
    {ok, Pid} = riak_cs_utils:riak_connection(),
    try
        Reply = riak_cs_utils:get_manifests(Pid, Bucket, Key),
        {reply, Reply, State}
    catch _:_=E ->
        {reply, {error, E}, State}
    after
        riak_cs_utils:close_riak_connection(Pid)
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

