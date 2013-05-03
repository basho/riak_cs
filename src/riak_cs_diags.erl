-module(riak_cs_diags).
-behaviour(gen_server).

-include("riak_cs.hrl").

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

-spec print_manifests(binary() | string(), binary() | string()) -> string().
print_manifests(Bucket, Key) when is_list(Bucket), is_list(Key) ->
    print_manifests(list_to_binary(Bucket), list_to_binary(Key));
print_manifests(Bucket, Key) ->
    Manifests = gen_server:call(?MODULE, {get_manifests, Bucket, Key}),
    Rows = manifest_rows(orddict_values(Manifests)),
    table:print(manifest_table_spec(), Rows).

orddict_values(Dict) ->
    [Val || {_, Val} <- orddict:to_list(Dict)].

%% ====================================================================
%% Table Specifications and Record to Row conversions
%% ====================================================================
manifest_table_spec() ->
    [{block_size, 12}, {bucket, 15}, {key, 20}, {state, 20}].

manifest_rows(Manifests) ->
    [[M?MANIFEST.block_size, element(1, M?MANIFEST.bkey), 
        element(2, M?MANIFEST.bkey), M?MANIFEST.state] || M <- Manifests].

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([]) ->
    {ok, #state{}}.

handle_call({get_manifests, Bucket, Key}, _From, State) ->
    {ok, Pid} = riak_cs_utils:riak_connection(),
    try
        {ok, _, Manifests} = riak_cs_utils:get_manifests(Pid, Bucket, Key),
        {reply, Manifests, State}
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

