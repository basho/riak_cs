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
    [{state, 20}, {dead, 8}, {deleted, 8},  {mp, 6}, {created, 28}, {uuid, 36}, 
     {write_start_time, 23}, {delete_marked_time, 23}].

manifest_rows(Manifests) ->
    [ [M?MANIFEST.state, dead(M?MANIFEST.props), deleted(M?MANIFEST.props),
       riak_cs_mp_utils:is_multipart_manifest(M), 
       M?MANIFEST.created, mochihex:to_hex(M?MANIFEST.uuid),
       M?MANIFEST.write_start_time, M?MANIFEST.delete_marked_time] || M <- Manifests].

dead(Props) ->
    lists:keymember(dead, 1, Props).

deleted(Props) ->
    lists:keymember(deleted, 1, Props).

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

