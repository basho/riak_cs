-module(riak_cs_diags).
-behaviour(gen_server).

-include("riak_cs.hrl").

%% API
-export([start_link/0,
         print_manifests/2,
         print_manifest/3]).

-define(INDENT_LEVEL, 4).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {}).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec print_manifests(binary() | string(), binary() | string()) -> ok.
print_manifests(Bucket, Key) when is_list(Bucket), is_list(Key) ->
    print_manifests(list_to_binary(Bucket), list_to_binary(Key));
print_manifests(Bucket, Key) ->
    Manifests = gen_server:call(?MODULE, {get_manifests, Bucket, Key}),
    Rows = manifest_rows(orddict_values(Manifests)),
    table:print(manifest_table_spec(), Rows).

-spec print_manifest(binary() | string(), binary() | string(), binary() | string()) -> ok.
print_manifest(Bucket, Key, Uuid) when is_list(Bucket), is_list(Key) ->
    print_manifest(list_to_binary(Bucket), list_to_binary(Key), Uuid);
print_manifest(Bucket, Key, Uuid) when is_list(Uuid) ->
    print_manifest(Bucket, Key, mochihex:to_bin(Uuid));
print_manifest(Bucket, Key, Uuid) ->
    Manifests = gen_server:call(?MODULE, {get_manifests, Bucket, Key}),
    {ok, Manifest} = orddict:find(Uuid, Manifests),
    io:format("\n~s", [pr(Manifest)]).

-spec pr(tuple()) -> iolist().
pr(Record) ->
    pr(Record, 0).

-spec pr(tuple(), non_neg_integer()) -> iolist().
pr(Record, Indent) ->
    {'$lager_record', RecordName, Zipped} = lager:pr(Record, ?MODULE), 
    ["\n", spaces(Indent), "#", atom_to_list(RecordName),
     "\n", spaces(Indent), "--------------------\n", 
     [print_field(Field, Indent) || Field <- Zipped]].

print_field({_, undefined}, _) ->
    "";
print_field({uuid, Uuid}, Indent) when is_binary(Uuid) ->
    print_field({uuid, mochihex:to_hex(Uuid)}, Indent);
print_field({content_md5, Value}, Indent) when is_binary(Value) ->
    print_field({content_md5, mochihex:to_hex(Value)}, Indent);
print_field({upload_id, Value}, Indent) when is_binary(Value) ->
    print_field({upload_id, mochihex:to_hex(Value)}, Indent);
print_field({part_id, Value}, Indent) when is_binary(Value) ->
    print_field({part_id, mochihex:to_hex(Value)}, Indent);
print_field({acl, Value}, Indent) ->
    io_lib:format("~s~s = ~s\n\n", [spaces(Indent), acl, pr(Value, Indent + 1)]);
print_field({props, Props}, Indent) ->
    io_lib:format("~s~s = ~s\n\n", [spaces(Indent), multipart, 
                                    print_multipart_manifest(Props, Indent)]);
print_field({parts, Parts}, Indent) ->
    io_lib:format("~s~s = ~s\n\n", [spaces(Indent), parts, 
                                  [pr(P, Indent + 1) ||  P <- Parts]]);
print_field({Key, Value}, Indent) ->
    io_lib:format("~s~s = ~p\n", [spaces(Indent), Key, Value]).

orddict_values(Dict) ->
    [Val || {_, Val} <- orddict:to_list(Dict)].

spaces(Num) ->
    [" " || _ <- lists:seq(1, Num*?INDENT_LEVEL)].

%% ====================================================================
%% Table Specifications and Record to Row conversions
%% ====================================================================
manifest_table_spec() ->
    [{state, 20}, {deleted, 8},  {mp, 6}, {created, 28}, {uuid, 36}, 
     {write_start_time, 23}, {delete_marked_time, 23}].

manifest_rows(Manifests) ->
    [ [M?MANIFEST.state, deleted(M?MANIFEST.props),
       riak_cs_mp_utils:is_multipart_manifest(M), 
       M?MANIFEST.created, mochihex:to_hex(M?MANIFEST.uuid),
       M?MANIFEST.write_start_time, M?MANIFEST.delete_marked_time] || M <- Manifests].

print_multipart_manifest(Props, Indent) ->
    case lists:keyfind(multipart, 1, Props) of
        {multipart, MpManifest} -> 
            pr(MpManifest, Indent + 1);
        _ ->
            ""
    end.

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

