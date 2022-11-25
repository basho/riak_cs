%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved,
%%               2021, 2022 TI Tokyo    All Rights Reserved.
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

-module(riak_cs_diags).
-behaviour(gen_server).

-dialyzer([ {nowarn_function, pr/1}
          , {nowarn_function, pr/2}
          , {nowarn_function, rec_to_proplist/1}
          , {nowarn_function, print_field/2}
          , {nowarn_function, spaces/1}
          , {nowarn_function, print_multipart_manifest/2}
          ]).

-include("riak_cs.hrl").

%% API
-export([start_link/0,
         print_manifests/3,
         print_manifest/4]).

-define(INDENT_LEVEL, 4).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {}).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec print_manifests(binary() | string(), binary() | string(), binary() | string()) -> ok.
print_manifests(Bucket, Key, Vsn) when is_list(Bucket) ->
    print_manifests(list_to_binary(Bucket), list_to_binary(Key), list_to_binary(Vsn));
print_manifests(Bucket, Key, Vsn) ->
    Manifests = gen_server:call(?MODULE, {get_manifests, Bucket, Key, Vsn}),
    Rows = manifest_rows(orddict_values(Manifests)),
    table:print(manifest_table_spec(), Rows).

-spec print_manifest(binary() | string(), binary() | string(), binary() | string(), binary() | string()) -> ok.
print_manifest(Bucket, Key, Vsn, Uuid) when is_list(Bucket) ->
    print_manifest(list_to_binary(Bucket), list_to_binary(Key), list_to_binary(Vsn), Uuid);
print_manifest(Bucket, Key, Vsn, Uuid) when is_list(Uuid) ->
    print_manifest(Bucket, Key, Vsn, mochihex:to_bin(Uuid));
print_manifest(Bucket, Key, Vsn, Uuid) ->
    Manifests = gen_server:call(?MODULE, {get_manifests, Bucket, Key, Vsn}),
    {ok, Manifest} = orddict:find(Uuid, Manifests),
    io:format("\n~s", [pr(Manifest)]).

-spec pr(tuple()) -> iolist().
pr(Record) ->
    pr(Record, 0).

-spec pr(tuple(), non_neg_integer()) -> iolist().
pr(Record, Indent) ->
    Zipped = rec_to_proplist(Record),
    ["\n", spaces(Indent), "#", atom_to_list(hd(tuple_to_list(Record))),
     "\n", spaces(Indent), "--------------------\n",
     [print_field(FV, Indent) || FV <- Zipped]].

rec_to_proplist(R) ->
    RFF = exprec:info(R),
    lists:zip(RFF, tl(tuple_to_list(R))).

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

handle_call({get_manifests, Bucket, Key, Vsn}, _From, State) ->
    {ok, Pid} = riak_cs_utils:riak_connection(),
    try
        {ok, _, Manifests} = riak_cs_manifest:get_manifests(Pid, Bucket, Key, Vsn),
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
