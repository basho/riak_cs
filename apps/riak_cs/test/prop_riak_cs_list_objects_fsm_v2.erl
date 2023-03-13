%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved,
%%               2021 TI Tokyo    All Rights Reserved.
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

-module(prop_riak_cs_list_objects_fsm_v2).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("riak_cs.hrl").
-include("list_objects.hrl").

%% properties
-export([prop_skip_past_prefix_and_delimiter/0,
         prop_prefix_must_be_in_between/0,
         prop_list_all_active_keys_without_delimiter/0,
         prop_list_all_active_keys_with_delimiter/0]).

%% Helpers
-export([test/0,
         test/1]).

-define(TEST_ITERATIONS, 1000).
-define(QC_OUT(P),
        on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-define(LOGFILE, "riak_cs_list_objects_fsm_v2_proper.log").

%%====================================================================
%% Eunit tests
%%====================================================================

proper_test_() ->
    {spawn,
     [
      {setup,
       fun setup/0,
       fun cleanup/1,
       [
        ?_assert(proper:quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_skip_past_prefix_and_delimiter())))),
        ?_assert(proper:quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_prefix_must_be_in_between())))),
        {timeout, 10*60, % 10min.
         ?_assert(proper:quickcheck(numtests(
                                      ?TEST_ITERATIONS,
                                      ?QC_OUT(prop_list_all_active_keys_without_delimiter()))))},
        {timeout, 10*60, % 10min.
         ?_assert(proper:quickcheck(numtests(
                                      ?TEST_ITERATIONS,
                                      ?QC_OUT(prop_list_all_active_keys_with_delimiter()))))}
       ]
      }
     ]}.

setup() ->
    error_logger:tty(false),
    error_logger:logfile({open, ?LOGFILE}),
    application:set_env(lager, handlers, []),
    exometer:start(),
    riak_cs_stats:init(),
    ok.

cleanup(_) ->
    exometer:stop(),
    file:delete(?LOGFILE),
    ok.

%% ====================================================================
%% EQC Properties
%% ====================================================================

%% @doc Forall non-empty binaries `B',
%% `B < skip_past_prefix_and_delimiter(B)'.
%% Unless the last byte in `B' is 255, then they should be equal
prop_skip_past_prefix_and_delimiter() ->
    ?FORALL(B, non_empty_binary(),
            less_than_prop(B)).

less_than_prop(Binary) ->
    case binary:last(Binary) of
        255 ->
            Binary == riak_cs_list_objects_fsm_v2:skip_past_prefix_and_delimiter(Binary);
        _Else ->
            Binary < riak_cs_list_objects_fsm_v2:skip_past_prefix_and_delimiter(Binary)
    end.

%% No delimiter, flat keys.
prop_list_all_active_keys_without_delimiter() ->
    prop_list_all_active_keys([]).

%% Delimiter "/"
prop_list_all_active_keys_with_delimiter() ->
    prop_list_all_active_keys([{delimiter, <<$/>>}]).

%% @doc For sets manifests, list all manifests by calling riak_cs_list_objects_fsm_v2
%% repeatedly and compare the list to all active manifests.
%% TODO: random max-keys
prop_list_all_active_keys(ListOpts) ->
    ?FORALL({NumKeys, StateFlavor, PrefixFlavor, UserPage, BatchSize},
            {num_keys(), manifest_state_gen_flavor(), manifest_prefix_gen_flavor(),
             user_page(), batch_size()},
    %% Generating lists of manifests seems natural but it is slow.
    %% As workaround, only states are generated here.
    ?FORALL(KeysAndStates, manifest_keys_and_states(NumKeys, StateFlavor, PrefixFlavor),
            begin
                Manifests = [manifest(Key, State) || {Key, State} <- KeysAndStates],
                Sorted = sort_manifests(Manifests),
                %% io:format("Manifests: ~p~n", [Sorted]),
                Listed = keys_in_list(list_manifests(Sorted, ListOpts, UserPage, BatchSize)),
                Expected = active_manifest_keys(KeysAndStates, ListOpts),
                collect(with_title(pages), trunc(NumKeys/UserPage),
                collect(with_title(batches), trunc(NumKeys/BatchSize),
                        ?WHENFAIL(
                           format_diff({NumKeys, StateFlavor, PrefixFlavor},
                                       Expected, Listed, Sorted),
                           Expected =:= Listed)
                       ))
            end
           )).

%% ====================================================================

%% Forall binaries `A' and `B',
%% concatenating `B' to `A' (A + B) should sort inbetween
%% `A' and `skip_past_prefix_and_delimiter(A)'
prop_prefix_must_be_in_between() ->
    ?FORALL({A, B}, {non_empty_binary(), non_empty_binary()},
            ?IMPLIES(no_max_byte(A), bool_in_between(A, B))).

no_max_byte(<<255:8/integer>>) ->
    false;
no_max_byte(Binary) ->
    255 =/= binary:last(Binary).

bool_in_between(A, B) ->
    Between = <<A/binary, B/binary>>,
    (A < Between) andalso (Between < riak_cs_list_objects_fsm_v2:skip_past_prefix_and_delimiter(A)).

%%====================================================================
%% Generators
%%====================================================================

non_empty_binary() ->
    ?SUCHTHAT(B, binary(), B =/= <<>>).

num_keys() ->
    ?LET(X, nat(), X + 1).

%% Naturals starting at 1, not 0.
batch_size() ->
    ?LET(X, nat(), X + 1).

user_page() ->
    ?LET(X, nat(), X + 1).

manifest_state_gen_flavor() ->
    frequency([
               {1, all_active},
               {1, all_deleted},
               {1, almost_active},
               {1, almost_deleted},
               {1, half_active}
              ]).

manifest_keys_and_states(NumKeys, StateFlavor, PrefixFlavor) ->
    [{manifest_key_with_prefix(PrefixFlavor, Index), manifest_state(StateFlavor)} ||
        Index <- lists:seq(1, NumKeys)].

manifest_key_with_prefix(PrefixFlavor, Index) ->
    {manifest_prefix(PrefixFlavor), manifest_key_simple(Index)}.

manifest_prefix_gen_flavor() ->
    oneof([flat, single, handful, many]).

manifest_prefix(flat) ->
    no_prefix;
manifest_prefix(single) ->
    <<"ABC">>;
manifest_prefix(handful) ->
    oneof([no_prefix, <<"ABC">>, <<"0123456">>, <<"ZZZZZZZZ">>]);
manifest_prefix(many) ->
    frequency([{1, no_prefix},
               {10, ?LET(P, vector(30, choose($A, $Z)), list_to_binary(P))}]).

manifest_state(all_active) ->
    active;
manifest_state(all_deleted) ->
    scheduled_delete;
manifest_state(almost_active) ->
    frequency([
               {1, writing},
               {3000, active},
               {1, pending_delete},
               {1, scheduled_delete}
              ]);
manifest_state(almost_deleted) ->
    frequency([
               {1, writing},
               {1, active},
               {1, pending_delete},
               {3000, scheduled_delete}
              ]);
manifest_state(half_active) ->
    frequency([
               {1, writing},
               {3, active},
               {1, pending_delete},
               {1, scheduled_delete}
              ]).

%%====================================================================
%% Test Helpers
%%====================================================================

test() ->
    test(?TEST_ITERATIONS).

test(Iterations) ->
    proper:quickcheck(numtests(Iterations, prop_skip_past_prefix_and_delimiter())),
    proper:quickcheck(numtests(Iterations, prop_prefix_must_be_in_between())),
    proper:quickcheck(numtests(Iterations, prop_list_all_active_keys_without_delimiter())),
    proper:quickcheck(numtests(Iterations, prop_list_all_active_keys_with_delimiter())).


%% TODO: Common prefix, more randomness
manifest_key_simple(Index) ->
    list_to_binary(integer_to_list(Index)).

manifest(Key, State) ->
    Raw = raw_manifest(Key, State),
    process_manifest(Raw).

raw_manifest(Key, State) ->
    ?MANIFEST{uuid = <<"uuid-1">>,
              bkey={<<"bucket">>, bin_key(Key)},
              state=State,
              created = "20221111T010101",
              content_md5 = <<"Content-MD5">>,
              content_length=100,
              acl=?ACL{owner=#{display_name => "display-name",
                               canonical_id => "canonical-id",
                               key_id =>"key-id"}}}.

bin_key({no_prefix, Rest}) ->
    Rest;
bin_key({Prefix, Rest}) ->
    <<Prefix/binary, $/, Rest/binary>>;
bin_key(Key) ->
    Key.

process_manifest(Manifest=?MANIFEST{state=State}) ->
    case State of
        writing ->
            Manifest?MANIFEST{last_block_written_time=os:timestamp()};
        active ->
            %% this clause isn't needed but it makes things more clear imho
            Manifest?MANIFEST{last_block_deleted_time=os:timestamp()};
        pending_delete ->
            Manifest?MANIFEST{last_block_deleted_time=os:timestamp()};
        scheduled_delete ->
            Manifest?MANIFEST{last_block_deleted_time=os:timestamp(),
                              scheduled_delete_time=os:timestamp()}
    end.

sort_manifests(Manifests) ->
    lists:sort(fun(?MANIFEST{bkey=BKey1}, ?MANIFEST{bkey=BKey2}) ->
                       BKey1 < BKey2
               end, Manifests).

active_manifest_keys(KeysAndStates, ListOpts) ->
    active_manifest_keys(KeysAndStates,
                         proplists:get_value(delimiter, ListOpts, undefined),
                         proplists:get_value(prefix, ListOpts, undefined)).

active_manifest_keys(KeysAndStates, undefined=_Delimiter, undefined=_Prefix) ->
    Keys = [bin_key({Prefix, Key}) || {{Prefix, Key}, State} <- KeysAndStates,
                                       State =:= active],
    {[], lists:usort(Keys)};
active_manifest_keys(KeysAndStates, <<$/>>=_Delimiter, undefined=_Prefix) ->
    Keys = [Key || {{Prefix, Key}, State} <- KeysAndStates,
                   State =:= active,
                   Prefix =:= no_prefix],
    CommonPrefixes = [<<Prefix/binary, $/>> ||
                         {{Prefix, _Key}, State} <- KeysAndStates,
                         State =:= active,
                         Prefix =/= no_prefix],
    {lists:usort(CommonPrefixes), lists:usort(Keys)}.

keys_in_list({CPrefixes, Contents}) ->
    {CPrefixes, [Key || #list_objects_key_content{key=Key} <- Contents]}.

list_manifests(Manifests, Opts, UserPage, BatchSize) ->
    {ok, DummyRc} = riak_cs_dummy_riak_client_list_objects_v2:start_link([Manifests]),
    list_manifests_to_the_end(DummyRc, Opts, UserPage, BatchSize, [], [], []).

list_manifests_to_the_end(DummyRc, Opts, UserPage, BatchSize, CPrefixesAcc, ContestsAcc, MarkerAcc) ->
    Bucket = <<"bucket">>,
    %% TODO: Generator?
    %% delimeter, marker and prefix should be generated?
    ListKeysRequest = riak_cs_list_objects:new_request(objects,
                                                       Bucket,
                                                       UserPage,
                                                       Opts),
    {ok, FsmPid} = riak_cs_list_objects_fsm_v2:start_link(DummyRc, ListKeysRequest, BatchSize),
    {ok, ListResp} = riak_cs_list_objects_utils:get_object_list(FsmPid),
    CommonPrefixes = ListResp?LORESP.common_prefixes,
    Contents = ListResp?LORESP.contents,
    NewCPrefixAcc = [CommonPrefixes | CPrefixesAcc],
    NewContentsAcc = [Contents | ContestsAcc],
    case ListResp?LORESP.is_truncated of
        true ->
            Marker = create_marker(ListResp),
            list_manifests_to_the_end(DummyRc, update_marker(Marker, Opts),
                                      UserPage, BatchSize,
                                      NewCPrefixAcc, NewContentsAcc,
                                      [Marker | MarkerAcc]);
        false ->
            riak_cs_dummy_riak_client_list_objects_v2:stop(DummyRc),
            %% TODO: Should assert every result but last has exactly max-results?
            {lists:append(lists:reverse(NewCPrefixAcc)),
             lists:append(lists:reverse(NewContentsAcc))}
    end.

create_marker(?LORESP{next_marker=undefined,
                      contents=Contents}) ->
    LastEntry = lists:last(Contents),
    LastEntry#list_objects_key_content.key;
create_marker(?LORESP{next_marker=NextMarker}) ->
    NextMarker.

update_marker(Marker, Opts) ->
    lists:keystore(marker, 1, Opts, {marker, Marker}).

format_diff({NumKeys, StateFlavor, PrefixFlavor},
            {ExpectedCPs, ExpectedKeys}, {ListedCPs, ListedKeys}, Manifests) ->
    output_entries(Manifests),
    io:nl(),
    io:format("Expected CPs: ~p~n", [ExpectedCPs]),
    io:format("Listed   CPs: ~p~n", [ListedCPs]),
    io:format("Expected Keys: ~p~n", [ExpectedKeys]),
    io:format("Listed   Keys: ~p~n", [ListedKeys]),
    io:nl(),
    io:format("Expected length(CPs): ~p~n", [length(ExpectedCPs)]),
    io:format("Listed   length(CPs): ~p~n", [length(ListedCPs)]),
    io:format("Expected length(Keys): ~p~n", [length(ExpectedKeys)]),
    io:format("Listed   length(Keys): ~p~n", [length(ListedKeys)]),
    first_diff_cp(ExpectedCPs, ListedCPs),
    first_diff_key(ExpectedKeys, ListedKeys, Manifests),
    io:format("NumKeys: ~p~n", [NumKeys]),
    io:format("StateFlavor: ~p~n", [StateFlavor]),
    io:format("PrefixFlavor: ~p~n", [PrefixFlavor]),
    ok.

output_entries(Manifests) ->
    FileName = <<".riak_cs_list_objects_fsm_v2_proper.txt">>,
    io:format("Write states and keys to file: ~s ...", [filename:absname(FileName)]),
    {ok, File} = file:open(FileName, [write, raw]),
    output_entries(File, Manifests, 1),
    io:format(" Done.~n").

output_entries(File, [], _) ->
    file:close(File);
output_entries(File, [M | Manifests], LineNo) ->
    {_, Key} = M?MANIFEST.bkey,
    State = M?MANIFEST.state,
    ok = file:write(File, [integer_to_list(LineNo), $,,
                           atom_to_list(State), $,, Key, $\n]),
    output_entries(File, Manifests, LineNo + 1).

first_diff_cp([], []) ->
    io:format("No diff in CPs.~n");
first_diff_cp([], [CPInList | _Rest]) ->
    io:format("Listed results has more CPs.~n"),
    print_cp(CPInList, "CPInList");
first_diff_cp([CPInExpected | _Rest], []) ->
    io:format("Expected results has more CPs.~n"),
    print_cp(CPInExpected, "CPInExpected");
first_diff_cp([CP | Expected], [CP | Listed]) ->
    first_diff_cp(Expected, Listed);
first_diff_cp([CPInExpected | _Expected], [CPInList | _Listed]) ->
    io:format("first_diff_cp: CPInExpected=~p, CPInList=~p~n",
              [CPInExpected, CPInList]).

print_cp(Key, Label) ->
    io:format("    ~s=~p~n", [Label, Key]).

first_diff_key([], [], _Manifests) ->
    io:format("No diff in Keys.~n");
first_diff_key([], [KeyInList | _Rest], Manifests) ->
    io:format("Listed results has more keys.~n"),
    print_key_and_manifest(KeyInList, "KeyInList", Manifests);
first_diff_key([KeyInExpected | _Rest], [], Manifests) ->
    io:format("Expected results has more keys.~n"),
    print_key_and_manifest(KeyInExpected, "KeyInExpected", Manifests);
first_diff_key([Key | Expected], [Key | Listed], Manifests) ->
    first_diff_key(Expected, Listed, Manifests);
first_diff_key([KeyInExpected | _Expected], [KeyInList | _Listed], Manifests) ->
    io:format("first_diff_key: KeyInExpected=~p, KeyInList=~p~n",
              [KeyInExpected, KeyInList]),
    print_key_and_manifest(KeyInExpected, "KeyInExpected", Manifests),
    print_key_and_manifest(KeyInList, "KeyInList", Manifests).

print_key_and_manifest(Key, Label, []) ->
    io:format("    ~s=~p is not in Manifests~n", [Label, Key]);
print_key_and_manifest(Key, Label, [M | Manifests]) ->
    case M?MANIFEST.bkey of
        {_, Key} ->
            io:format("    ~s=~p Manifest:~n~p~n", [Label, Key, M]);
        _ ->
            print_key_and_manifest(Key, Label, Manifests)
    end.
