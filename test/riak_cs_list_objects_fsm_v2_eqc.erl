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

-module(riak_cs_list_objects_fsm_v2_eqc).

-ifdef(EQC).

-compile(export_all).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("riak_cs.hrl").
-include("list_objects.hrl").

%% eqc properties
-export([prop_skip_past_prefix_and_delimiter/0,
         prop_prefix_must_be_in_between/0]).

%% Helpers
-export([test/0,
         test/1]).

-define(TEST_ITERATIONS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

%%====================================================================
%% Eunit tests
%%====================================================================

eqc_test_() ->
    [?_assert(quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_skip_past_prefix_and_delimiter())))),
     ?_assert(quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_prefix_must_be_in_between()))))].

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

%%====================================================================
%% Test Helpers
%%====================================================================

test() ->
    test(?TEST_ITERATIONS).

test(Iterations) ->
    eqc:quickcheck(eqc:numtests(Iterations, prop_skip_past_prefix_and_delimiter())),
    eqc:quickcheck(eqc:numtests(Iterations, prop_prefix_must_be_in_between())).

test2(Iterations) ->
    eqc:quickcheck(eqc:numtests(Iterations, prop_list_all_active_keys())).


prop_list_all_active_keys() ->
    ?FORALL({NumKeys, Flavor}, {boudary_aware_num_keys(), manifest_state_flaver()},
            ?FORALL(Manifests, manifests(NumKeys, Flavor),
                    begin
                        Sorted = sort_manifests(Manifests),
                        %% io:format("Manifests: ~p~n", [Sorted]),
                        Listed = keys_in_list(list_manifests(Sorted)),
                        Expected = active_manifest_keys(Sorted),
                        collect(with_title(list_length), length(Sorted),
                                ?WHENFAIL(
                                   format_diff(Expected, Listed, Sorted),
                                   Expected =:= Listed
                               ))
                    end
                   )).

format_diff(Expected, Listed, Manifests) ->
    io:format("Expected Keys: ~p~n", [Expected]),
    io:format("Listed Keys:   ~p~n", [Listed]),
    io:format("Expected length(Keys): ~p~n", [length(Expected)]),
    io:format("Listed length(Keys):   ~p~n", [length(Listed)]),
    first_diff_key(Expected, Listed, Manifests),
    ok.

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
    io:format("~s=~p is not in Manifests~n", [Label, Key]);
print_key_and_manifest(Key, Label, [M | Manifests]) ->
    case M?MANIFEST.bkey of
        {_, Key} ->
            io:format("~s=~p Manifest:~n~p~n", [Label, Key, M]);
        _ ->
            print_key_and_manifest(Key, Label, Manifests)
    end.

%% TODO: Entries with same key should be _merge_'d
sort_manifests(Manifests) ->
    lists:sort(fun(?MANIFEST{bkey=BKey1}, ?MANIFEST{bkey=BKey2}) ->
                       BKey1 < BKey2
               end, Manifests).

active_manifest_keys(Manifests) ->
    [element(2, M?MANIFEST.bkey) || M <- Manifests, M?MANIFEST.state =:= active].

keys_in_list(ListedContents) ->
    [Key || #list_objects_key_content_v1{key=Key} <- ListedContents].

list_manifests(Manifests) ->
    {ok, DummyRiakc} = riak_cs_dummy_riakc:start_link([Manifests]),
    list_manifests_to_the_end(DummyRiakc, []).

list_manifests_to_the_end(DummyRiakc, Acc) ->
    Bucket = <<"bucket">>,
    %% TODO: Generator?
    MaxKeys = 1000,
    %% delimeter, marker and prefix should be generated?
    Options = [],
    ListKeysRequest = riak_cs_list_objects:new_request(Bucket,
                                                       MaxKeys,
                                                       Options),
    {ok, FsmPid} = riak_cs_list_objects_fsm_v2:start_link(DummyRiakc, ListKeysRequest),
    {ok, ListResp} = riak_cs_list_objects_utils:get_object_list(FsmPid),
    %% io:format("ListResp: ~p~n", [ListResp]),
    %% TODO: CommonPrefix?
    Contents = ListResp?LORESP.contents,
    %% io:format("Contents: ~p~n", [Contents]),
    NewAcc = [Contents | Acc],
    case ListResp?LORESP.is_truncated of
        true ->
            list_manifests_to_the_end(DummyRiakc, NewAcc);
        false ->
            %% TODO: Should assert every result but last has exactly 1,000 keys?
            lists:append(lists:reverse(NewAcc))
    end.

%%====================================================================
%% Generators
%%====================================================================

boudary_aware_num_keys() ->
    %% TODO(shino): generates integers around 0, 1000, 2000, 3000,...
    %% nat().
    %% 3000.
    frequency([{1, choose(0, 10)},
               {3, ?LET({Multiplier, Offset}, {choose(1, 3), choose(-10, 10)},
                        Multiplier * 1000 + Offset)}]).

manifest_state_flaver() ->
    %% TODO: all_active, almost_deleted, all_deleted, random
    frequency([{1, almost_active}]).

manifests(NumKeys, Flavor) ->
    %% TODO: Using ?LET to bind NumKeys from nat() gen is better? difference?
    %% vector(NumKeys, manifest()).
    [manifest(I, Flavor) || I <- lists:seq(1, NumKeys)].

raw_manifest(Index, Flavor) ->
    ?MANIFEST{uuid=riak_cs_gen:bounded_uuid(),
              %% TODO: Key should be some generators and unique.
              bkey={<<"bucket">>, list_to_binary(integer_to_list(Index))},
              state=manifest_state(Flavor),
              content_md5 = <<"Content-MD5">>,
              content_length=100,
              acl=?ACL{owner={"display-name", "canonical-id", "key-id"}}}.

manifest(Index, Flavor) ->
    ?LET(Manifest, raw_manifest(Index, Flavor), process_manifest(Manifest)).

manifest_state(almost_active) ->
    frequency([{1, writing},
               {3000, active},
               {1, pending_delete},
               {1, scheduled_delete}]).

%% TODO: More simplified generator suffices?
process_manifest(Manifest=?MANIFEST{state=State}) ->
    case State of
        writing ->
            Manifest?MANIFEST{last_block_written_time=os:timestamp(),
                              write_blocks_remaining=blocks_set()};
        active ->
            %% this clause isn't
            %% needed but it makes
            %% things more clear imho
            Manifest?MANIFEST{last_block_deleted_time=os:timestamp()};
        pending_delete ->
            Manifest?MANIFEST{last_block_deleted_time=os:timestamp(),
                              delete_blocks_remaining=blocks_set()};
        scheduled_delete ->
            Manifest?MANIFEST{last_block_deleted_time=os:timestamp(),
                              scheduled_delete_time=os:timestamp(),
                              delete_blocks_remaining=blocks_set()};
        deleted ->
            Manifest?MANIFEST{last_block_deleted_time=os:timestamp()}
    end.

blocks_set() ->
    ?LET(L, eqc_gen:list(int()), ordsets:from_list(L)).

-endif.
