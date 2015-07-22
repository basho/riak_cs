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

%% @doc ACL utility functions

-module(stanchion_acl_utils).

-include("stanchion.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-endif.

%% Public API
-export([acl/5,
         acl_from_json/1,
         acl_to_json_term/1
        ]).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Construct an acl. The structure is the same for buckets
%% and objects.
-spec acl(string(), string(), string(), [acl_grant()], erlang:timestamp()) -> acl2().
acl(DisplayName, CanonicalId, KeyId, Grants, CreationTime) ->
    OwnerData = {DisplayName, CanonicalId, KeyId},
    ?ACL{owner=OwnerData,
         grants=Grants,
         creation_time=CreationTime}.

%% @doc Convert a set of JSON terms representing an ACL into
%% an internal representation.
-spec acl_from_json(term()) -> acl2().
acl_from_json({struct, Json}) ->
    process_acl_contents(Json, ?ACL{});
acl_from_json(Json) ->
    process_acl_contents(Json, ?ACL{}).

%% @doc Convert an internal representation of an ACL into
%% erlang terms that can be encoded using `mochijson2:encode'.
-spec acl_to_json_term(acl()) -> term().
acl_to_json_term(?ACL{owner={DisplayName, CanonicalId, KeyId},
                      grants=Grants,
                      creation_time=CreationTime}) ->
    {<<"acl">>,
     {struct, [{<<"version">>, 1},
               owner_to_json_term(DisplayName, CanonicalId, KeyId),
               grants_to_json_term(Grants, []),
               erlang_time_to_json_term(CreationTime)]}
    }.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @doc Convert an information from an ACL into erlang
%% terms that can be encoded using `mochijson2:encode'.
-spec erlang_time_to_json_term(erlang:timestamp()) -> term().
erlang_time_to_json_term({MegaSecs, Secs, MicroSecs}) ->
    {<<"creation_time">>,
     {struct, [{<<"mega_seconds">>, MegaSecs},
               {<<"seconds">>, Secs},
               {<<"micro_seconds">>, MicroSecs}]}
    }.

%% @doc Convert grantee information from an ACL into erlang
%% terms that can be encoded using `mochijson2:encode'.
-spec grantee_to_json_term(acl_grant()) -> term().
grantee_to_json_term({Group, Perms}) when is_atom(Group) ->
    {struct, [{<<"group">>, list_to_binary(
                              atom_to_list(Group))},
              {<<"permissions">>, permissions_to_json_term(Perms)}]};
grantee_to_json_term({{DisplayName, CanonicalId}, Perms}) ->
    {struct, [{<<"display_name">>, list_to_binary(DisplayName)},
              {<<"canonical_id">>, list_to_binary(CanonicalId)},
              {<<"permissions">>, permissions_to_json_term(Perms)}]}.

%% @doc Convert owner information from an ACL into erlang
%% terms that can be encoded using `mochijson2:encode'.
-spec grants_to_json_term([acl_grant()], [term()]) -> term().
grants_to_json_term([], GrantTerms) ->
    {<<"grants">>, GrantTerms};
grants_to_json_term([HeadGrant | RestGrants], GrantTerms) ->
    grants_to_json_term(RestGrants,
                        [grantee_to_json_term(HeadGrant) | GrantTerms]).

%% @doc Convert owner information from an ACL into erlang
%% terms that can be encoded using `mochijson2:encode'.
-spec owner_to_json_term(string(), string(), string()) -> term().
owner_to_json_term(DisplayName, CanonicalId, KeyId) ->
    {<<"owner">>,
     {struct, [{<<"display_name">>, list_to_binary(DisplayName)},
               {<<"canonical_id">>, list_to_binary(CanonicalId)},
               {<<"key_id">>, list_to_binary(KeyId)}]}
    }.

%% @doc Convert a list of permissions into binaries
%% that can be encoded using `mochijson2:encode'.
-spec permissions_to_json_term(acl_perms()) -> term().
permissions_to_json_term(Perms) ->
    [list_to_binary(atom_to_list(Perm)) || Perm <- Perms].

%% @doc Process the top-level elements of the
-spec process_acl_contents([term()], acl()) -> acl2().
process_acl_contents([], Acl) ->
    Acl;
process_acl_contents([{Name, Value} | RestObjects], Acl) ->
    _ = lager:debug("Object name: ~p", [Name]),
    case Name of
        <<"owner">> ->
            {struct, OwnerData} = Value,
            UpdAcl = process_owner(OwnerData, Acl);
        <<"grants">> ->
            UpdAcl = process_grants(Value, Acl);
        <<"creation_time">> ->
            {struct, TimeData} = Value,
            CreationTime = process_creation_time(TimeData, {1,1,1}),
            UpdAcl = Acl?ACL{creation_time=CreationTime};
        _ ->
            UpdAcl = Acl
    end,
    process_acl_contents(RestObjects, UpdAcl).

%% @doc Process an JSON element containing acl owner information.
-spec process_owner([term()], acl()) -> acl2().
process_owner([], Acl) ->
    Acl;
process_owner([{Name, Value} | RestObjects], Acl) ->
    Owner = Acl?ACL.owner,
    case Name of
        <<"key_id">> ->
            _ = lager:debug("Owner Key ID value: ~p", [Value]),
            {OwnerName, OwnerCID, _} = Owner,
            UpdOwner = {OwnerName, OwnerCID, binary_to_list(Value)};
        <<"canonical_id">> ->
            _ = lager:debug("Owner ID value: ~p", [Value]),
            {OwnerName, _, OwnerId} = Owner,
            UpdOwner = {OwnerName, binary_to_list(Value), OwnerId};
        <<"display_name">> ->
            _ = lager:debug("Owner Name content: ~p", [Value]),
            {_, OwnerCID, OwnerId} = Owner,
            UpdOwner = {binary_to_list(Value), OwnerCID, OwnerId};
        _ ->
            _ = lager:debug("Encountered unexpected element: ~p", [Name]),
            UpdOwner = Owner
    end,
    process_owner(RestObjects, Acl?ACL{owner=UpdOwner}).

%% @doc Process an JSON element containing the grants for the acl.
-spec process_grants([term()], acl()) -> acl2().
process_grants([], Acl) ->
    Acl;
process_grants([{_, Value} | RestObjects], Acl) ->
    Grant = process_grant(Value, {{"", ""}, []}),
    UpdAcl = Acl?ACL{grants=[Grant | Acl?ACL.grants]},
    process_grants(RestObjects, UpdAcl).

%% @doc Process an JSON element containing information about
%% an ACL permission grants.
-spec process_grant([term()], acl_grant()) -> acl_grant().
process_grant([], Grant) ->
    Grant;
process_grant([{Name, Value} | RestObjects], Grant) ->
    case Name of
        <<"canonical_id">> ->
            _ = lager:debug("ID value: ~p", [Value]),
            {{DispName, _}, Perms} = Grant,
            UpdGrant = {{DispName, binary_to_list(Value)}, Perms};
        <<"display_name">> ->
            _ = lager:debug("Name value: ~p", [Value]),
            {{_, Id}, Perms} = Grant,
            UpdGrant = {{binary_to_list(Value), Id}, Perms};
        <<"group">> ->
            _ = lager:debug("Group value: ~p", [Value]),
            {_, Perms} = Grant,
            UpdGrant = {list_to_atom(
                          binary_to_list(Value)), Perms};
        <<"permissions">> ->
            {Grantee, _} = Grant,
            Perms = process_permissions(Value),
            _ = lager:debug("Perms value: ~p", [Value]),
            UpdGrant = {Grantee, Perms};
        _ ->
            UpdGrant = Grant
    end,
    process_grant(RestObjects, UpdGrant).

%% @doc Process a list of JSON elements containing
%% ACL permissions.
-spec process_permissions([binary()]) -> acl_perms().
process_permissions(Perms) ->
    lists:usort(
      lists:filter(fun(X) -> X /= undefined end,
                   [binary_perm_to_atom(Perm) || Perm <- Perms])).

%% @doc Convert a binary permission type to a
%% corresponding atom or return `undefined' if
%% the permission is invalid.
-spec binary_perm_to_atom(binary()) -> atom().
binary_perm_to_atom(Perm) ->
    case Perm of
        <<"FULL_CONTROL">> ->
            'FULL_CONTROL';
        <<"READ">> ->
            'READ';
        <<"READ_ACP">> ->
            'READ_ACP';
        <<"WRITE">> ->
            'WRITE';
        <<"WRITE_ACP">> ->
            'WRITE_ACP';
        _ ->
            undefined
    end.

%% @doc Process the JSON element containing creation time
%% data for an ACL.
-spec process_creation_time([term()], erlang:timestamp()) -> erlang:timestamp().
process_creation_time([], CreationTime) ->
    CreationTime;
process_creation_time([{Name, Value} | RestObjects], CreationTime) ->
    case Name of
        <<"mega_seconds">> ->
            {_, Secs, MicroSecs} = CreationTime,
            UpdCreationTime = {Value, Secs, MicroSecs};
        <<"seconds">> ->
            {MegaSecs, _, MicroSecs} = CreationTime,
            UpdCreationTime = {MegaSecs, Value, MicroSecs};
        <<"micro_seconds">> ->
            {MegaSecs, Secs, _} = CreationTime,
            UpdCreationTime = {MegaSecs, Secs, Value}
    end,
    process_creation_time(RestObjects, UpdCreationTime).

%% ===================================================================
%% Eunit tests
%% ===================================================================

-ifdef(TEST).

acl_from_json_test() ->
    CreationTime = erlang:now(),
    {AclMegaSecs, AclSecs, AclMicroSecs} = CreationTime,
    JsonTerm = [{<<"version">>,1},
                {<<"owner">>,
                 {struct,
                  [{<<"display_name">>,<<"tester1">>},
                   {<<"canonical_id">>,<<"TESTID1">>},
                   {<<"key_id">>,<<"TESTKEYID1">>}]}},
                {<<"grants">>,
                 [{struct,
                   [{<<"group">>,<<"AllUsers">>},
                    {<<"permissions">>,[<<"WRITE_ACP">>]}]},
                  {struct,
                   [{<<"display_name">>,<<"tester2">>},
                    {<<"canonical_id">>,<<"TESTID2">>},
                    {<<"permissions">>,[<<"WRITE">>]}]},
                  {struct,
                   [{<<"display_name">>,<<"tester1">>},
                    {<<"canonical_id">>,<<"TESTID1">>},
                    {<<"permissions">>,[<<"READ">>]}]}]},
                {<<"creation_time">>,
                 {struct,
                  [{<<"mega_seconds">>, AclMegaSecs},
                   {<<"seconds">>, AclSecs},
                   {<<"micro_seconds">>, AclMicroSecs}]}}],
    Acl = acl_from_json(JsonTerm),
    ExpectedAcl = acl("tester1",
                      "TESTID1",
                      "TESTKEYID1",
                      [{{"tester1", "TESTID1"}, ['READ']},
                       {{"tester2", "TESTID2"}, ['WRITE']},
                       {'AllUsers', ['WRITE_ACP']}],
                      CreationTime),
    ?assertEqual(ExpectedAcl, Acl).

acl_to_json_term_test() ->
    CreationTime = erlang:now(),
    Acl = acl("tester1",
              "TESTID1",
              "TESTKEYID1",
              [{{"tester1", "TESTID1"}, ['READ']},
               {{"tester2", "TESTID2"}, ['WRITE']}],
              CreationTime),
    JsonTerm = acl_to_json_term(Acl),
    {AclMegaSecs, AclSecs, AclMicroSecs} = CreationTime,
    ExpectedTerm = {<<"acl">>,
                    {struct,
                     [{<<"version">>,1},
                      {<<"owner">>,
                       {struct,
                        [{<<"display_name">>,<<"tester1">>},
                         {<<"canonical_id">>,<<"TESTID1">>},
                         {<<"key_id">>,<<"TESTKEYID1">>}]}},
                      {<<"grants">>,
                       [{struct,
                         [{<<"display_name">>,<<"tester2">>},
                          {<<"canonical_id">>,<<"TESTID2">>},
                          {<<"permissions">>,[<<"WRITE">>]}]},
                        {struct,
                         [{<<"display_name">>,<<"tester1">>},
                          {<<"canonical_id">>,<<"TESTID1">>},
                          {<<"permissions">>,[<<"READ">>]}]}]},
                      {<<"creation_time">>,
                       {struct,
                        [{<<"mega_seconds">>, AclMegaSecs},
                         {<<"seconds">>, AclSecs},
                         {<<"micro_seconds">>, AclMicroSecs}]}}]}},
    ?assertEqual(ExpectedTerm, JsonTerm).

owner_to_json_term_test() ->
    JsonTerm = owner_to_json_term("name", "cid123", "keyid123"),
    ExpectedTerm = {<<"owner">>,
                    {struct, [{<<"display_name">>, <<"name">>},
                              {<<"canonical_id">>, <<"cid123">>},
                              {<<"key_id">>, <<"keyid123">>}]}
                   },
    ?assertEqual(ExpectedTerm, JsonTerm).

grants_to_json_term_test() ->
    CreationTime = erlang:now(),
    Acl = acl("tester1",
              "TESTID1",
              "TESTKEYID1",
              [{{"tester1", "TESTID1"}, ['READ']},
               {{"tester2", "TESTID2"}, ['WRITE']}],
              CreationTime),
    JsonTerm = grants_to_json_term(Acl?ACL.grants, []),
    ExpectedTerm =
        {<<"grants">>, [{struct,
                         [{<<"display_name">>,<<"tester2">>},
                          {<<"canonical_id">>,<<"TESTID2">>},
                          {<<"permissions">>,[<<"WRITE">>]}]},
                        {struct,
                         [{<<"display_name">>,<<"tester1">>},
                          {<<"canonical_id">>,<<"TESTID1">>},
                          {<<"permissions">>,[<<"READ">>]}]}]},

    ?assertEqual(ExpectedTerm, JsonTerm).

grantee_to_json_term_test() ->
    JsonTerm1 = grantee_to_json_term({{"tester1", "TESTID1"}, ['READ']}),
    JsonTerm2 = grantee_to_json_term({'AllUsers', ['WRITE']}),
    ExpectedTerm1 = {struct,
                     [{<<"display_name">>,<<"tester1">>},
                      {<<"canonical_id">>,<<"TESTID1">>},
                      {<<"permissions">>,[<<"READ">>]}]},
    ExpectedTerm2 = {struct,
                     [{<<"group">>,<<"AllUsers">>},
                      {<<"permissions">>,[<<"WRITE">>]}]},
    ?assertEqual(ExpectedTerm1, JsonTerm1),
    ?assertEqual(ExpectedTerm2, JsonTerm2).

permissions_to_json_term_test() ->
    JsonTerm = permissions_to_json_term(['READ',
                                         'WRITE',
                                         'READ_ACP',
                                         'WRITE_ACP',
                                         'FULL_CONTROL']),
    ExpectedTerm = [<<"READ">>,
                    <<"WRITE">>,
                    <<"READ_ACP">>,
                    <<"WRITE_ACP">>,
                    <<"FULL_CONTROL">>],
    ?assertEqual(ExpectedTerm, JsonTerm).

erlang_time_to_json_term_test() ->
    JsonTerm = erlang_time_to_json_term({1000, 100, 10}),
    ExpectedTerm = {<<"creation_time">>,
                    {struct,
                     [{<<"mega_seconds">>, 1000},
                      {<<"seconds">>, 100},
                      {<<"micro_seconds">>, 10}]}},
    ?assertEqual(ExpectedTerm, JsonTerm).

-endif.
