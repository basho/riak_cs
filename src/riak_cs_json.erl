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

%% @doc A collection functions for going to or from JSON to an erlang
%% record type.

-module(riak_cs_json).

-include("riak_cs.hrl").
-include("list_objects.hrl").
-include("oos_api.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Public API
-export([from_json/1,
         get/2,
         to_json/1,
         value_or_default/2]).

-type attributes() :: [{atom(), string()}].
-type external_node() :: {atom(), [string()]}.
-type internal_node() :: {atom(), [internal_node() | external_node()]} |
                         {atom(), attributes(), [internal_node() | external_node()]}.

%% ===================================================================
%% Public API
%% ===================================================================

-spec from_json(string()) -> {struct, term()} | [term()] | {error, decode_failed}.
from_json(JsonString) ->
    case catch mochijson2:decode(JsonString) of
        {'EXIT', _} ->
            {error, decode_failed};
        Result ->
            Result
    end.

-type match_spec() :: {index, non_neg_integer()} | {key, binary(), binary()}.
-type path_query() :: {find, match_spec()}.
-type path() :: [binary() | path_query()].
-spec get({struct, term()} | [term()] | undefined, path()) -> term().
get({struct, _}=Object, Path) ->
    follow_path(Object, Path);
get(Array, [{find, Query} | RestPath]) when is_list(Array) ->
    follow_path(find(Array, Query), RestPath);
get(_Array, [{find, _Query} | _RestPath]) ->
    {error, invalid_path};
get(undefined, _) ->
    {error, not_found};
get(not_found, _) ->
    {error, not_found}.

-spec to_json(term()) -> binary().
to_json(?KEYSTONE_S3_AUTH_REQ{}=Req) ->
    Inner = {struct, [{<<"access">>, Req?KEYSTONE_S3_AUTH_REQ.access},
                      {<<"signature">>, Req?KEYSTONE_S3_AUTH_REQ.signature},
                      {<<"token">>, Req?KEYSTONE_S3_AUTH_REQ.token}]},
    iolist_to_binary(mochijson2:encode({struct, [{<<"credentials">>, Inner}]}));
to_json(undefined) ->
    [];
to_json([]) ->
    [].

-spec value_or_default({ok, term()} | {error, term()}, term()) -> term().
value_or_default({error, Reason}, Default) ->
    _ = lager:debug("JSON error: ~p", [Reason]),
    Default;
value_or_default({ok, Value}, _) ->
    Value.

%% ===================================================================
%% Internal functions
%% ===================================================================

-spec follow_path(tuple() | [term()] | undefined, path()) ->
                         {ok, term()} | {error, not_found}.
follow_path(undefined, _) ->
    {error, not_found};
follow_path(Value, []) ->
    {ok, Value};
follow_path(JsonItems, [{find, Query}]) ->
    follow_path(find(JsonItems, Query), []);
follow_path(JsonItems, [{find, Query} | RestPath]) ->
    get(find(JsonItems, Query), RestPath);
follow_path({struct, JsonItems}, [Key]) when is_tuple(Key) ->
    follow_path(target_tuple_values(Key, JsonItems), []);
follow_path({struct, JsonItems}, [Key]) ->
    follow_path(proplists:get_value(Key, JsonItems), []);
follow_path({struct, JsonItems}, [Key | RestPath]) ->
    Value = proplists:get_value(Key, JsonItems),
    follow_path(Value, RestPath).

-spec find([term()], match_spec()) -> undefined | {struct, term()}.
find(Array, {key, Key, Value}) ->
    lists:foldl(key_folder(Key, Value), not_found, Array);
find(Array, {index, Index}) when Index =< length(Array) ->
    lists:nth(Index, Array);
find(_, {index, _}) ->
    undefined.

key_folder(Key, Value) ->
    fun({struct, Items}=X, Acc) ->
            case lists:keyfind(Key, 1, Items) of
                {Key, Value} ->
                    X;
                _ ->
                    Acc
            end;
       (_, Acc) ->
            Acc
    end.

-spec target_tuple_values(tuple(), proplists:proplist()) -> tuple().
target_tuple_values(Keys, JsonItems) ->
    list_to_tuple(
      [proplists:get_value(element(Index, Keys), JsonItems)
       || Index <- lists:seq(1, tuple_size(Keys))]).


%% ===================================================================
%% Eunit tests
%% ===================================================================
-ifdef(TEST).

get_single_key_test() ->
    Object1 = "{\"abc\":\"123\", \"def\":\"456\", \"ghi\":\"789\"}",
    Object2 = "{\"test\":{\"abc\":\"123\", \"def\":\"456\", \"ghi\":\"789\"}}",
    ?assertEqual({ok, <<"123">>}, get(from_json(Object1), [<<"abc">>])),
    ?assertEqual({ok, <<"456">>}, get(from_json(Object1), [<<"def">>])),
    ?assertEqual({ok, <<"789">>}, get(from_json(Object1), [<<"ghi">>])),
    ?assertEqual({ok, <<"123">>}, get(from_json(Object2), [<<"test">>, <<"abc">>])),
    ?assertEqual({ok, <<"456">>}, get(from_json(Object2), [<<"test">>, <<"def">>])),
    ?assertEqual({ok, <<"789">>}, get(from_json(Object2), [<<"test">>, <<"ghi">>])),
    ?assertEqual({error, not_found}, get(from_json(Object1), [<<"zzz">>])),
    ?assertEqual({error, not_found}, get(from_json(Object2), [<<"test">>, <<"zzz">>])).

get_array_test() ->
    Array = "[\"abc\", \"123\", \"def\", \"456\", 7]",
    ?assertEqual({ok, <<"abc">>}, get(from_json(Array), [{find, {index, 1}}])),
    ?assertEqual({ok, <<"123">>}, get(from_json(Array), [{find, {index, 2}}])),
    ?assertEqual({ok, <<"def">>}, get(from_json(Array), [{find, {index, 3}}])),
    ?assertEqual({ok, <<"456">>}, get(from_json(Array), [{find, {index, 4}}])),
    ?assertEqual({ok, 7}, get(from_json(Array), [{find, {index, 5}}])),
    ?assertEqual({error, not_found}, get(from_json(Array), [{find, {index, 6}}])).

get_multi_key_test() ->
    Object1 = "{\"test\":{\"abc\":\"123\", \"def\":\"456\", \"ghi\":\"789\"}}",
    Object2 = "{\"test\":{\"abc\":{\"123\":123,\"456\":456,\"789\":789},\"def\""
        ":{\"123\":123,\"456\":456,\"789\":789},\"ghi\":{\"123\":123,\"456\""
        ":456,\"789\":789}}}",
    ?assertEqual({ok, {<<"123">>, <<"789">>}}, get(from_json(Object1), [<<"test">>, {<<"abc">>, <<"ghi">>}])),
    ?assertEqual({ok, {123, 789}}, get(from_json(Object2), [<<"test">>, <<"abc">>, {<<"123">>, <<"789">>}])),
    ?assertEqual({ok, {123, 789}}, get(from_json(Object2), [<<"test">>, <<"def">>, {<<"123">>, <<"789">>}])),
    ?assertEqual({ok, {123, 789}}, get(from_json(Object2), [<<"test">>, <<"ghi">>, {<<"123">>, <<"789">>}])).

get_embedded_key_from_array_test() ->
    Object = "{\"test\":{\"objects\":[{\"key1\":\"a1\",\"key2\":\"a2\",\"key3\""
        ":\"a3\"},{\"key1\":\"b1\",\"key2\":\"b2\",\"key3\":\"b3\"},{\"key1\""
        ":\"c1\",\"key2\":\"c2\",\"key3\":\"c3\"}]}}",
    ?assertEqual({ok, {struct, [{<<"key1">>, <<"a1">>}, {<<"key2">>, <<"a2">>}, {<<"key3">>, <<"a3">>}]}},
                 get(from_json(Object),
                     [<<"test">>, <<"objects">>, {find, {key, <<"key1">>, <<"a1">>}}])),
    ?assertEqual({ok, {struct, [{<<"key1">>, <<"a1">>}, {<<"key2">>, <<"a2">>}, {<<"key3">>, <<"a3">>}]}},
                 get(from_json(Object),
                     [<<"test">>, <<"objects">>, {find, {key, <<"key2">>, <<"a2">>}}])),
    ?assertEqual({ok, {struct, [{<<"key1">>, <<"a1">>}, {<<"key2">>, <<"a2">>}, {<<"key3">>, <<"a3">>}]}},
                 get(from_json(Object),
                     [<<"test">>, <<"objects">>, {find, {key, <<"key3">>, <<"a3">>}}])),
    ?assertEqual({ok, {struct, [{<<"key1">>, <<"b1">>}, {<<"key2">>, <<"b2">>}, {<<"key3">>, <<"b3">>}]}},
                 get(from_json(Object),
                     [<<"test">>, <<"objects">>, {find, {key, <<"key1">>, <<"b1">>}}])),
    ?assertEqual({ok, {struct, [{<<"key1">>, <<"b1">>}, {<<"key2">>, <<"b2">>}, {<<"key3">>, <<"b3">>}]}},
                 get(from_json(Object),
                     [<<"test">>, <<"objects">>, {find, {key, <<"key2">>, <<"b2">>}}])),
    ?assertEqual({ok, {struct, [{<<"key1">>, <<"b1">>}, {<<"key2">>, <<"b2">>}, {<<"key3">>, <<"b3">>}]}},
                 get(from_json(Object),
                     [<<"test">>, <<"objects">>, {find, {key, <<"key3">>, <<"b3">>}}])),
    ?assertEqual({ok, {struct, [{<<"key1">>, <<"c1">>}, {<<"key2">>, <<"c2">>}, {<<"key3">>, <<"c3">>}]}},
                 get(from_json(Object),
                     [<<"test">>, <<"objects">>, {find, {key, <<"key1">>, <<"c1">>}}])),
    ?assertEqual({ok, {struct, [{<<"key1">>, <<"c1">>}, {<<"key2">>, <<"c2">>}, {<<"key3">>, <<"c3">>}]}},
                 get(from_json(Object),
                     [<<"test">>, <<"objects">>, {find, {key, <<"key2">>, <<"c2">>}}])),
    ?assertEqual({ok, {struct, [{<<"key1">>, <<"c1">>}, {<<"key2">>, <<"c2">>}, {<<"key3">>, <<"c3">>}]}},
                 get(from_json(Object),
                     [<<"test">>, <<"objects">>, {find, {key, <<"key3">>, <<"c3">>}}])).


-endif.
