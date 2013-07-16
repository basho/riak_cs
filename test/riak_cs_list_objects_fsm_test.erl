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

-module(riak_cs_list_objects_fsm_test).

-compile(export_all).

-ifdef(TEST).

-include("riak_cs.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Tests for `riak_cs_list_objects_fsm:filter_prefix_keys/3'

filter_prefix_keys_test_() ->
    [
        %% simple test
        test_creator(riak_cs_list_objects:new_request(<<"bucket">>),
                     {keys_and_manifests(), []}),

        %% simple prefix
        test_creator(riak_cs_list_objects:new_request(<<"bucket">>,
                                                      1000,
                                                      [{prefix, <<"a">>}]),
                     {keys_and_manifests([<<"a">>]), []}),

        %% simple prefix 2
        test_creator(riak_cs_list_objects:new_request(<<"bucket">>,
                                                      1000,
                                                      [{prefix, <<"photos/">>}]),
                     {lists:sublist(keys_and_manifests(), 4,
                      length(keys_and_manifests())), []}),

        %% prefix and delimiter
        test_creator(riak_cs_list_objects:new_request(<<"bucket">>,
                                                      1000,
                                                      [{prefix, <<"photos/">>},
                                                       {delimiter, <<"/">>}]),
                     {[], [<<"photos/01/">>, <<"photos/02/">>]}),

        %% prefix and delimiter 2
        %% The only difference from the above test is
        %% in the `prefix', note the lack of `/' after `photos'
        test_creator(riak_cs_list_objects:new_request(<<"bucket">>,
                                                      1000,
                                                      [{prefix, <<"photos">>},
                                                       {delimiter, <<"/">>}]),
                     {[], [<<"photos/">>]}),

        %% prefix and delimiter
        test_creator(riak_cs_list_objects:new_request(<<"bucket">>,
                                                      1000,
                                                      [{delimiter, <<"/">>}]),
                     {keys_and_manifests([<<"a">>, <<"b">>, <<"c">>]),
                      [<<"photos/">>]})
    ].

%% Test creator

test_creator(Request, Expected) ->
    test_creator(keys_and_manifests(), Request, Expected).

test_creator(Keys, Request, Expected) ->
    fun () ->
            Result =
                riak_cs_list_objects_fsm:filter_prefix_keys(Keys, [], Request),
            ?assertEqual(two_tuple_sort(Expected),
                         two_tuple_sort(Result))
    end.

%% Test helpers

two_tuple_sort({A, B}) ->
    {lists:sort(A),
     lists:sort(B)}.

keys_and_manifests() ->
    keys_and_manifests(keys()).

keys_and_manifests(Items) ->
    [{K, fake_manifest} || K <- Items].

keys() ->
    [<<"a">>, <<"b">>, <<"c">>, <<"photos/01/foo">>, <<"photos/01/bar">>,
     <<"photos/02/baz">>, <<"photos/02/quz">>].

-endif.
