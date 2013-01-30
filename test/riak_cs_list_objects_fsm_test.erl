%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_list_objects_fsm_test).

-compile(export_all).

-ifdef(TEST).

-include("riak_cs.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Tests for `riak_cs_list_objects_fsm:filter_prefix_keys/3'

%% A request with no prefix, delimiter, or marker
%% should just return the same list of keys
simple_request_test() ->
    Keys = keys(),
    Request = riak_cs_list_objects:new_request(<<"bucket">>),
    {ResultKeys, _CommonPrefixes} =
        riak_cs_list_objects_fsm:filter_prefix_keys(Keys, [], Request),
    ?assertEqual(lists:sort(Keys), lists:sort(ResultKeys)).

prefix_request_1_test() ->
    Keys = keys(),
    Request = riak_cs_list_objects:new_request(<<"bucket">>, 1000,
                                               [{prefix, <<"a">>}]),
    Result =
        riak_cs_list_objects_fsm:filter_prefix_keys(Keys, [], Request),
    Expected = {[<<"a">>], []},
    ?assertEqual(Expected, Result).

prefix_request_2_test() ->
    Keys = keys(),
    Request = riak_cs_list_objects:new_request(<<"bucket">>, 1000,
                                               [{prefix, <<"photos/">>}]),
    {ResultKeys, CommonPrefixes} =
        riak_cs_list_objects_fsm:filter_prefix_keys(Keys, [], Request),
    Expected = {lists:sort(lists:sublist(Keys, 4, length(Keys))), []},
    ?assertEqual(Expected, {lists:sort(ResultKeys), CommonPrefixes}).

prefix_and_delimiter_1_test() ->
    Keys = keys(),
    Request = riak_cs_list_objects:new_request(<<"bucket">>, 1000,
                                               [{prefix, <<"photos/">>},
                                                {delimiter, <<"/">>}]),
    {ResultKeys, CommonPrefixes} =
        riak_cs_list_objects_fsm:filter_prefix_keys(Keys, [], Request),
    Expected = {[], lists:sort([<<"photos/01/">>, <<"photos/02/">>])},
    ?assertEqual(Expected, {ResultKeys, lists:sort(CommonPrefixes)}).

%% The only difference from the above test is
%% in the `prefix', note the lack of `/' after `photos'
prefix_and_delimiter_2_test() ->
    Keys = keys(),
    Request = riak_cs_list_objects:new_request(<<"bucket">>, 1000,
                                               [{prefix, <<"photos">>},
                                                {delimiter, <<"/">>}]),
    {ResultKeys, CommonPrefixes} =
        riak_cs_list_objects_fsm:filter_prefix_keys(Keys, [], Request),
    Expected = {[], lists:sort([<<"photos/">>])},
    ?assertEqual(Expected, {ResultKeys, lists:sort(CommonPrefixes)}).

prefix_and_delimiter_3_test() ->
    Keys = keys(),
    Request = riak_cs_list_objects:new_request(<<"bucket">>, 1000,
                                               [{delimiter, <<"/">>}]),
    {ResultKeys, CommonPrefixes} =
        riak_cs_list_objects_fsm:filter_prefix_keys(Keys, [], Request),
    Expected = {[<<"a">>, <<"b">>, <<"c">>], [<<"photos/">>]},
    ?assertEqual(Expected, {lists:sort(ResultKeys), CommonPrefixes}).

%% Test helpers

keys() ->
    [<<"a">>, <<"b">>, <<"c">>, <<"photos/01/foo">>, <<"photos/01/bar">>,
     <<"photos/02/baz">>, <<"photos/02/quz">>].

-endif.
