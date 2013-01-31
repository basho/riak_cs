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

filter_prefix_keys_test_() ->
    [
        %% simple test
        test_creator(riak_cs_list_objects:new_request(<<"bucket">>),
                     {keys(), []}),

        %% simple prefix
        test_creator(riak_cs_list_objects:new_request(<<"bucket">>,
                                                      1000,
                                                      [{prefix, <<"a">>}]),
                     {[<<"a">>], []}),

        %% simple prefix 2
        test_creator(riak_cs_list_objects:new_request(<<"bucket">>,
                                                      1000,
                                                      [{prefix, <<"photos/">>}]),
                     {lists:sublist(keys(), 4, length(keys())), []}),

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
                     {[<<"a">>, <<"b">>, <<"c">>], [<<"photos/">>]})
    ].

%% Test creator

test_creator(Request, Expected) ->
    test_creator(keys(), Request, Expected).

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

keys() ->
    [<<"a">>, <<"b">>, <<"c">>, <<"photos/01/foo">>, <<"photos/01/bar">>,
     <<"photos/02/baz">>, <<"photos/02/quz">>].

-endif.
