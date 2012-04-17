%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_utils_test).

-ifdef(TEST).

-include("riak_moss.hrl").
-include_lib("eunit/include/eunit.hrl").

setup() ->
    %% Silence logging
    application:load(sasl),
    application:load(riak_moss),
    application:set_env(sasl, sasl_error_logger, {file, "riak_moss_utils_sasl.log"}),
    error_logger:tty(false),

    %% Start erlang node
    application:start(sasl),
    TestNode = list_to_atom("testnode" ++ integer_to_list(element(3, now()))),
    net_kernel:start([TestNode, shortnames]),
    application:start(lager),
    application:start(riakc),
    application:start(inets),
    application:start(mochiweb),
    application:start(webmachine),
    application:start(crypto),
    application:start(riakc),
    application:start(riak_moss).

%% TODO:
%% Implement this
teardown(_) ->
    ok.

riak_moss_utils_test_() ->
    {spawn,
     [
      {setup,
       fun setup/0,
       fun teardown/1,
       fun(_X) ->
               [
               ]
       end
      }]}.

bucket_resolution_test() ->
    %% @TODO Replace or augment this with eqc testing.
    UserRecord = riak_moss_utils:user_record("uncle fester", "fester@tester.com"),
    BucketList1 = [riak_moss_utils:bucket_record(<<"bucket1">>, create),
                   riak_moss_utils:bucket_record(<<"bucket2">>, create),
                   riak_moss_utils:bucket_record(<<"bucket3">>, create)],
    BucketList2 = [riak_moss_utils:bucket_record(<<"bucket1">>, create),
                   riak_moss_utils:bucket_record(<<"bucket1">>, create),
                   riak_moss_utils:bucket_record(<<"bucket1">>, create)],
    BucketList3 = [riak_moss_utils:bucket_record(<<"bucket1">>, create),
                   riak_moss_utils:bucket_record(<<"bucket1">>, delete),
                   riak_moss_utils:bucket_record(<<"bucket1">>, create)],
    BucketList4 = [riak_moss_utils:bucket_record(<<"bucket1">>, create),
                   riak_moss_utils:bucket_record(<<"bucket1">>, create),
                   riak_moss_utils:bucket_record(<<"bucket1">>, delete)],
    BucketList5 = [riak_moss_utils:bucket_record(<<"bucket1">>, delete),
                   riak_moss_utils:bucket_record(<<"bucket1">>, delete),
                   riak_moss_utils:bucket_record(<<"bucket1">>, delete)],
    Obj1 = riakc_obj:new_obj(<<"bucket">>,
                            <<"key">>,
                            <<"value">>,
                            [{[], UserRecord?MOSS_USER{buckets=[Buckets]}} ||
                                Buckets <- BucketList1]),
    Obj2 = riakc_obj:new_obj(<<"bucket">>,
                            <<"key">>,
                            <<"value">>,
                            [{[], UserRecord?MOSS_USER{buckets=[Buckets]}} ||
                                Buckets <- BucketList2]),
    Obj3 = riakc_obj:new_obj(<<"bucket">>,
                            <<"key">>,
                            <<"value">>,
                            [{[], UserRecord?MOSS_USER{buckets=[Buckets]}} ||
                                Buckets <- BucketList3]),
    Obj4 = riakc_obj:new_obj(<<"bucket">>,
                            <<"key">>,
                            <<"value">>,
                            [{[], UserRecord?MOSS_USER{buckets=[Buckets]}} ||
                                Buckets <- BucketList4]),
    Obj5 = riakc_obj:new_obj(<<"bucket">>,
                            <<"key">>,
                            <<"value">>,
                            [{[], UserRecord?MOSS_USER{buckets=[Buckets]}} ||
                                Buckets <- BucketList5]),
    Values1 = riakc_obj:get_values(Obj1),
    Values2 = riakc_obj:get_values(Obj2),
    Values3 = riakc_obj:get_values(Obj3),
    Values4 = riakc_obj:get_values(Obj4),
    Values5 = riakc_obj:get_values(Obj5),
    ResBuckets1 = riak_moss_utils:resolve_buckets(Values1, [], true),
    ResBuckets2 = riak_moss_utils:resolve_buckets(Values2, [], true),
    ResBuckets3 = riak_moss_utils:resolve_buckets(Values3, [], true),
    ResBuckets4 = riak_moss_utils:resolve_buckets(Values4, [], true),
    ResBuckets5 = riak_moss_utils:resolve_buckets(Values5, [], true),
    ?assertEqual(BucketList1, ResBuckets1),
    ?assertEqual([hd(BucketList2)], ResBuckets2),
    ?assertEqual([hd(lists:reverse(BucketList3))], ResBuckets3),
    ?assertEqual([hd(lists:reverse(BucketList4))], ResBuckets4),
    ?assertEqual([hd(BucketList5)], ResBuckets5).

-endif.
