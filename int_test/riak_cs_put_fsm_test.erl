%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_put_fsm_test).

-include("riak_cs.hrl").
-include_lib("eunit/include/eunit.hrl").

setup() ->
    application:start(lager),
    application:start(folsom),
    {ok, _} = riak_cs_stats:start_link().

%% TODO:
%% Implement this
teardown(_) ->
    application:stop(folsom),
    application:stop(lager),
    ok.

put_fsm_test_() ->
    {setup,
     fun setup/0,
     fun teardown/1,
     [fun small_put/0,
      fun zero_byte_put/0]}.

small_put() ->
    {ok, RiakPid} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    {ok, Pid} =
        riak_cs_put_fsm:start_link({<<"bucket">>, <<"key">>, 10, <<"ctype">>,
                                      orddict:new(), 2, riak_cs_acl_utils:default_acl("display", "canonical_id", "key_id"),
                                     60000, self(), RiakPid}),
    Data = <<"0123456789">>,
    Md5 = make_md5(Data),
    Parts = [<<"0">>, <<"123">>, <<"45">>, <<"678">>, <<"9">>],
    [riak_cs_put_fsm:augment_data(Pid, D) || D <- Parts],
    {ok, Manifest} = riak_cs_put_fsm:finalize(Pid),
    ?assert(Manifest?MANIFEST.state == active andalso
            Manifest?MANIFEST.content_md5==Md5).

zero_byte_put() ->
    {ok, RiakPid} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    {ok, Pid} =
        riak_cs_put_fsm:start_link({<<"bucket">>, <<"key">>, 0, <<"ctype">>,
                                      orddict:new(), 2, riak_cs_acl_utils:default_acl("display", "canonical_id", "key_id"),
                                     60000, self(), RiakPid}),
    Data = <<>>,
    Md5 = make_md5(Data),
    {ok, Manifest} = riak_cs_put_fsm:finalize(Pid),
    ?assert(Manifest?MANIFEST.state == active andalso
            Manifest?MANIFEST.content_md5==Md5).

%%%===================================================================
%%% Internal functions
%%%===================================================================
make_md5(Bin) ->
    M = crypto:md5_init(),
    M1 = crypto:md5_update(M, Bin),
    crypto:md5_final(M1).
