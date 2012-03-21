%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_put_fsm_test).

-include("riak_moss.hrl").
-include_lib("eunit/include/eunit.hrl").

setup() ->
    application:start(lager).

%% TODO:
%% Implement this
teardown(_) ->
    ok.

put_fsm_test_() ->
    {setup,
     fun setup/0,
     fun teardown/1,
     [fun small_put/0]}.

small_put() ->
    {ok, Pid} =
    riak_moss_put_fsm:start_link(<<"bucket">>, <<"key">>, 10, <<"ctype">>,
        orddict:new(), 2, riak_moss_acl_utils:default_acl("display", "canonical_id"),
        60000, self()),
    Data = <<"0123456789">>,
    Md5 = make_md5(Data),
    Parts = [<<"0">>, <<"123">>, <<"45">>, <<"678">>, <<"9">>],
    [riak_moss_put_fsm:augment_data(Pid, D) || D <- Parts],
    {ok, Manifest} = riak_moss_put_fsm:finalize(Pid),
    ?assert(Manifest#lfs_manifest_v2.state == active andalso
            Manifest#lfs_manifest_v2.content_md5==Md5).

%%%===================================================================
%%% Internal functions
%%%===================================================================
make_md5(Bin) ->
    M = crypto:md5_init(),
    M1 = crypto:md5_update(M, Bin),
    crypto:md5_final(M1).
