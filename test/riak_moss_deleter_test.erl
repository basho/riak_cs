%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Test module for `riak_moss_deleter'.

-module(riak_moss_deleter_test).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-export([receiver/1]).

%% ===================================================================
%% Eunit tests
%% ===================================================================

deleter_response_test_() ->
    {spawn,
     [
      {setup,
       fun setup/0,
       fun cleanup/1,
       fun(X) ->
               [initialize_case(X),
                delete_root_case(X),
                update_root_case(X),
                delete_block_case(X)
               ]
       end
      }]}.

initialize_case({DeleterPid, ReceiverPid}) ->
    Result = riak_moss_deleter:initialize(DeleterPid,
                                         ReceiverPid,
                                         <<"testbucket">>,
                                         <<"testfile">>),
    {"initialize response test",
     fun() ->
             [
              ?_assertEqual(ok, Result),
              ?_assertMatch({deleter_ready, object}, get_response(ReceiverPid))
             ]
     end
    }.

delete_root_case({DeleterPid, ReceiverPid}) ->
    {"delete_root response test",
     fun() ->
             [
              ?_assertEqual(ok, riak_moss_deleter:delete_root(DeleterPid)),
              ?_assertEqual(root_deleted, get_response(ReceiverPid))
             ]
     end
    }.

update_root_case({DeleterPid, ReceiverPid}) ->
    {"update_root response test",
     fun() ->
             [
              ?_assertEqual(ok, riak_moss_deleter:update_root(DeleterPid, set_inactive)),
              ?_assertEqual(root_inactive, get_response(ReceiverPid))
             ]
     end
    }.

delete_block_case({DeleterPid, ReceiverPid}) ->
    {"delete_block response test",
     fun() ->
             [
              ?_assertEqual(ok, riak_moss_deleter:delete_block(DeleterPid, 1)),
              ?_assertMatch({block_deleted, _}, get_response(ReceiverPid))
             ]
     end
    }.

setup() ->
    {ok, DeleterPid} = riak_moss_deleter:test_link(),
    ReceiverPid = spawn_link(?MODULE, receiver, [undefined]),
    {DeleterPid, ReceiverPid}.

cleanup({_, ReceiverPid}) ->
    ReceiverPid ! finish,
    ok.

receiver(LastResponse) ->
    receive
        {get_last_response, TestPid} ->
            TestPid ! LastResponse,
            receiver(LastResponse);
        {_, Response} ->
            receiver(Response);
        finish ->
            ok
    end.

get_response(Pid) ->
    Pid ! {get_last_response, self()},
    receive
        undefined ->
            get_response(Pid);
        Response ->
            Response
    end.

-endif.
