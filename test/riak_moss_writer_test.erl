%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Test module for `riak_moss_writer'.

-module(riak_moss_writer_test).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-export([receiver/1]).

%% ===================================================================
%% Eunit tests
%% ===================================================================

writer_response_test_() ->
    {spawn,
     [
      {setup,
       fun setup/0,
       fun cleanup/1,
       fun(X) ->
               [initialize_case(X),
                write_root_case(X),
                update_root_case(X),
                write_block_case(X)
               ]
       end
      }]}.

initialize_case({WriterPid, ReceiverPid}) ->
    Result = riak_moss_writer:initialize(WriterPid,
                                         ReceiverPid,
                                         <<"testbucket">>,
                                         <<"testfile">>,
                                         1024,
                                        "text/plain"),
    {"initialize response test",
     fun() ->
             [
              ?_assertEqual(ok, Result),
              ?_assertEqual(writer_ready, get_response(ReceiverPid))
             ]
     end
    }.

write_root_case({WriterPid, ReceiverPid}) ->
    {"write_root response test",
     fun() ->
             [
              ?_assertEqual(ok, riak_moss_writer:write_root(WriterPid)),
              ?_assertEqual(root_ready, get_response(ReceiverPid))
             ]
     end
    }.

update_root_case({WriterPid, ReceiverPid}) ->
    {"update_root response test",
     fun() ->
             [
              ?_assertEqual(ok, riak_moss_writer:update_root(WriterPid, {block_ready, 1})),
              ?_assertEqual(root_ready, get_response(ReceiverPid))
             ]
     end
    }.

write_block_case({WriterPid, ReceiverPid}) ->
    {"write_block response test",
     fun() ->
             [
              ?_assertEqual(ok, riak_moss_writer:write_block(WriterPid, 1, <<"data">>)),
              ?_assertEqual(block_ready, get_response(ReceiverPid))
             ]
     end
    }.

setup() ->
    {ok, WriterPid} = riak_moss_writer:test_link(),
    ReceiverPid = spawn_link(?MODULE, receiver, [undefined]),
    {WriterPid, ReceiverPid}.

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
