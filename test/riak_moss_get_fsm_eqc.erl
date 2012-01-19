%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Quickcheck test module for `riak_moss_get_fsm'.

-module(riak_moss_get_fsm_eqc).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Public API
-compile(export_all).
-export([test/0, test/1]).

%% eqc_fsm callbacks
-export([initial_state/0,
         initial_state_data/0,
         next_state_data/5,
         precondition/4,
         postcondition/5]).

%% eqc property
-export([prop_get_fsm/0]).

%% States
-export([start/1]).

%% Helpers
-export([]).

-define(TEST_ITERATIONS, 500).

-record(state, {fsm_pid :: pid(), %% real pid}
                content_length :: integer(), %% not symbolic
                total_blocks :: integer(), %% not symbolic
                last_chunk :: binary(), %% not symbolic
                counter=0 :: integer()}). %% not symbolic

%% ====================================================================
%% Public API
%% ====================================================================

test() ->
    test(?TEST_ITERATIONS).

test(Iterations) ->
    eqc:quickcheck(eqc:numtests(Iterations, prop_get_fsm())).

%% ====================================================================
%% eqc property
%% ====================================================================

prop_get_fsm() ->
    ?FORALL(Cmds, commands(?MODULE),
        #state{content_length=moss_gen:bounded_content_length()},
            begin
                {H,{_F,S},Res} = run_commands(?MODULE, Cmds),
                equals(ok, Res)
            end).

%%====================================================================
%% Generators
%%====================================================================

%%====================================================================
%% Helpers
%%====================================================================

check_chunk(Counter, Chunk) ->
    <<Counter:4/integer, _>> = Chunk.

%%====================================================================
%% eqc_fsm callbacks
%%====================================================================

initial_state() ->
    {start, true}.

next_state_data(start, waiting_chunk, #state{content_length=ContentLength}=S, _R, _C) ->
    BlockSize = riak_moss_lfs_utils:block_size(),
    {ok, FSMPid} = riak_moss_get_fsm:test_link(<<"bucket">>, <<"key">>, ContentLength, BlockSize),
    BlockCount = riak_moss_lfs_utils:block_count(ContentLength, BlockSize),
    _Metadata = riak_moss_get_fsm:get_metadata(FSMPid),
    riak_moss_get_fsm:continue(FSMPid),
    S#state{fsm_pid=FSMPid, total_blocks=BlockCount};
next_state_data(waiting_chunk, waiting_chunk, #state{fsm_pid=FSMPid}=S, _R, _C) ->
    NextChunk = riak_moss_get_fsm:get_next_chunk(FSMPid),
    S#state{last_chunk=NextChunk}.
next_state_data(_From, _To, S, _R, _C) ->
    S.

start(#state{content_length=ContentLength}) ->
    [{waiting_chunk, {call, ?MODULE, waiting_chunk, []}}].

waiting_chunk(S) ->
    [{waiting_chunk, {call, ?MODULE, waiting_chunk, []}},
     {stop, {call, ?MODULE, stop, []}}].

stop(S) ->
    [].

precondition(waiting_chunk,stop,S#{counter=Counter, total_blocks=TotalBlocks,_C) ->
    Counter == TotalBlocks;
precondition(_From,_To,_S,_C) ->
    true.

postcondition(waiting_chunk, waiting_chunk, #state{counter=Counter,last_chunk={done, Chunk},total_blocks=Counter, _C, _R) ->
    check_chunk(Counter, Chunk);
postcondition(waiting_chunk, waiting_chunk, #state{counter=Counter,last_chunk={chunk, Chunk}, _C, _R) ->
    check_chunk(Counter, Chunk);
postcondition(_From, _To, _S, _C, _R) ->
    true.

-endif.

