%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_fault_injection).

-behaviour(gen_server).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).                           % debugging
%% API
-export([start_link/0, clear_fault_list/0,
         set_fault_list/1, get_fault_list/0,
         get_fault/1, get_step/0, reset_step/0, set_step/1,
         apply/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-type fault_action()    :: term().              % Not 'none'!
-type fault_list_item() :: {fault_step(), fault_action()}.
-type fault_name()      :: atom().
-type fault_step()      :: {fault_name(), step_number()}.
-type step_number()     :: non_neg_integer().

-record(state, {
          d :: dict(),
          step = 1 :: non_neg_integer()
         }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

clear_fault_list() ->
    set_fault_list([]).

-spec set_fault_list([fault_list_item()]) -> ok.
set_fault_list(List) ->
    gen_server:call(?SERVER, {set_fault_list, List}, infinity).

-spec get_fault_list() -> [fault_list_item()].
get_fault_list() ->
    gen_server:call(?SERVER, {get_fault_list}, infinity).

-spec get_fault(fault_name()) -> none | fault_action().
get_fault(Fault) ->
    try
        gen_server:call(?SERVER, {get_fault, Fault}, infinity)
    catch _:_ ->
            none
    end.

get_step() ->
    gen_server:call(?SERVER, {get_step}, infinity).

reset_step() ->
    set_step(1).

set_step(N) when is_integer(N), N >= 1 ->
    gen_server:call(?SERVER, {set_step, N}, infinity).

apply(F) when is_function(F, 0), not is_tuple(F)  ->
    %% !@#$! you 2-tuple fun-not-fun
    F();
apply({mfa, M, F, A}) ->
    erlang:apply(M, F, A);
apply(Else) ->
    Else.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    {ok, #state{d = dict:new(),
                step = 1}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({get_fault, Fault}, _From,
            #state{d = Dict, step = Step} = State) ->
    Reply = case dict:find({Fault, Step}, Dict) of
                error ->
                    none;
                {ok, Val} ->
                    Val
            end,
    {reply, Reply, State#state{step = Step + 1}};
handle_call({set_fault_list, List}, _From, State) ->
    {reply, ok, State#state{d = dict:from_list(List)}};
handle_call({get_fault_list}, _From, #state{d = Dict} = State) ->
    Res = lists:sort(fun({{_,X},_}, {{_, Y},_}) ->
                             X < Y
                     end, dict:to_list(Dict)),
    {reply, Res, State};
handle_call({get_step}, _From, State) ->
    {reply, State#state.step, State};
handle_call({set_step, N}, _From, State) ->
    {reply, N, State#state{step = N}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_set_faults_test() ->
    Cleanup = fun() -> catch exit(whereis(?SERVER), kill), timer:sleep(50) end,
    Cleanup(),
    Fs = [{{get, 1}, {error,notfound}},
          {{get, 3}, {error,notfound3}}, {{put, 2}, bummer}],
    try
        spawn(fun() -> {ok, _} = ?MODULE:start_link(), timer:sleep(1000) end),
        timer:sleep(50),
        ok = ?MODULE:set_fault_list(Fs),
        true = (lists:usort(Fs) == lists:usort(?MODULE:get_fault_list())),
        {error,notfound} = ?MODULE:get_fault(get),
        bummer = ?MODULE:get_fault(put),
        {error,notfound3} = ?MODULE:get_fault(get),
        none = ?MODULE:get_fault(get),
        none = ?MODULE:get_fault(put),
        none = ?MODULE:get_fault(zaweeeeeeeeee),
        _= ?MODULE:reset_step(),
        {error,notfound9} = ?MODULE:get_fault(get)
    catch X:Y ->
            exit({error, X, Y, erlang:get_stacktrace()})
    after
            Cleanup()
    end.

-ifdef(FOOFOO).

original_name(Name) -> list_to_atom(atom_to_list(Name) ++ "_meck_original").

diff --git a/src/meck.erl b/src/meck.erl
index 1669c32..6f8a577 100644
--- a/src/meck.erl
+++ b/src/meck.erl
@@ -40,6 +40,7 @@
 -export([called/4]).
 -export([num_calls/3]).
 -export([num_calls/4]).
+-export([orig_apply/3]).
 
 %% Callback exports
 -export([init/1]).
@@ -455,6 +456,9 @@ proc_name(Name) -> list_to_atom(atom_to_list(Name) ++ "_meck").
 
 original_name(Name) -> list_to_atom(atom_to_list(Name) ++ "_meck_original").
 
+orig_apply(Name, Func, Args) ->
+    erlang:apply(original_name(Name), Func, Args).
+
 wait_for_exit(Mod) ->
     MonitorRef = erlang:monitor(process, proc_name(Mod)),
     receive {'DOWN', MonitorRef, _Type, _Object, _Info} -> ok end.

-endif.


-endif. % EQC
