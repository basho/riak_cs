%% -------------------------------------------------------------------
%%
%% Copyright (c) 2023 TI Tokyo    All Rights Reserved.
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
%% -------------------------------------------------------------------

%% Object locking: application-level lock-for-update, originally for
%% AttachRolePolicy and similar, which involve reading and updating of
%% multiple logically linked IAM entities.

-module(stanchion_lock).

-export([acquire/1,
         release/2,
         cleanup/0
        ]).
-export([start_link/0
        ]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

-define(SERVER, ?MODULE).

-include("riak_cs.hrl").
-include_lib("kernel/include/logger.hrl").

-define(ACQUIRE_WAIT_MSEC, 300).
-define(AUTO_RELEASE_MSEC, 2000).
-define(MAX_ACQUIRE_WAIT_MSEC, 5000).

-spec acquire(binary()) -> binary() | busy.
acquire(A) ->
    acquire(A, 0).
acquire(A, TT) ->
    case gen_server:call(?SERVER, {acquire, A}, infinity) of
        busy ->
            ?LOG_DEBUG("waiting tick ~b to acquire lock on ~p", [TT, A]),
            timer:sleep(?ACQUIRE_WAIT_MSEC),
            acquire(A, TT + 1);
        Precious ->
            Precious
    end.

-spec release(binary(), binary()) -> ok.
release(A, Precious) ->
    gen_server:call(?SERVER, {release, A, Precious}, infinity).

-spec cleanup() -> ok.
cleanup() ->
    gen_server:call(?SERVER, cleanup, infinity).


-record(state, {pbc :: undefined | pid()}).


-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?SERVER, [], []).

-spec init([]) -> {ok, #state{}}.
init([]) ->
    {ok, Pbc} = riak_cs_utils:riak_connection(),
    {ok, #state{pbc = Pbc}}.

-spec handle_call(term(), {pid(), term()}, #state{}) ->
          {reply, ok, #state{}}.
handle_call({acquire, A}, _From, State = #state{pbc = Pbc}) ->
    {reply, do_acquire(A, Pbc), State};
handle_call({release, A, B}, _From, State = #state{pbc = Pbc}) ->
    {reply, do_release(A, B, Pbc), State};
handle_call(cleanup, _From, State = #state{pbc = Pbc}) ->
    {reply, do_cleanup(Pbc), State};
handle_call(_Msg, _From, State) ->
    ?LOG_WARNING("Unhandled call ~p from ~p", [_Msg, _From]),
    {reply, ok, State}.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(_Msg, State) ->
    ?LOG_WARNING("Unhandled cast ~p", [_Msg]),
    {noreply, State}.

-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(term(), #state{}) -> ok.
terminate(_Reason, #state{pbc = Pbc}) ->
    riak_cs_utils:close_riak_connection(Pbc),
    ok.

-spec code_change(term(), #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


do_acquire(A, Pbc) ->
    case riakc_pb_socket:get(Pbc, ?OBJECT_LOCK_BUCKET, A, ?CONSISTENT_READ_OPTIONS) of
        {ok, _x} ->
            busy;
        {error, notfound} ->
            Precious = term_to_binary(make_ref()),
            ok = riakc_pb_socket:put(
                   Pbc, riakc_obj:new(?OBJECT_LOCK_BUCKET, A, Precious),
                   ?CONSISTENT_WRITE_OPTIONS),
            _ = spawn(fun() ->
                              timer:sleep(?AUTO_RELEASE_MSEC),
                              do_release(A, Precious, Pbc)
                      end),
            Precious
    end.

do_release(A, PreciousOrig, Pbc) ->
    case riakc_pb_socket:get(Pbc, ?OBJECT_LOCK_BUCKET, A, ?CONSISTENT_READ_OPTIONS) of
        {ok, Obj} ->
            case riakc_obj:get_value(Obj) of
                PreciousOrig ->
                    ?LOG_DEBUG("lock ~p found, releasing it", [A]);
                _NotOurs ->
                    logger:error("found a lock on ~p overwritten by another process", [A])
            end,
            ok = riakc_pb_socket:delete(Pbc, ?OBJECT_LOCK_BUCKET, A, ?CONSISTENT_DELETE_OPTIONS);
        {error, notfound} ->
            ok
    end.

do_cleanup(Pbc) ->
    case riakc_pb_socket:list_keys(Pbc, ?OBJECT_LOCK_BUCKET) of
        {ok, []} ->
            ok;
        {ok, KK} ->
            logger:notice("found ~b stale locks; deleting them now", [length(KK)]),
            delete_all(Pbc, KK)
    end.

delete_all(_, []) ->
    ok;
delete_all(Pbc, [A | AA]) ->
    ok = riakc_pb_socket:delete(Pbc, ?OBJECT_LOCK_BUCKET, A, ?CONSISTENT_DELETE_OPTIONS),
    delete_all(Pbc, AA).
