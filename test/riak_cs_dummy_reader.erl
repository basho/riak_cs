%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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
%% ---------------------------------------------------------------------

%% @doc Module to write data to Riak.

-module(riak_cs_dummy_reader).

-behaviour(gen_server).

-include("riak_cs.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.

-ifdef(PULSE).
-include_lib("pulse/include/pulse.hrl").
-compile({parse_transform, pulse_instrument}).
-compile({pulse_replace_module,[{gen_server,pulse_gen_server}]}).
-endif.

%% API
-export([start_link/1,
         get_manifest/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {content_length :: integer(),
                remaining :: integer(),
                block_size :: integer(),
                bucket :: binary(),
                key :: binary(),
                caller_pid :: pid()}).

-type state() :: #state{}.


%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Start a `riak_cs_reader'.
-spec start_link(list()) -> {ok, pid()} | {error, term()}.
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

get_manifest(Pid) ->
    gen_server:call(Pid, get_manifest, infinity).

%% TODO:
%% Add shutdown public function

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

%% @doc Initialize the server.
-spec init([pid()] | {test, [pid()]}) -> {ok, state()} | {stop, term()}.
init([CallerPid, Bucket, Key, ContentLength, BlockSize]) ->
    %% Get a connection to riak
    random:seed(now()),
    {ok, #state{content_length=ContentLength,
                remaining=ContentLength,
                bucket=Bucket,
                key=Key,
                block_size=BlockSize,
                caller_pid=CallerPid}}.

%% @doc Unused
-spec handle_call(term(), {pid(), term()}, state()) ->
                         {reply, ok, state()}.
handle_call(get_manifest, _From, #state{bucket=Bucket,
                                               key=Key,
                                               content_length=ContentLength}=State) ->
    Manifest = riak_cs_lfs_utils:new_manifest(Bucket,
                                                Key,
                                                druuid:v4(),
                                                ContentLength,
                                                "application/test",
                                                <<"md5">>,
                                                orddict:new(),
                                                riak_cs_lfs_utils:block_size(),
                                                riak_cs_acl_utils:default_acl("tester", "id123", "keyid123")),
    {reply, {ok, Manifest}, State};
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast({get_block, _From, _Bucket, _Key, UUID, BlockNumber}, State) ->
    send_fake_data(UUID, BlockNumber, State);
handle_cast({get_block, _From, _Bucket, _Key, _ClusterID, UUID, BlockNumber}, State) ->
    send_fake_data(UUID, BlockNumber, State);
handle_cast(Event, State) ->
    lager:warning("Received unknown cast event: ~p", [Event]),
    {noreply, State}.

%% @doc @TODO
-spec handle_info(term(), state()) ->
                         {noreply, state()}.
handle_info(_Info, State) ->
    {noreply, State}.

%% @doc Unused.
-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) ->
    ok.

%% @doc Unused.
-spec code_change(term(), state(), term()) ->
                         {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

send_fake_data(UUID, BlockNumber, #state{caller_pid=CallerPid,remaining=Remaining}=State) ->
    Bytes = erlang:min(State#state.block_size, Remaining),
    FakeData = <<BlockNumber:(8*Bytes)/little-integer>>,
    riak_cs_get_fsm:chunk(CallerPid, {UUID, BlockNumber}, {ok, FakeData}),
    {noreply, State#state{remaining = Remaining - Bytes}}.
