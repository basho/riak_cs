%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved,
%%               2021 TI Tokyo    All Rights Reserved.
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

%% @doc Module to return manifests for cs_bucket_fold requests

%% Fake up some `handle_call' of `riak_cs_riak_client' and `riakc_pb_socket'
%% both in this single module (a bit of abuse, sorry).

-module(riak_cs_dummy_riak_client_list_objects_v2).

-behaviour(gen_server).

-include_lib("riak_pb/include/riak_pb.hrl").
-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include("riak_cs.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.

%% API
-export([start_link/1, stop/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
          manifests :: [term()],
          reqid :: term(),
          caller :: pid(),
          max_results :: integer(),
          replied_at_least_once=false :: boolean()
         }).

-type state() :: #state{}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Start a dummy `riakc_pb_socket'.
-spec start_link(list()) -> {ok, pid()} | {error, term()}.
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%% @doc Stop the process
-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:call(Pid, stop).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

%% @doc Initialize the server.
-spec init([term()]) -> {ok, state()} | {stop, term()}.
init([Manifests]) ->
    {ok, #state{manifests=Manifests}}.

%% @doc Unused
-spec handle_call(term(), {pid(), term()}, state()) ->
                         {reply, ok, state()}.
handle_call(manifest_pbc, _From, State) ->
    {reply, {ok, self()}, State};
handle_call({req, #rpbcsbucketreq{start_key = <<0>>} = _Req,
             _Timeout, _Ctx}, _From,
            #state{replied_at_least_once=true}=State) ->
    %% Already replied first set of manifests but requested again from the beginning
    %% Forbid repeated call so that EQC does not fall into infinite loop.
    {reply, {error, second_list_request}, State};
handle_call({req, #rpbcsbucketreq{max_results=MaxResults,
                                  start_key=StartKey} = _Req,
             _Timeout, {ReqId, Caller}=_Ctx}, _From,
            State) ->
    %% io:format("================= handle_call (req)~n"),
    %% io:format("_Caller: ~p~n", [Caller]),
    %% io:format("_Ctx: ~p~n", [_Ctx]),
    %% io:format("MaxResults: ~p~n", [MaxResults]),
    gen_server:cast(self(), {send_manifests, StartKey}),
    {reply, {ok, ReqId}, State#state{reqid=ReqId, caller=Caller,
                                     max_results=MaxResults,
                                     replied_at_least_once=true}};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Msg, _From, State) ->
    io:format("=============== Received unknown call message: ~p~n", [_Msg]),
    {reply, ok, State}.

handle_cast({send_manifests, StartKey},
            #state{manifests=Manifests, reqid=ReqId, caller=Caller,
                   max_results=MaxResults} = State) ->
    %% io:format("================ handle_cast (send_manifests)~n"),
    _Rest = send_manifests_and_done(Caller, ReqId, StartKey, MaxResults, Manifests),
    {noreply, State};
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

send_manifests_and_done(Caller, ReqId, StartKey, MaxResults, Manifests) ->
    %% io:format("{Caller, ReqId, StartKey, MaxResults}: ~p~n",
    %%           [{Caller, ReqId, StartKey, MaxResults}]),
    {Head, Rest} = take_n(StartKey, MaxResults, Manifests),
    RiakcObjs = manifests_to_robjs(Head),
    Msg = {ok, RiakcObjs},
    Caller ! {ReqId, Msg},
    DoneMsg = {done, continuation_ignored},
    Caller ! {ReqId, DoneMsg},
    Rest.

take_n(StartKey, N, Manifests) ->
    Remaining = lists:dropwhile(fun(?MANIFEST{bkey={_,Key}}) ->
                                        Key =< StartKey
                                end, Manifests),
    take_n(N, Remaining).

take_n(N, Manifests) ->
    try lists:split(N, Manifests) of
        {Head, Rest} ->
            {Head, Rest}
    catch error:badarg ->
            {Manifests, []}
    end.

manifests_to_robjs(Manifests) ->
    [manifest_to_robj(M) || M <- Manifests].

%% TODO: Metadatas
manifest_to_robj(?MANIFEST{bkey={Bucket, Key}, uuid=UUID}=M) ->
    Dict = rcs_common_manifest_utils:new_dict(UUID, M),
    ManifestBucket = riak_cs_utils:to_bucket_name(objects, Bucket),
    riakc_obj:new(ManifestBucket, Key, riak_cs_utils:encode_term(Dict)).
