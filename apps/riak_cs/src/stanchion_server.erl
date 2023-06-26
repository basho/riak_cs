%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%               2021, 2022 TI Tokyo    All Rights Reserved.
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

%% @doc Module to process bucket creation requests.

-module(stanchion_server).

-behaviour(gen_server).

%% API
-export([start_link/0,
         create_bucket/1,
         create_user/1,
         delete_user/1,
         delete_bucket/2,
         set_bucket_acl/2,
         set_bucket_policy/2,
         set_bucket_versioning/2,
         delete_bucket_policy/2,
         stop/1,
         update_user/1,
         create_role/1,
         update_role/1,
         delete_role/1,
         create_policy/1,
         update_policy/1,
         delete_policy/1,
         create_saml_provider/1,
         delete_saml_provider/1,
         msgq_len/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("moss.hrl").
-include("stanchion.hrl").
-include_lib("kernel/include/logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Test API
-export([test_link/0]).

-endif.

-record(state, {pbc :: pid()}).
-type state() :: #state{}.

%% This ?TURNAROUND_TIME has another ?TURNAROUND_TIME at gen_server
%% process, to measure both waiting time and service time.
-define(MEASURE(Name, Call),
        begin
            {{Result_____, ServiceTime____},
             TATus_____} = ?TURNAROUND_TIME(Call),
            WaitingTime____ = TATus_____ - ServiceTime____,
            ?LOG_DEBUG("~p ~p ~p", [Name, Result_____, ServiceTime____]),
            stanchion_stats:update(Name, ServiceTime____, WaitingTime____),
            Result_____
        end).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Start a `stanchion_server'.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Attempt to create a bucket
-spec create_bucket([{term(), term()}]) -> ok | {error, term()}.
create_bucket(BucketData) ->
    ?MEASURE([bucket, create],
             gen_server:call(?MODULE,
                             {create_bucket, BucketData},
                             infinity)).

%% @doc Attempt to create a bucket
-spec create_user([{term(), term()}]) ->
          ok | {error, term() | stanchion_utils:riak_connect_failed()}.
create_user(UserData) ->
    ?MEASURE([user, create],
             gen_server:call(?MODULE,
                             {create_user, UserData},
                             infinity)).

-spec delete_user([{term(), term()}]) ->
          ok | {error, term() | stanchion_utils:riak_connect_failed()}.
delete_user(A) ->
    ?MEASURE([user, delete],
             gen_server:call(?MODULE,
                             {delete_user, A},
                             infinity)).

%% @doc Attempt to delete a bucket
-spec delete_bucket(binary(), binary()) -> ok | {error, term()}.
delete_bucket(Bucket, UserId) ->
    ?MEASURE([bucket, delete],
             gen_server:call(?MODULE,
                             {delete_bucket, Bucket, UserId},
                             infinity)).

%% @doc Set the ACL for a bucket
-spec set_bucket_acl(binary(), term()) -> ok | {error, term()}.
set_bucket_acl(Bucket, FieldList) ->
    ?MEASURE([bucket, put_acl],
             gen_server:call(?MODULE,
                             {set_acl, Bucket, FieldList},
                             infinity)).

%% @doc Set the policy for a bucket
-spec set_bucket_policy(binary(), term()) -> ok | {error, term()}.
set_bucket_policy(Bucket, FieldList) ->
    ?MEASURE([bucket, set_policy],
             gen_server:call(?MODULE,
                             {set_policy, Bucket, FieldList},
                             infinity)).

%% @doc Set the versioning option for a bucket
-spec set_bucket_versioning(binary(), term()) -> ok | {error, term()}.
set_bucket_versioning(Bucket, FieldList) ->
    ?MEASURE([bucket, set_versioning],
             gen_server:call(?MODULE,
                             {set_versioning, Bucket, FieldList},
                             infinity)).

%% @doc delete the policy for a bucket
-spec delete_bucket_policy(binary(), binary()) -> ok | {error, term()}.
delete_bucket_policy(Bucket, RequesterId) ->
    ?MEASURE([bucket, delete_policy],
             gen_server:call(?MODULE,
                             {delete_policy, Bucket, RequesterId},
                             infinity)).

-spec create_role(maps:map()) -> {ok, string()} | {error, term()}.
create_role(A) ->
    ?MEASURE([role, create],
             gen_server:call(?MODULE,
                             {create_role, A},
                             infinity)).

-spec update_role(maps:map()) -> ok | {error, term()}.
update_role(A) ->
    ?MEASURE([role, update],
             gen_server:call(?MODULE,
                             {update_role, A},
                             infinity)).

-spec delete_role(string()) -> ok | {error, term()}.
delete_role(A) ->
    ?MEASURE([role, delete],
             gen_server:call(?MODULE,
                             {delete_role, A},
                             infinity)).

-spec create_policy(maps:map()) -> {ok, string()} | {error, term()}.
create_policy(A) ->
    ?MEASURE([policy, create],
             gen_server:call(?MODULE,
                             {create_policy, A},
                             infinity)).

-spec update_policy(maps:map()) -> ok | {error, term()}.
update_policy(A) ->
    ?MEASURE([policy, update],
             gen_server:call(?MODULE,
                             {update_policy, A},
                             infinity)).

-spec delete_policy(string()) -> ok | {error, term()}.
delete_policy(A) ->
    ?MEASURE([policy, delete],
             gen_server:call(?MODULE,
                             {delete_policy, A},
                             infinity)).

-spec create_saml_provider(maps:map()) -> ok | {error, term()}.
create_saml_provider(A) ->
    ?MEASURE([saml_provider, create],
             gen_server:call(?MODULE,
                             {create_saml_provider, A},
                             infinity)).

-spec delete_saml_provider(string()) -> ok | {error, term()}.
delete_saml_provider(A) ->
    ?MEASURE([saml_provider, delete],
             gen_server:call(?MODULE,
                             {delete_saml_provider, A},
                             infinity)).

stop(Pid) ->
    gen_server:cast(Pid, stop).

-spec update_user(rcs_user()) -> ok | {error, term() | stanchion_utils:riak_connect_failed()}.
update_user(A) ->
    ?MEASURE([user, update],
             gen_server:call(?MODULE,
                             {update_user, A},
                             infinity)).

-spec msgq_len() -> non_neg_integer().
msgq_len() ->
    Pid = whereis(?MODULE),
    {message_queue_len, Len} = erlang:process_info(Pid, message_queue_len),
    Len.

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

%% @doc Initialize the server.
-spec init([] | test) -> {ok, state()}.
init([]) ->
    {ok, Pbc} = riak_cs_utils:riak_connection(),
    {ok, #state{pbc = Pbc}};
init(test) ->
    {ok, #state{}}.

%% @doc Handle synchronous commands issued via exported functions.
-spec handle_call(term(), {pid(), term()}, state()) ->
                         {reply, ok, state()}.
handle_call({create_bucket, BucketData},
            _From,
            State=#state{pbc = Pbc}) ->
    Result = ?TURNAROUND_TIME(stanchion_utils:create_bucket(BucketData, Pbc)),
    {reply, Result, State};
handle_call({create_user, UserData},
            _From,
            State=#state{pbc = Pbc}) ->
    Result = ?TURNAROUND_TIME(stanchion_utils:create_user(UserData, Pbc)),
    {reply, Result, State};
handle_call({delete_user, UserData},
            _From,
            State=#state{pbc = Pbc}) ->
    Result = ?TURNAROUND_TIME(stanchion_utils:delete_user(UserData, Pbc)),
    {reply, Result, State};
handle_call({update_user, UserData},
            _From,
            State=#state{pbc = Pbc}) ->
    Result = ?TURNAROUND_TIME(stanchion_utils:update_user(UserData, Pbc)),
    {reply, Result, State};
handle_call({delete_bucket, Bucket, OwnerId},
            _From,
            State=#state{pbc = Pbc}) ->
    Result = ?TURNAROUND_TIME(stanchion_utils:delete_bucket(Bucket, OwnerId, Pbc)),
    {reply, Result, State};
handle_call({set_acl, Bucket, FieldList},
            _From,
            State=#state{pbc = Pbc}) ->
    Result = ?TURNAROUND_TIME(stanchion_utils:set_bucket_acl(Bucket, FieldList, Pbc)),
    {reply, Result, State};
handle_call({set_policy, Bucket, FieldList},
            _From,
            State=#state{pbc = Pbc}) ->
    Result = ?TURNAROUND_TIME(stanchion_utils:set_bucket_policy(Bucket, FieldList, Pbc)),
    {reply, Result, State};
handle_call({set_versioning, Bucket, FieldList},
            _From,
            State=#state{pbc = Pbc}) ->
    Result = ?TURNAROUND_TIME(stanchion_utils:set_bucket_versioning(Bucket, FieldList, Pbc)),
    {reply, Result, State};
handle_call({delete_policy, Bucket, RequesterId},
            _From,
            State=#state{pbc = Pbc}) ->
    Result = ?TURNAROUND_TIME(stanchion_utils:delete_bucket_policy(Bucket, RequesterId, Pbc)),
    {reply, Result, State};
handle_call({create_role, A},
            _From,
            State=#state{pbc = Pbc}) ->
    Result = ?TURNAROUND_TIME(stanchion_utils:create_role(A, Pbc)),
    {reply, Result, State};
handle_call({update_role, A},
            _From,
            State=#state{pbc = Pbc}) ->
    Result = ?TURNAROUND_TIME(stanchion_utils:updeate_role(A, Pbc)),
    {reply, Result, State};
handle_call({delete_role, A},
            _From,
            State=#state{pbc = Pbc}) ->
    Result = ?TURNAROUND_TIME(stanchion_utils:delete_role(A, Pbc)),
    {reply, Result, State};
handle_call({create_policy, A},
            _From,
            State=#state{pbc = Pbc}) ->
    Result = ?TURNAROUND_TIME(stanchion_utils:create_policy(A, Pbc)),
    {reply, Result, State};
handle_call({update_policy, A},
            _From,
            State=#state{pbc = Pbc}) ->
    Result = ?TURNAROUND_TIME(stanchion_utils:update_policy(A, Pbc)),
    {reply, Result, State};
handle_call({delete_policy, A},
            _From,
            State=#state{pbc = Pbc}) ->
    Result = ?TURNAROUND_TIME(stanchion_utils:delete_policy(A, Pbc)),
    {reply, Result, State};
handle_call({create_saml_provider, A},
            _From,
            State=#state{pbc = Pbc}) ->
    Result = ?TURNAROUND_TIME(stanchion_utils:create_saml_provider(A, Pbc)),
    {reply, Result, State};
handle_call({delete_saml_provider, A},
            _From,
            State=#state{pbc = Pbc}) ->
    Result = ?TURNAROUND_TIME(stanchion_utils:delete_saml_provider(A, Pbc)),
    {reply, Result, State};
handle_call(_Msg, _From, State) ->
    ?LOG_WARNING("Unhandled call ~p", [_Msg]),
    {reply, ok, State}.

%% @doc Handle asynchronous commands issued via
%% the exported functions.
-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(list_buckets, State) ->
    %% @TODO Handle bucket listing and reply
    {noreply, State};
handle_cast(stop, State = #state{pbc = Pbc}) ->
    riak_cs_utils:close_riak_connection(Pbc),
    {stop, normal, State#state{pbc = undefined}};
handle_cast(Event, State) ->
    logger:warning("Received unknown cast event: ~p", [Event]),
    {noreply, State}.

%% @doc @TODO
-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(_Info, State) ->
    {noreply, State}.

%% @doc Unused.
-spec terminate(term(), state()) -> ok.
terminate(_Reason, #state{pbc = Pbc}) ->
    riak_cs_utils:close_riak_connection(Pbc),
    ok.

%% @doc Unused.
-spec code_change(term(), state(), term()) ->
                         {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).

%% @doc Start a `stanchion_server' for testing.
-spec test_link() -> {ok, pid()} | {error, term()}.
test_link() ->
    gen_server:start_link(?MODULE, test, []).

-endif.
