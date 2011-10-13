%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_moss_riakc).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("riak_moss.hrl").

-compile(export_all).

-define(SERVER, ?MODULE).

-record(state, {riakc_pid}).


start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    {ok, Pid} = riak_moss:riak_client(),
    {ok, #state{riakc_pid=Pid}}.

get_user(KeyID) ->
    gen_server:call(?MODULE, {get_user, KeyID}).

create_user(UserName) ->
    gen_server:call(?MODULE, {create_user, UserName}).

get_buckets(KeyID) ->
    gen_server:call(?MODULE, {get_buckets, KeyID}).

%% TODO:
%% If there is only one namespace
%% for buckets then do we need
%% the KeyID here?
create_bucket(KeyID, BucketName) ->
    gen_server:call(?MODULE, {create_bucket, KeyID, BucketName}).

%% TODO:
%% If there is only one namespace
%% for buckets then do we need
%% the KeyID here?
%% TODO:
%% missing do_create_bucket
delete_bucket(KeyID, BucketName) ->
    gen_server:call(?MODULE, {delete_bucket, KeyID, BucketName}).

%% TODO:
%% What are we actually doing with
%% the KeyID?
%% TODO:
%% missing do_create_bucket
%% and handle_call pattern
put_object(KeyID, Bucket, Key, Val, Metadata) ->
    gen_server:call(?MODULE, {put_object, KeyID, Bucket, Key, Val, Metadata}).

do_create_user(UserName, RiakcPid) ->
    %% TODO: Is it outside the scope
    %% of this module for this func
    %% to be making up the key/secret?
    KeyID = riak_moss:unique_id_62(),
    Secret = riak_moss:unique_id_62(),

    User = #rs3_user{name=UserName, key_id=KeyID, key_secret=Secret},
    do_save_user(User, RiakcPid),
    User.

do_get_buckets(KeyID, RiakcPid) ->
    User = do_get_user(KeyID, RiakcPid),
    User#rs3_user.buckets.

%% TODO:
%% We need to be checking that
%% this bucket doesn't already
%% exist anywhere, since everyone
%% shares a global bucket namespace
do_create_bucket(KeyID, BucketName, RiakcPid) ->
    Bucket = #rs3_bucket{name=BucketName, creation_date=httpd_util:rfc1123_date()},
    {ok, User} = do_get_user(KeyID, RiakcPid),
    OldBuckets = User#rs3_user.buckets,
    case [B || B <- OldBuckets, B#rs3_bucket.name =:= BucketName] of
        [] ->
            NewUser = User#rs3_user{buckets=[Bucket|OldBuckets]},
            do_save_user(NewUser, RiakcPid);
        _ ->
            ignore
    end,
    %% TODO:
    %% Maybe this should return
    %% the updated list of buckets
    %% owned by the user?
    ok.

do_save_user(User, RiakcPid) ->
    UserObj = riakc_obj:new(?USER_BUCKET, list_to_binary(User#rs3_user.key_id),User),
    ok = riakc_pb_socket:put(RiakcPid, UserObj),
    ok.

do_get_user(KeyID, RiakcPid) ->
    case riakc_pb_socket:get(RiakcPid, ?USER_BUCKET, list_to_binary(KeyID)) of
        {ok, Obj} ->
            {ok, binary_to_term(riakc_obj:get_value(Obj))};
        Error ->
            Error
    end.

handle_call({get_user, KeyID}, _From, State=#state{riakc_pid=RiakcPid}) ->
    {reply, do_get_user(KeyID, RiakcPid), State};
handle_call({create_user, UserName}, _From, State=#state{riakc_pid=RiakcPid}) ->
    {reply, do_create_user(UserName, RiakcPid), State};
handle_call({get_buckets, KeyID}, _From, State=#state{riakc_pid=RiakcPid}) ->
    {reply, do_get_buckets(KeyID, RiakcPid), State};
%% TODO:
%% move this into do_create_bucket
handle_call({create_bucket, KeyID, BucketName}, _From, State=#state{riakc_pid=RiakcPid}) ->
    {reply, do_create_bucket(KeyID, BucketName, RiakcPid), State};
handle_call({delete_bucket, _KeyID, _Name}, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{riakc_pid=RiakcPid}) ->
    riakc_pb_socket:stop(RiakcPid),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

