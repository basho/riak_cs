%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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

-record(state, {}).


start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    {ok, #state{}}.

get_user(KeyID) ->
    gen_server:call(?MODULE, {get_user, KeyID}).

create_user(UserName) ->
    gen_server:call(?MODULE, {create_user, UserName}).

get_buckets(KeyID) ->
    gen_server:call(?MODULE, {get_buckets, KeyID}).

create_bucket(KeyID, BucketName) ->
    gen_server:call(?MODULE, {create_bucket, KeyID, BucketName}).

delete_bucket(KeyID, BucketName) ->
    gen_server:call(?MODULE, {delete_bucket, KeyID, BucketName}).

put_object(KeyId, Bucket, Key, Val, Metadata) ->
    gen_server:call(?MODULE, {put_object, KeyId, Bucket, Key, Val, Metadata}).

do_create_user(UserName) ->
    {ok, Pid} = riak_moss:riak_client(),
    {ok, KL} = riakc_pb_socket:list_keys(Pid, ?USER_BUCKET),
    KeyId = integer_to_list(length(KL)),
    KeyData = riak_moss:make_key(),
    User = #rs3_user{name=UserName, key_id=KeyId, key_data=KeyData},
    UserObj = riakc_obj:new(?USER_BUCKET, list_to_binary(KeyId), User),
    ok = riakc_pb_socket:put(Pid, UserObj),
    riakc_pb_socket:stop(Pid),
    User.

do_save_user(User=#rs3_user{key_id=KeyId}) when KeyId /= undefined  ->
    {ok, Pid} = riak_moss:riak_client(),
    UserObj = riakc_obj:new(?USER_BUCKET,list_to_binary(KeyId),User),
    ok = riakc_pb_socket:put(Pid, UserObj),
    riakc_pb_socket:stop(Pid),
    ok.

do_get_user(KeyId) ->
    {ok, Pid} = riak_moss:riak_client(),
    case riakc_pb_socket:get(Pid, ?USER_BUCKET, list_to_binary(KeyId)) of
        {ok, Obj} ->
            riakc_pb_socket:stop(Pid),
            {ok, binary_to_term(riakc_obj:get_value(Obj))};
        Error ->
            riakc_pb_socket:stop(Pid),
            Error
    end.

handle_call({get_user, KeyId}, _From, State) ->
    {reply, do_get_user(KeyId), State};
handle_call({create_user, UserName}, _From, State) ->
    {reply, {ok, do_create_user(UserName)}, State};
handle_call({get_buckets, _KeyId}, _From, State) ->
    {reply, ok, State};
handle_call({create_bucket, KeyId, Name}, _From, State) ->
    Bucket = #rs3_bucket{name=Name, creation_date=httpd_util:rfc1123_date()},
    {ok, User} = do_get_user(KeyId),
    OldBuckets = User#rs3_user.buckets,
    case [B || B <- OldBuckets, B#rs3_bucket.name =:= Name] of
        [] ->
            NewUser = User#rs3_user{buckets=[Bucket|OldBuckets]},
            do_save_user(NewUser);
        _ ->
            ignore
    end,
    {reply, ok, State};
handle_call({delete_bucket, _KeyId, _Name}, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

