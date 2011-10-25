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

-record(state, {riakc_pid}).

%% ====================================================================
%% Public API
%% ====================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    case riak_moss:riak_client() of
        {ok, Pid} ->
            {ok, #state{riakc_pid=Pid}};
        {error, Reason} ->
            %% TODO:
            %% I think this actually only gets called
            %% because of a race. This process is going
            %% to be killed because riak_moss:riak_client()
            %% calls riakc_pb_socket:start_link(). However
            %% lager will print a convenient error anyway
            %% if this lager message loses the race.
            lager:error("Couldn't connect to Riak: ~p", [Reason])
    end.

get_user(KeyID) ->
    gen_server:call(?MODULE, {get_user, KeyID}).

create_user(UserName) ->
    gen_server:call(?MODULE, {create_user, UserName}).

get_buckets(#moss_user{buckets=Buckets}) ->
    Buckets.

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
delete_bucket(KeyID, BucketName) ->
    gen_server:call(?MODULE, {delete_bucket, KeyID, BucketName}).

list_keys(BucketName) ->
    gen_server:call(?MODULE, {list_keys, BucketName}).

get_object(BucketName, Key) ->
    gen_server:call(?MODULE, {get_object, BucketName, Key}).

%% TODO:
%% What are we actually doing with
%% the KeyID?
put_object(KeyID, BucketName, Key, Val, Metadata) ->
    gen_server:call(?MODULE, {put_object, KeyID, BucketName, Key, Val, Metadata}).

delete_object(BucketName, Key) ->
    gen_server:call(?MODULE, {delete_object, BucketName, Key}).

handle_call({get_user, KeyID}, _From, State=#state{riakc_pid=RiakcPid}) ->
    {reply, do_get_user(KeyID, RiakcPid), State};

handle_call({create_user, UserName}, _From, State=#state{riakc_pid=RiakcPid}) ->
    {reply, do_create_user(UserName, RiakcPid), State};

handle_call({create_bucket, KeyID, BucketName}, _From, State=#state{riakc_pid=RiakcPid}) ->
    {reply, do_create_bucket(KeyID, BucketName, RiakcPid), State};

handle_call({delete_bucket, KeyID, BucketName}, _From, State=#state{riakc_pid=RiakcPid}) ->
    {reply, do_delete_bucket(KeyID, BucketName, RiakcPid), State};

handle_call({list_keys, BucketName}, _From, State=#state{riakc_pid=RiakcPid}) ->
    {reply, do_list_keys(BucketName, RiakcPid), State};

handle_call({get_object, BucketName, Key}, _From, State=#state{riakc_pid=RiakcPid}) ->
    {reply, do_get_object(BucketName, Key, RiakcPid), State};

handle_call({put_object, KeyID, BucketName, Key, Value, Metadata},
                   _From, State=#state{riakc_pid=RiakcPid}) ->
    {reply, do_put_object(KeyID, BucketName, Key, Value, Metadata, RiakcPid), State};

handle_call({delete_object, BucketName, Key},
                   _From, State=#state{riakc_pid=RiakcPid}) ->
    {reply, do_delete_object(BucketName, Key, RiakcPid), State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{riakc_pid=RiakcPid}) ->
    riakc_pb_socket:stop(RiakcPid),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

do_get_user(KeyID, RiakcPid) ->
    case riakc_pb_socket:get(RiakcPid, ?USER_BUCKET, list_to_binary(KeyID)) of
        {ok, Obj} ->
            {ok, binary_to_term(riakc_obj:get_value(Obj))};
        Error ->
            Error
    end.

do_create_user(UserName, RiakcPid) ->
    %% TODO: Is it outside the scope
    %% of this module for this func
    %% to be making up the key/secret?
    KeyID = riak_moss:unique_hex_id(),
    Secret = riak_moss:unique_hex_id(),

    User = #moss_user{name=UserName, key_id=KeyID, key_secret=Secret},
    do_save_user(User, RiakcPid),
    {ok, User}.

do_save_user(User, RiakcPid) ->
    UserObj = riakc_obj:new(?USER_BUCKET, list_to_binary(User#moss_user.key_id), User),
    ok = riakc_pb_socket:put(RiakcPid, UserObj),
    ok.

%% TODO:
%% We need to be checking that
%% this bucket doesn't already
%% exist anywhere, since everyone
%% shares a global bucket namespace
do_create_bucket(KeyID, BucketName, RiakcPid) ->
    Bucket = #moss_bucket{name=BucketName, creation_date=httpd_util:rfc1123_date()},
    %% TODO:
    %% We don't do anything about
    %% {error, Reason} here
    {ok, User} = do_get_user(KeyID, RiakcPid),
    OldBuckets = User#moss_user.buckets,
    case [B || B <- OldBuckets, B#moss_bucket.name =:= BucketName] of
        [] ->
            NewUser = User#moss_user{buckets=[Bucket|OldBuckets]},
            do_save_user(NewUser, RiakcPid);
        _ ->
            ignore
    end,
    %% TODO:
    %% Maybe this should return
    %% the updated list of buckets
    %% owned by the user?
    ok.

do_delete_bucket(KeyID, BucketName, RiakcPid) ->
    %% TODO:
    %% Right now we're just removing
    %% the bucket from the list of
    %% buckets owned by the user.
    %% What do we need to do
    %% to actually "delete"
    %% the bucket?
    {ok, User} = do_get_user(KeyID, RiakcPid),
    CurrentBuckets = User#moss_user.buckets,

    %% TODO:
    %% This logic is pure and should
    %% be separated out into it's
    %% own func so it can be easily
    %% unit tested.
    FilterFun = fun(Element) -> Element#moss_bucket.name =/= BucketName end,
    UpdatedBuckets = lists:filter(FilterFun, CurrentBuckets),
    UpdatedUser = User#moss_user{buckets=UpdatedBuckets},
    do_save_user(UpdatedUser, RiakcPid).

do_list_keys(BucketName, RiakcPid) ->
    {ok, Keys} = riakc_pb_socket:list_keys(RiakcPid, BucketName),
    %% TODO:
    %% This is a naive implementation,
    %% the longer-term solution is likely
    %% going to involve 2i and merging the
    %% results from each of the vnodes.
    {ok, lists:sort(Keys)}.

do_get_object(BucketName, Key, RiakcPid) ->
    %% TODO:
    %% Should we be converting the
    %% key to binary here, or in the
    %% the public api method?
    BinKey = list_to_binary(Key),
    riakc_pb_socket:get(RiakcPid, BucketName, BinKey).

do_put_object(_KeyID, BucketName, Key, Value, Metadata, RiakcPid) ->
    %% TODO: KeyID is currently
    %% not used

    %% TODO:
    %% Should we be converting the
    %% key to binary here, or in the
    %% the public api method?
    BinKey = list_to_binary(Key),
    RiakObject = riakc_obj:new(BucketName, BinKey, Value),
    NewObj = riakc_obj:update_metadata(RiakObject, Metadata),
    riakc_pb_socket:put(RiakcPid, NewObj).

do_delete_object(BucketName, Key, RiakcPid) ->
    BinKey = list_to_binary(Key),
    riakc_pb_socket:delete(RiakcPid, BucketName, BinKey).
