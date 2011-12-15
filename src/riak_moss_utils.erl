%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc riak_moss utility functions

-module(riak_moss_utils).

%% Public API
-export([binary_to_hexlist/1,
         close_riak_connection/1,
         create_bucket/2,
         create_user/1,
         delete_bucket/2,
         delete_object/2,
         from_bucket_name/1,
         get_buckets/1,
         get_object/2,
         get_object/3,
         get_user/1,
         list_keys/1,
         pow/2,
         pow/3,
         put_object/4,
         riak_connection/0,
         riak_connection/2,
         to_bucket_name/2,
         unique_hex_id/0]).

-include("riak_moss.hrl").

-define(OBJECT_BUCKET_PREFIX, <<"objects:">>).
-define(BLOCK_BUCKET_PREFIX, <<"blocks:">>).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Convert the passed binary into a string where the numbers are represented in hexadecimal (lowercase and 0 prefilled).
-spec binary_to_hexlist(binary()) -> string().
binary_to_hexlist(Bin) ->
    XBin =
        [ begin
              Hex = erlang:integer_to_list(X, 16),
              if
                  X < 16 ->
                      lists:flatten(["0" | Hex]);
                  true ->                      Hex
              end
          end || X <- binary_to_list(Bin)],
    string:to_lower(lists:flatten(XBin)).

%% @doc Close a protobufs connection to the riak cluster.
-spec close_riak_connection(pid()) -> ok.
close_riak_connection(Pid) ->
    riakc_pb_socket:stop(Pid).

%% @doc Create a bucket in the global namespace or return
%% an error if it already exists.
%% TODO:
%% We need to be checking that
%% this bucket doesn't already
%% exist anywhere, since everyone
%% shares a global bucket namespace
-spec create_bucket(string(), binary()) -> ok.
create_bucket(KeyID, BucketName) ->
    Bucket = #moss_bucket{name=BucketName,
                          creation_date=riak_moss_wm_utils:iso_8601_datetime()},
    case riak_connection() of
        {ok, RiakPid} ->
            %% TODO:
            %% We don't do anything about
            %% {error, Reason} here
            {ok, User} = get_user(KeyID, RiakPid),
            OldBuckets = User#moss_user.buckets,
            case [B || B <- OldBuckets, B#moss_bucket.name =:= BucketName] of
                [] ->
                    NewUser = User#moss_user{buckets=[Bucket|OldBuckets]},
                    save_user(NewUser, RiakPid);
                _ ->
                    ignore
            end,
            close_riak_connection(RiakPid),
            %% TODO:
            %% Maybe this should return
            %% the updated list of buckets
            %% owned by the user?
            ok;
        {error, Reason} ->
            {error, {riak_connect_failed, Reason}}
    end.

%% @doc Create a new MOSS user
-spec create_user(string()) -> {ok, moss_user()}.
create_user(UserName) ->
    case riak_connection() of
        {ok, RiakPid} ->
            %% TODO: Is it outside the scope
            %% of this module for this func
            %% to be making up the key/secret?
            KeyID = unique_hex_id(),
            Secret = unique_hex_id(),

            User = #moss_user{name=UserName, key_id=KeyID, key_secret=Secret},
            save_user(User, RiakPid),
            close_riak_connection(RiakPid),
            {ok, User};
        {error, Reason} ->
            {error, {riak_connect_failed, Reason}}
    end.

%% @doc Delete a bucket
-spec delete_bucket(string(), binary()) -> ok.
delete_bucket(KeyID, BucketName) ->
    case riak_connection() of
        {ok, RiakPid} ->
            %% TODO:
            %% Right now we're just removing
            %% the bucket from the list of
            %% buckets owned by the user.
            %% What do we need to do
            %% to actually "delete"
            %% the bucket?
            {ok, User} = get_user(KeyID, RiakPid),
            CurrentBuckets = User#moss_user.buckets,

            %% TODO:
            %% This logic is pure and should
            %% be separated out into it's
            %% own func so it can be easily
            %% unit tested.
            FilterFun =
                fun(Element) ->
                        Element#moss_bucket.name =/= BucketName
                end,
            UpdatedBuckets = lists:filter(FilterFun, CurrentBuckets),
            UpdatedUser = User#moss_user{buckets=UpdatedBuckets},
            Res = save_user(UpdatedUser, RiakPid),
            close_riak_connection(RiakPid),
            Res;
        {error, Reason} ->
            {error, {riak_connect_failed, Reason}}
    end.

%% @doc Delete an object from Riak
-spec delete_object(binary(), binary()) -> ok.
delete_object(BucketName, Key) ->
    case riak_connection() of
        {ok, RiakPid} ->
            Res = riakc_pb_socket:delete(RiakPid, BucketName, Key),
            close_riak_connection(RiakPid),
            Res;
        {error, Reason} ->
            {error, {riak_connect_failed, Reason}}
    end.

%% Get the root bucket name for either a MOSS object
%% bucket or the data block bucket name.
-spec from_bucket_name(binary()) -> binary().
from_bucket_name(BucketNameWithPrefix) ->
    BlocksName = ?BLOCK_BUCKET_PREFIX,
    ObjectsName = ?OBJECT_BUCKET_PREFIX,
    BlockByteSize = byte_size(BlocksName),
    ObjectsByteSize = byte_size(ObjectsName),

    case BucketNameWithPrefix of
        <<BlocksName:BlockByteSize/binary, BucketName/binary>> ->
            {blocks, BucketName};
        <<ObjectsName:ObjectsByteSize/binary, BucketName/binary>> ->
            {objects, BucketName}
    end.

%% @doc Return a user's buckets.
-spec get_buckets(moss_user()) -> [binary()].
get_buckets(#moss_user{buckets=Buckets}) ->
    Buckets.

%% @doc Get an object from Riak
-spec get_object(binary(), binary()) ->
                        {ok, binary()} | {error, term()}.
get_object(BucketName, Key) ->
    case riak_connection() of
        {ok, RiakPid} ->
            Res = get_object(BucketName, Key, RiakPid),
            close_riak_connection(RiakPid),
            Res;
        {error, Reason} ->
            {error, {riak_connect_failed, Reason}}
    end.

%% @doc Get an object from Riak
-spec get_object(binary(), binary(), pid()) ->
                        {ok, binary()} | {error, term()}.
get_object(BucketName, Key, RiakPid) ->
    riakc_pb_socket:get(RiakPid, BucketName, Key).

%% @doc Retrieve a MOSS user's information based on their id string.
-spec get_user(string()) -> {ok, term()} | {error, term()}.
get_user(KeyID) ->
    case riak_connection() of
        {ok, RiakPid} ->
            Res = get_user(KeyID, RiakPid),
            close_riak_connection(RiakPid),
            Res;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc List the keys from a bucket
-spec list_keys(binary()) -> {ok, [binary()]}.
list_keys(BucketName) ->
    case riak_connection() of
        {ok, RiakPid} ->
            {ok, Keys} = riakc_pb_socket:list_keys(RiakPid, BucketName),
            close_riak_connection(RiakPid),
            %% TODO:
            %% This is a naive implementation,
            %% the longer-term solution is likely
            %% going to involve 2i and merging the
            %% results from each of the vnodes.
            {ok, lists:sort(Keys)};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Integer version of the standard pow() function; call the recursive accumulator to calculate.
-spec pow(integer(), integer()) -> integer().
pow(Base, Power) ->
    pow(Base, Power, 1).

%% @doc Integer version of the standard pow() function.
-spec pow(integer(), integer(), integer()) -> integer().
pow(Base, Power, Acc) ->
    case Power of
        0 ->
            Acc;
        _ ->
            pow(Base, Power - 1, Acc * Base)
    end.

%% @doc Store an object in Riak
-spec put_object(binary(), binary(), binary(), [term()]) -> ok.
put_object(BucketName, Key, Value, Metadata) ->
    case riak_connection() of
        {ok, RiakPid} ->
            RiakObject = riakc_obj:new(BucketName, Key, Value),
            NewObj = riakc_obj:update_metadata(RiakObject, Metadata),
            Res = riakc_pb_socket:put(RiakPid, NewObj),
            close_riak_connection(RiakPid),
            Res;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Get a protobufs connection to the riak cluster
%% using information from the application environment.
-spec riak_connection() -> {ok, pid()} | {error, term()}.
riak_connection() ->
    case application:get_env(riak_moss, riak_ip) of
        {ok, Host} ->
            ok;
        undefined ->
            Host = "127.0.0.1"
    end,
    case application:get_env(riak_moss, riak_pb_port) of
        {ok, Port} ->
            ok;
        undefined ->
            Port = 8087
    end,
    riak_connection(Host, Port).

%% @doc Get a protobufs connection to the riak cluster.
-spec riak_connection(string(), pos_integer()) -> {ok, pid()} | {error, term()}.
riak_connection(Host, Port) ->
    riakc_pb_socket:start_link(Host, Port).

%% Get the proper bucket name for either the MOSS object
%% bucket or the data block bucket.
-spec to_bucket_name([objects | blocks], binary()) -> binary().
to_bucket_name(objects, Name) ->
    <<?OBJECT_BUCKET_PREFIX/binary, Name/binary>>;
to_bucket_name(blocks, Name) ->
    <<?BLOCK_BUCKET_PREFIX/binary, Name/binary>>.

%% @doc Create a random identifying integer, returning its string
%%      representation in base 62.
-spec unique_hex_id() -> string().
unique_hex_id() ->
    Rand = crypto:sha(term_to_binary({make_ref(), now()})),
    <<I:160/integer>> = Rand,
    integer_to_list(I, 16).

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @doc Retrieve a MOSS user's information based on their id string.
-spec get_user(string(), pid()) -> {ok, term()} | {error, term()}.
get_user(KeyID, RiakPid) ->
    case riakc_pb_socket:get(RiakPid, ?USER_BUCKET, list_to_binary(KeyID)) of
        {ok, Obj} ->
            {ok, binary_to_term(riakc_obj:get_value(Obj))};
        Error ->
            Error
    end.

%% @doc Save information about a MOSS user
-spec save_user(moss_user(), pid()) -> ok.
save_user(User, RiakPid) ->
    UserObj = riakc_obj:new(?USER_BUCKET, list_to_binary(User#moss_user.key_id), User),
    %% @TODO Error handling
    ok = riakc_pb_socket:put(RiakPid, UserObj),
    ok.
