%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module to expose riak_moss operations.

-module(riak_moss_client, [Pid, User]).

-export([make_bucket/1,
         stop/0,
         get_object/2,
         put_object/3,
         delete_object/2,
         list_bucket/1]).

-include("riak_moss.hrl").

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Compose a moss bucket name using the user's key id
%% and the specified bucket name.
%% @spec make_bucket(binary()) -> binary().
make_bucket(Bucket) ->
    iolist_to_binary([User#rs3_user.key_id, ".", Bucket]).

%% @doc Terminate the moss client reference.
%% @spec stop() -> ok.
stop() ->
    riakc_pb_socket:stop(Pid).

%% @doc Fetch the object for the given bucket and key
%% @spec get_object(binary(), binary()) -> {ok, tuple()} | {error, term()}.
get_object(Bucket, Key) ->
    riakc_pb_socket:get(Pid, make_bucket(Bucket), Key).

%% @doc Store the specified key and value in the given bucket
%% @spec put_object(binary(), binary(), term()) ->
%%                        ok |
%%                        {ok, tuple()} |
%%                        {ok, binary()} |
%%                        {error, term()}.
put_object(Bucket, Key, Value) ->
    Obj = riakc_obj:new(make_bucket(Bucket), Key, Value),
    riakc_pb_socket:put(Pid, Obj).

%% @doc Delete the object stored for the given key
%% from the specified bucket
%%
%% @spec delete_object(binary(), binary()) -> ok | {error, term()}.
delete_object(Bucket, Key) ->
    riakc_pb_socket:delete(Pid, make_bucket(Bucket), Key).

%% @doc List the keys in the specified bucket
%% @spec list_bucket(binary()) -> [{binary(), integer()}].
list_bucket(Bucket) ->
    {ok, KL} = riakc_pb_socket:list_keys(Pid, make_bucket(Bucket)),
    add_sizes(KL, Bucket, []).

%% ===================================================================
%% Internal functions
%% ===================================================================

add_sizes([], _Bucket, Acc) ->
    lists:reverse(Acc);
add_sizes([H|T], Bucket, Acc) ->
    case get_object(Bucket, H) of
        {ok, O} ->
            add_sizes(T, Bucket, [{H, size(riakc_obj:get_value(O))}|Acc]);
        _ ->
            add_sizes(T, Bucket, [{H, 0}|Acc])
    end.

