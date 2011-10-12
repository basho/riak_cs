%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc riak_moss utility functions

-module(riak_moss).

%% Public API
-export([riak_client/0,
         moss_client/1,
         make_bucket/2,
         make_key/0]).

-include("riak_moss.hrl").

-type user() :: #rs3_user{}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Return a riak protocol buffers client reference
-spec riak_client() -> {ok, pid()}.
riak_client() ->
    riakc_pb_socket:start_link("127.0.0.1", 8087).

%% @doc Return a moss client reference for the specified user
-spec moss_client(user()) -> {ok, term()}.
moss_client(User) ->
    {ok, Pid} = riak_client(),
    {ok, riak_moss_client:new(Pid, User)}.

%% @doc Compose a moss bucket name using the key id
%% and the specified bucket name.
-spec make_bucket(binary(), binary()) -> binary().
make_bucket(KeyId, Bucket) ->
    iolist_to_binary([KeyId, ":", Bucket]).

%% @doc Generate a pseudo-random 64-byte key
-spec make_key() -> string().
make_key() ->
    KeySize = 64,
    {A, B, C} = erlang:now(),
    random:seed(A, B, C),
    Rand = random:uniform(pow(2, KeySize)),
    BKey = <<Rand:KeySize>>,
    binary_to_hexlist(BKey).

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @doc Integer version of the standard pow() function.
-spec pow(integer(), integer(), integer()) -> integer().
pow(Base, Power, Acc) ->
    case Power of
        0 ->
            Acc;
        _ ->
            pow(Base, Power - 1, Acc * Base)
    end.

%% @doc Integer version of the standard pow() function; call the recursive accumulator to calculate.
-spec pow(integer(), integer()) -> integer().
pow(Base, Power) ->
    pow(Base, Power, 1).

%% @doc Convert the passed binary into a string where the numbers are represented in hexadecimal (lowercase and 0 prefilled).
-spec binary_to_hexlist(binary()) -> string().
binary_to_hexlist(Bin) ->
    XBin =
        [ begin
              Hex = erlang:integer_to_list(X, 16),
              if
                  X < 16 ->
                      lists:flatten(["0" | Hex]);
                  true ->
                      Hex
              end
          end || X <- binary_to_list(Bin)],
    string:to_lower(lists:flatten(XBin)).
