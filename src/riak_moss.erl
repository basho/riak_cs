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

%% @doc riak_moss startup code

-module(riak_moss).

-export([start/0, start_link/0, stop/0]).

-compile(export_all).

-include("riak_moss.hrl").

make_bucket(KeyId, Bucket) ->
    iolist_to_binary([KeyId, ":", Bucket]).

make_key() ->
    KeySize = 64,
    {A, B, C} = erlang:now(),
    random:seed(A, B, C),
    Rand = random:uniform(pow(2, KeySize)),
    BKey = <<Rand:KeySize>>,
    binary_to_hexlist(BKey).

%% @spec (integer(), integer(), integer()) -> integer()
%% @doc Integer version of the standard pow() function.
pow(Base, Power, Acc) ->
    case Power of
        0 ->
            Acc;
        _ ->
            pow(Base, Power - 1, Acc * Base)
    end.

%% @spec (integer(), integer()) -> integer()
%% @doc Integer version of the standard pow() function; call the recursive accumulator to calculate.
pow(Base, Power) ->
    pow(Base, Power, 1).

%% @spec (binary()) -> string()
%% @doc Convert the passed binary into a string where the numbers are represented in hexadecimal (lowercase and 0 prefilled).
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


riak_client() ->
    {ok, _Pid} = riakc_pb_socket:start_link("127.0.0.1", 8087).


moss_client(User) ->
    {ok, Pid} = riak_client(),
    {ok, riak_s3_client:new(Pid, User)}.


ensure_started(App) ->
    case application:start(App) of
        ok ->
            ok;
        {error, {already_started, App}} ->
            ok
    end.

%% @spec start_link() -> {ok,Pid::pid()}
%% @doc Starts the app for inclusion in a supervisor tree
start_link() ->
    ensure_started(inets),
    ensure_started(crypto),
    ensure_started(mochiweb),
    application:set_env(webmachine, webmachine_logger_module,
                        webmachine_logger),
    ensure_started(webmachine),
    riak_moss_sup:start_link().

%% @spec start() -> ok
%% @doc Start the riak_moss server.
start() ->
    ensure_started(inets),
    ensure_started(crypto),
    ensure_started(mochiweb),
    application:set_env(webmachine, webmachine_logger_module,
                        webmachine_logger),
    ensure_started(webmachine),
    application:start(riak_moss).

%% @spec stop() -> ok
%% @doc Stop the riak_moss server.
stop() ->
    Res = application:stop(riak_moss),
    application:stop(webmachine),
    application:stop(mochiweb),
    application:stop(crypto),
    application:stop(inets),
    Res.
