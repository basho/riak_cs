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

%% @doc These functions are used by the riak-cs-stanchion command line script.

-module(riak_cs_stanchion_console).

-export([
         switch/1,
         show/1
        ]).

-define(SAFELY(Code, Description),
        try
            Code
        catch
            Type:Reason ->
                io:format("~s failed:~n  ~p:~p~n",
                          [Description, Type, Reason]),
                error
        end).

-define(SCRIPT_NAME, "riak-cs-admin stanchion").
-include("riak_cs.hrl").

%%%===================================================================
%%% Public API
%%%===================================================================

%% @doc Switch Stanchion to a new one. Can be used for disabling.
switch([Host, Port]) ->
    Msg = io_lib:format("Switching stanchion to ~s:~s", [Host, Port]),
    ?SAFELY(begin
                NewPort = list_to_integer(Port),
                true = (0 < NewPort andalso NewPort < 65536),
                %% {_, _, SSL} = riak_cs_utils:stanchion_data(),
                %% currently this does not work due to bad path generation of velvet:ping/3
                %% ok = velvet:ping(Host, NewPort, SSL),

                %% Or, set this configuration to dummy host/port
                %% to block bucket/user creation/deletion
                %% in case of multiple stanchion working.
                ok = application:set_env(riak_cs, stanchion_host, {Host, NewPort}),
                Msg2 = io_lib:format("Succesfully switched stanchion to ~s:~s: This change is only effective until restart.",
                                    [Host, Port]),
                _ = lager:info(Msg2),
                io:format("~s~nTo make permanent change, be sure to edit configuration file.~n", [Msg2])
            end, Msg);
switch(_) ->
    io:format("Usage: riak-cs-admin stanchion switch IP Port~n"),
    error.

show([]) ->
    ?SAFELY(begin
                {Host, Port, SSL} = riak_cs_utils:stanchion_data(),
                Scheme = case SSL of
                             true -> "https://";
                             false -> "http://"
                         end,
                io:format("Current Stanchion Address: ~s~s:~s~n",
                          [Scheme, Host, integer_to_list(Port)])
            end, "Retrieving Stanchion info").
