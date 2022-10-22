%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Callbacks for the stanchion application.

-module(stanchion_app).

-behaviour(application).

-include_lib("riak_cs/include/moss.hrl").

%% application API
-export([start/2,
         stop/1]).


-type start_type() :: normal | {takeover, node()} | {failover, node()}.
-type start_args() :: term().

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc application start callback for stanchion.
-spec start(start_type(), start_args()) -> {ok, pid()} |
                                           {error, term()}.
start(_Type, _StartArgs) ->
    case stanchion_utils:riak_connection() of
        {ok, Pid} ->
            try
                case check_admin_creds(Pid) of
                    ok ->
                        stanchion_sup:start_link();
                    Error ->
                        Error
                end
            after
                stanchion_utils:close_riak_connection(Pid)
            end;
        {error, Reason} ->
            logger:error("Couldn't connect to Riak: ~p", [Reason]),
            {error, Reason}
    end.

%% @doc application stop callback for stanchion.
-spec stop(term()) -> ok.
stop(_State) ->
    ok.

check_admin_creds(Pid) ->
    case application:get_env(stanchion, admin_key) of
        {ok, "admin-key"} ->
            logger:warning("admin.key is defined as default. Please create"
                           " admin user and configure it.", []),
            application:set_env(stanchion, admin_secret, "admin-secret");
        {ok, KeyId} ->
            case application:get_env(stanchion, admin_secret) of
                {ok, _} ->
                    logger:warning("admin.secret is ignored.");
                _ ->
                    ok
            end,
            StrongOpts = [{r, quorum}, {pr, one}, {notfound_ok, false}],
            case riakc_pb_socket:get(Pid, ?USER_BUCKET, KeyId,  StrongOpts) of
                {ok, Obj} ->
                    case stanchion_utils:from_riakc_obj(Obj, false) of
                        {ok, {User, _}} ->
                            Secret = User?RCS_USER.key_secret,
                            application:set_env(stanchion, admin_secret, Secret);
                        Error ->
                            Error
                    end;
                {error, notfound} ->
                    logger:error("admin.key defined in stanchion.conf was not found."
                                 "Please create it."),
                    {error, admin_not_configured};
                Error ->
                    logger:error("Error loading administrator configuration: ~p", [Error]),
                    Error
            end;
        Error ->
            Error
    end.
