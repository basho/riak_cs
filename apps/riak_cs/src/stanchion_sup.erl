%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2022, 2023 TI Tokyo, All Rights Reserved.
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

%% @doc Supervisor for the stanchion process tree.

-module(stanchion_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).
-export([stanchion_process_specs/0]).

-include("stanchion.hrl").
-include("riak_cs.hrl").
-include("moss.hrl").

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    stanchion_stats:init(),

    ets:new(?STANCHION_OWN_PBC_TABLE, [named_table]),

    Pbc = stanchion_utils:make_pbc(),
    ok = ensure_service_bucket_props(Pbc),

    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

ensure_service_bucket_props(Pbc) ->
    riakc_pb_socket:set_bucket(Pbc, ?SERVICE_BUCKET, [{allow_mult, false}]).


-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    Children = [],
    {ok, {#{strategy => one_for_one,
            intensity => 10,
            period => 10}, Children
         }}.



stanchion_process_specs() ->
    {Ip, Port, _SSL} = riak_cs_config:stanchion(),

    %% Hide any bags from user-facing parts.
    case application:get_env(riak_cs, supercluster_members) of
        undefined -> ok;
        {ok, Bags} -> application:set_env(riak_cs, bags, Bags)
    end,

    WebConfig1 = [{dispatch, stanchion_web:dispatch_table()},
                  {ip, Ip},
                  {port, Port},
                  {nodelay, true},
                  {log_dir, "log"},
                  {error_handler, stanchion_wm_error_handler}
                 ],
    WebConfig =
        case application:get_env(riak_cs, stanchion_ssl) of
            {ok, true} ->
                {ok, CF} = application:get_env(riak_cs, stanchion_ssl_certfile),
                {ok, KF} = application:get_env(riak_cs, stanchion_ssl_keyfile),
                WebConfig1 ++ [{ssl, true},
                               {ssl_opts, [{certfile, CF}, {keyfile, KF}]}];
            {ok, false} ->
                WebConfig1
        end,
    Web =
        #{id => stanchion_webmachine,
          start => {webmachine_mochiweb, start, [WebConfig]},
          restart => permanent,
          shutdown => 5000,
          modules => dynamic},
    ServerSup =
        #{id => stanchion_server_sup,
          start => {stanchion_server_sup, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type => supervisor,
          modules => dynamic},
    [ServerSup, Web].
