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

%% @doc Callbacks for the riak_cs application.

-module(riak_cs_app).

-behaviour(application).

%% application API
-export([start/2,
         stop/1,
         sanity_check/2,
         check_bucket_props/2,
         atoms_for_check_bucket_props/0]).

-include("riak_cs.hrl").

-type start_type() :: normal | {takeover, node()} | {failover, node()}.
-type start_args() :: term().

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc application start callback for riak_cs.
-spec start(start_type(), start_args()) -> {ok, pid()} |
                                           {error, term()}.
start(_Type, _StartArgs) ->
    riak_cs_config:warnings(),
    sanity_check(is_config_valid(),
                 check_bucket_props()).

%% @doc application stop callback for riak_cs.
-spec stop(term()) -> ok.
stop(_State) ->
    ok.

-spec sanity_check(boolean(), {ok, boolean()} | {error, term()}) -> {ok, pid()} | {error, term()}.
sanity_check(true, {ok, true}) ->
    riak_cs_sup:start_link();
sanity_check(false, _) ->
    _ = lager:error("You must update your Riak CS app.config. Please see the"
                    "release notes for more information on updating you"
                    "configuration."),
    {error, bad_config};
sanity_check(true, {ok, false}) ->
        _ = lager:error("Invalid Riak bucket properties detected. Please "
                        "verify that allow_mult is set to true for all "
                        "buckets."),
    {error, invalid_bucket_props};
sanity_check(true, {error, Reason}) ->
        _ = lager:error("Could not verify bucket properties. Error was"
                       " ~p.", [Reason]),
    {error, error_verifying_props}.

-spec is_config_valid() -> boolean().
is_config_valid() ->
    get_env_response_to_bool(application:get_env(riak_cs, connection_pools))
        andalso
        case riak_cs_list_objects_utils:fold_objects_for_list_keys() of
            false ->
                _ = lager:critical("`fold_objects_for_list_keys` is set as false."
                                   " This is removed in this version."),
                false;
            _ -> true
        end.

-spec get_env_response_to_bool(term())  -> boolean().
get_env_response_to_bool({ok, _}) ->
    true;
get_env_response_to_bool(_) ->
    false.

-spec check_bucket_props() -> {ok, boolean()} | {error, term()}.
check_bucket_props() ->
    Buckets = [?USER_BUCKET,
               ?ACCESS_BUCKET,
               ?STORAGE_BUCKET,
               ?BUCKETS_BUCKET],
    {ok, MasterPbc} = riak_connection(),
    try
        Results = [check_bucket_props(Bucket, MasterPbc) ||
                   Bucket <- Buckets],
        lists:foldl(fun promote_errors/2, {ok, true}, Results)
    after
        riakc_pb_socket:stop(MasterPbc)
    end.

%% Put atoms into atom table to suppress warning logs in `check_bucket_props'
atoms_for_check_bucket_props() ->
    [riak_core_util, chash_std_keyfun,
     riak_kv_wm_link_walker, mapreduce_linkfun].

promote_errors(_Elem, {error, _Reason}=E) ->
    E;
promote_errors({error, _Reason}=E, _Acc) ->
    E;
promote_errors({ok, false}=F, _Acc) ->
    F;
promote_errors({ok, true}, Acc) ->
    Acc.

-spec check_bucket_props(binary(), pid()) -> {ok, boolean()} | {error, term()}.
check_bucket_props(Bucket, MasterPbc) ->
    case catch riakc_pb_socket:get_bucket(MasterPbc, Bucket) of
        {ok, Props} ->
            case lists:keyfind(allow_mult, 1, Props) of
                {allow_mult, true} ->
                    _ = lager:debug("~s bucket was"
                                    " already configured correctly.",
                                    [Bucket]),
                    {ok, true};
                _ ->
                    _ = lager:warning("~p is misconfigured", [Bucket]),
                    {ok, false}
            end;
        {error, Reason}=E ->
            _ = lager:warning(
                  "Unable to verify ~s bucket settings (~p).",
                  [Bucket, Reason]),
            E
    end.

riak_connection() ->
    {Host, Port} = riak_cs_config:riak_host_port(),
    Timeout = case application:get_env(riak_cs, riakc_connect_timeout) of
                  {ok, ConfigValue} ->
                      ConfigValue;
                  undefined ->
                      10000
              end,
    StartOptions = [{connect_timeout, Timeout},
                    {auto_reconnect, true}],
    riakc_pb_socket:start_link(Host, Port, StartOptions).
