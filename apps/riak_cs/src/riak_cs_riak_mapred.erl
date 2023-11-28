%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2023 TI Tokyo    All Rights Reserved.
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

-module(riak_cs_riak_mapred).

%% mapreduce functions that run on a riak node

-export([map_keys_and_manifests/3,
         maybe_process_resolved/3,
         reduce_keys_and_manifests/2,
         map_users/3,
         reduce_users/2,
         map_roles/3,
         reduce_roles/2,
         map_policies/3,
         reduce_policies/2,
         map_saml_providers/3,
         reduce_saml_providers/2,
         map_temp_sessions/3,
         reduce_temp_sessions/2,

         query/2
        ]).

-include("moss.hrl").


query(users, Arg) ->
    [{map, {modfun, ?MODULE, map_users},
      Arg, false},
     {reduce, {modfun, ?MODULE, reduce_users},
      Arg, true}];
query(roles, Arg) ->
    [{map, {modfun, ?MODULE, map_roles},
      Arg, false},
     {reduce, {modfun, ?MODULE, reduce_roles},
      Arg, true}];
query(policies, Arg) ->
    [{map, {modfun, ?MODULE, map_policies},
      Arg, false},
     {reduce, {modfun, ?MODULE, reduce_policies},
      Arg, true}];
query(saml_providers, Arg) ->
    [{map, {modfun, ?MODULE, map_saml_providers},
      Arg, false},
     {reduce, {modfun, ?MODULE, reduce_saml_providers},
      Arg, true}];
query(temp_sessions, Arg) ->
    [{map, {modfun, ?MODULE, map_temp_sessions},
      Arg, false},
     {reduce, {modfun, ?MODULE, reduce_temp_sessions},
      Arg, true}].


%% MapReduce function, runs on the Riak nodes, should therefore use
%% riak_object, not riakc_obj.
map_keys_and_manifests({error, notfound}, _, _) ->
    [];
map_keys_and_manifests(Object, _, _) ->
    Handler = fun(Resolved) ->
                      case rcs_common_manifest_utils:active_manifest(Resolved) of
                          {ok, Manifest} ->
                              [{riak_object:key(Object), {ok, Manifest}}];
                          _ ->
                              []
                      end
              end,
    maybe_process_resolved(Object, Handler, []).

maybe_process_resolved(Object, ResolvedManifestsHandler, ErrorReturn) ->
    try
        AllManifests = [ binary_to_term(V)
                         || {_, V} = Content <- riak_object:get_contents(Object),
                            not riak_cs_util:has_tombstone(Content) ],
        Upgraded = rcs_common_manifest_utils:upgrade_wrapped_manifests(AllManifests),
        Resolved = rcs_common_manifest_resolution:resolve(Upgraded),
        ResolvedManifestsHandler(Resolved)
    catch Type:Reason:StackTrace ->
            logger:error("Riak CS object mapreduce failed for ~p:~p with reason ~p:~p at ~p",
                         [riak_object:bucket(Object),
                          riak_object:key(Object),
                          Type,
                          Reason,
                          StackTrace]),
            ErrorReturn
    end.

%% Pipe all the bucket listing results through a passthrough reduce
%% phase.  This is just a temporary kludge until the sink backpressure
%% work is done.
reduce_keys_and_manifests(Acc, _) ->
    Acc.


map_users({error, notfound}, _, _) ->
    [];
map_users(Object, _2, Args) ->
    #{path_prefix := PathPrefix} = Args,
    case riak_object:get_values(Object) of
        [] ->
            [];
        [<<>>|_] ->
            [];
        [RBin|_] ->
            ?IAM_USER{path = Path} = R = binary_to_term(RBin),
            case binary:longest_common_prefix([Path, PathPrefix]) of
                0 ->
                    [];
                _ ->
                    [R]
            end
    end.

map_roles({error, notfound}, _, _) ->
    [];
map_roles(Object, _2, Args) ->
    #{path_prefix := PathPrefix} = Args,
    case riak_object:get_values(Object) of
        [] ->
            [];
        [<<>>|_] ->
            [];
        [RBin|_] ->
            ?IAM_ROLE{path = Path} = R = binary_to_term(RBin),
            case binary:longest_common_prefix([Path, PathPrefix]) of
                0 ->
                    [];
                _ ->
                    [R]
            end
    end.

reduce_users(Acc, _) ->
    Acc.
reduce_roles(Acc, _) ->
    Acc.

map_policies({error, notfound}, _, _) ->
    [];
map_policies(Object, _2, Args) ->
    #{path_prefix := PathPrefix,
      only_attached := OnlyAttached,
      policy_usage_filter := PolicyUsageFilter,
      scope := Scope} = Args,
    logger:notice("list_roles: Ignoring parameters PolicyUsageFilter (~s) and Scope (~s)", [PolicyUsageFilter, Scope]),
    case riak_object:get_values(Object) of
        [] ->
            [];
        [<<>>|_] ->
            [];
        [PBin|_] ->
            ?IAM_POLICY{path = Path,
                        attachment_count = AttachmentCount} = P = binary_to_term(PBin),
            case (0 < binary:longest_common_prefix([Path, PathPrefix])) and
                ((true == OnlyAttached andalso AttachmentCount > 0) orelse false == OnlyAttached) of
                false ->
                    [];
                true ->
                    [P]
            end
    end.

reduce_policies(Acc, _) ->
    Acc.

map_saml_providers({error, notfound}, _, _) ->
    [];
map_saml_providers(Object, _2, _Args) ->
    case riak_object:get_values(Object) of
        [] ->
            [];
        [<<>>|_] ->
            [];
        [PBin|_] ->
            ?IAM_SAML_PROVIDER{} = P = binary_to_term(PBin),
            [P]
    end.

reduce_saml_providers(Acc, _) ->
    Acc.

map_temp_sessions({error, notfound}, _, _) ->
    [];
map_temp_sessions(Object, _2, _Args) ->
    case riak_object:get_values(Object) of
        [] ->
            [];
        [<<>>|_] ->
            [];
        [PBin|_] ->
            ?TEMP_SESSION{created = Created,
                          duration_seconds = DurationSeconds} = P = binary_to_term(PBin),
            case Created + DurationSeconds * 1000 > os:system_time(millisecond) of
                true ->
                    [P];
                false ->
                    []
            end
    end.

reduce_temp_sessions(Acc, _) ->
    Acc.
