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

-module(riak_cs_roles).

-export([create_role/1,
         delete_role/1,
         get_role/2,
         exprec_detailed/1,
         fix_permissions_boundary/1
        ]).

-include("riak_cs.hrl").
-include("aws_api.hrl").
-include_lib("kernel/include/logger.hrl").


-spec create_role(proplist:proplist()) -> {ok, RoleId::string()} | {error, already_exists | term()}.
create_role(Specs) ->
    Encoded = jsx:encode(Specs),
    {ok, AdminCreds} = riak_cs_config:admin_creds(),
    Result = velvet:create_role(
               "application/json",
               Encoded,
               [{auth_creds, AdminCreds}]),
    handle_response(Result).

-spec delete_role(string()) -> ok | {error, term()}.
delete_role(RoleId) ->
    {ok, AdminCreds} = riak_cs_config:admin_creds(),
    Result = velvet:delete_role(RoleId, [{auth_creds, AdminCreds}]),
    handle_response(Result).

-spec get_role(string(), pid()) -> {ok, ?IAM_ROLE{}} | {error, term()}.
get_role(RoleName, RcPid) ->
    BinKey = list_to_binary(RoleName),
    case riak_cs_riak_client:get_role(RcPid, BinKey) of
        {ok, Obj} ->
            {ok, from_riakc_obj(Obj)};
        Error ->
            Error
    end.

from_riakc_obj(Obj) ->
    case riakc_obj:value_count(Obj) of
        1 ->
            binary_to_term(riakc_obj:get_value(Obj));
        0 ->
            error(no_value);
        N ->
            Values = [binary_to_term(Value) ||
                         Value <- riakc_obj:get_values(Obj),
                         Value /= <<>>  % tombstone
                     ],
            Role = hd(Values),
            logger:warning("Role object (RoleId: ~s, RoleName: \"~s\") has ~b siblings",
                           [Role?IAM_ROLE.role_id, Role?IAM_ROLE.role_name, N]),
            Role
    end.


handle_response({ok, RoleId}) ->
    {ok, RoleId};
handle_response(ok) ->
    ok;
handle_response({error, {error_status, _, _, ErrorDoc}}) ->
    riak_cs_s3_response:error_response(ErrorDoc);
handle_response({error, _} = Error) ->
    Error.


-spec exprec_detailed(maps:map()) -> ?IAM_ROLE{}.
exprec_detailed(Map) ->
    Role0 = ?IAM_ROLE{permissions_boundary = PB0,
                      role_last_used = LU0,
                      tags = TT0} = exprec:frommap_role_v1(Map),
    TT = [exprec:frommap_tag(T) || T <- TT0],
    LU = case LU0 of
             undefined ->
                 undefined;
             _ ->
                 exprec:frommap_role_last_used(LU0)
         end,
    PB = case PB0 of
             undefined ->
                 undefined;
             _ ->
                 exprec:frommap_permissions_boundary(PB0)
         end,
    Role0?IAM_ROLE{permissions_boundary = PB,
                   role_last_used = LU,
                   tags = TT}.

-spec fix_permissions_boundary(maps:map()) -> maps:map().
fix_permissions_boundary(#{permissions_boundary := A} = Map) when not is_map(A) ->
    maps:update(permissions_boundary, #{permissions_boundary_arn => A}, Map);
fix_permissions_boundary(Map) ->
    Map.


-ifdef(TEST).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-endif.
