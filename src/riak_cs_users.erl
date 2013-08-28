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

%% @doc Helpers for listing Riak CS users

-module(riak_cs_users).

-export([
         stream_users/4
]).
-include("riak_cs.hrl").

stream_users(Format, RiakPid, Boundary, Status) ->
    case riakc_pb_socket:stream_list_keys(RiakPid, ?USER_BUCKET) of
        {ok, ReqId} ->
            case riak_cs_utils:riak_connection() of
                {ok, RiakPid2} ->
                    Res = wait_for_users(Format, RiakPid2, ReqId, Boundary, Status),
                    riak_cs_utils:close_riak_connection(RiakPid2),
                    Res;
                {error, _Reason} ->
                    wait_for_users(Format, RiakPid, ReqId, Boundary, Status)
            end;
        {error, _Reason} ->
            {<<>>, done}
    end.

wait_for_users(Format, RiakPid, ReqId, Boundary, Status) ->
    receive
        {ReqId, {keys, UserIds}} ->
            FoldFun = user_fold_fun(RiakPid, Status),
            Doc = users_doc(lists:foldl(FoldFun, [], UserIds),
                            Format,
                            Boundary),
            {Doc, fun() -> wait_for_users(Format, RiakPid, ReqId, Boundary, Status) end};
        {ReqId, done} ->
            {list_to_binary(["\r\n--", Boundary, "--"]), done};
        _ ->
            wait_for_users(Format, RiakPid, ReqId, Boundary, Status)
    end.

%% @doc Compile a multipart entity for a set of user documents.
users_doc(UserDocs, xml, Boundary) ->
    ["\r\n--",
     Boundary,
     "\r\nContent-Type: ", ?XML_TYPE, "\r\n\r\n",
     riak_cs_xml:to_xml({users, UserDocs})];
users_doc(UserDocs, json, Boundary) ->
    ["\r\n--",
     Boundary,
     "\r\nContent-Type: ", ?JSON_TYPE, "\r\n\r\n",
     riak_cs_json:to_json({users, UserDocs})].

%% @doc Return a fold function to retrieve and filter user accounts
user_fold_fun(RiakPid, Status) ->
    fun(UserId, Users) ->
            case riak_cs_utils:get_user(binary_to_list(UserId), RiakPid) of
                {ok, {User, _}} when User?RCS_USER.status =:= Status;
                                     Status =:= undefined ->
                    [User | Users];
                {ok, _} ->
                    %% Status is defined and does not match the account status
                    Users;
                {error, Reason} ->
                    _ = lager:warning("Failed to fetch user record. KeyId: ~p"
                                      " Reason: ~p", [UserId, Reason]),
                    Users
            end
    end.
