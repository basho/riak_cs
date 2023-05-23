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

-module(riak_cs_temp_sessions).

-export([create/2,
         get/2
        ]).

-export([close_session/1]).

-include("riak_cs.hrl").
-include_lib("kernel/include/logger.hrl").

-define(USER_ID_LENGTH, 16).  %% length("ARO456EXAMPLE789").


-spec create(non_neg_integer(), pid()) -> {ok, temp_session()} | {error, term()}.
create(DurationSeconds, Pbc) ->
    UserId = riak_cs_aws_utils:make_id(?USER_ID_LENGTH, "ARO"),
    {KeyId, AccessKey} = riak_cs_aws_utils:generate_access_creds(UserId),
    Session = #temp_session{user_id = UserId,
                            credentials = #credentials{access_key_id = KeyId,
                                                       secret_access_key = AccessKey,
                                                       expiration = os:system_time(second) + DurationSeconds,
                                                       session_token = make_session_token()},
                            duration_seconds = DurationSeconds},
    Obj = riakc_obj:new(?TEMP_SESSIONS_BUCKET, UserId, term_to_binary(Session)),
    case riakc_pb_socket:put(Pbc, Obj, [{w, all}, {pw, all}]) of
        ok ->
            logger:info("Opened new temp session for user ~s", [UserId]),
            {ok, _Tref} = timer:apply_after(DurationSeconds * 1000,
                                            ?MODULE, close_session, [UserId]),
            {ok, Session};
        {error, Reason} = ER ->
            logger:error("Failed to save temp session: ~p", [Reason]),
            ER
    end.

-spec get(binary(), pid()) -> {ok, temp_session()} | {error, term()}.
get(Id, Pbc) ->
    case riakc_pb_socket:get(Pbc, ?TEMP_SESSIONS_BUCKET, Id) of
        {ok, Obj} ->
            session_from_riakc_obj(Obj);
        ER ->
            ER
    end.

session_from_riakc_obj(Obj) ->
    case [binary_to_term(Value) || Value <- riakc_obj:get_values(Obj),
                                   Value /= <<>>,
                                   Value /= ?DELETED_MARKER] of
        [] ->
            {error, notfound};
        [S] ->
            {ok, S};
        [S|_] = VV ->
            logger:warning("Temp session object for user ~s has ~b siblings", [S#temp_session.user_id, length(VV)]),
            {ok, S}
    end.

make_session_token() ->
    ?LOG_DEBUG("STUB"),
    
    riak_cs_aws_utils:make_id(800).

close_session(Id) ->
    {ok, Pbc} = riak_cs_riak_client:checkout(),
    _ = case riakc_pb_socket:get(Pbc, ?TEMP_SESSIONS_BUCKET, Id) of
            {ok, Obj0} ->
                Obj1 = riakc_obj:update_value(Obj0, ?DELETED_MARKER),
                case riakc_pb_socket:put(Pbc, Obj1, [{dw, all}]) of
                    ok ->
                        ok;
                    {error, Reason} ->
                        logger:warning("Failed to delete temp session for user ~s: ~p", [Id, Reason]),
                        still_ok
                end;
            {error, _} ->
                nevermind
        end,
    riak_cs_riak_client:checkin(Pbc).

