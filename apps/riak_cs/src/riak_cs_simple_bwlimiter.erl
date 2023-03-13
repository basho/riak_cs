%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved,
%%               2021-2023 TI Tokyo    All Rights Reserved.
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

%% @doc Simple node-level rate limit implementation, per user
%% This is experimental feature.
%%
%% riak_cs_bwlimiter help
%%
%%    This module enables node-level rate limiting.
%%
%% set_limits(DurationSec, BandwidthMax, AccessCountMax)
%%
%%   Set bandwidth and access limit. DurationSec is refresh rate.
%%   set_limits(100, -1, -1) will remove all rate limitation. But it is highly
%%   recommended to disable this module by not setting `quota_modules`.
%%
%% state()
%%
%%   Shows cached list of users being limited.
%%   {user_state, User Key, Current bytes, Access count}.
%%
%% reset()
%%
%%   Resets current state rate limiting. Limits won't be cleared.

-module(riak_cs_simple_bwlimiter).

-behaviour(riak_cs_quota).

-export([state/0, reset/0, set_limits/3]).
-export([start_link/0, allow/3, update/2, error_response/3]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine_logger.hrl").
-include_lib("webmachine/include/wm_reqdata.hrl").
-include_lib("kernel/include/logger.hrl").

-record(user_state, {
          user :: binary(),
          bandwidth_acc = 0 :: non_neg_integer(), %% Bytes usage per refresh rate
          access_count = 0 :: non_neg_integer() %% Access count per refresh rate
         }).

%% Quota not set
-define(DEFAULT_SIMPLE_QUOTA, -1).
%% A day
-define(DEFAULT_REFRESH_INTERVAL_SEC, 10).

-spec set_limits(integer(), integer(), integer()) -> true.
set_limits(DurationSec, BandwidthMax, AccessCountMax)
  when DurationSec > 0 ->
    ok = application:set_env(riak_cs, bwlimiter_interval,
                             DurationSec),
    ok = application:set_env(riak_cs, bwlimiter_bandwidth_limit,
                             BandwidthMax),
    ok = application:set_env(riak_cs, bwlimiter_rate_limit,
                             AccessCountMax),
    reset().

-spec state() -> tuple().
state() ->
    {{bwlimiter_interval,
      application:get_env(riak_cs, bwlimiter_interval)},
     {bwlimiter_bandwidth_limit,
      application:get_env(riak_cs, bwlimiter_bandwidth_limit)},
     {bwlimiter_rate_limit,
      application:get_env(riak_cs, bwlimiter_rate_limit)},
     {access_rates,
      ets:tab2list(?MODULE)}}.

-spec reset() -> true.
reset() ->
    ets:delete_all_objects(?MODULE),
    ?MODULE ! reset,
    true.

-spec start_link() -> {ok, pid()}.
start_link() ->
    ok = application:set_env(riak_cs, bwlimiter_interval,
                             ?DEFAULT_REFRESH_INTERVAL_SEC),
    ok = application:set_env(riak_cs, bwlimiter_bandwidth_limit, -1),
    ok = application:set_env(riak_cs, bwlimiter_rate_limit, -1),

    TableName = ?MODULE,
    TableName = ets:new(TableName, [{write_concurrency, true},
                                    {keypos, #user_state.user},
                                    named_table, public, set]),
    Pid = proc_lib:spawn_link(fun() -> refresher() end),
    register(?MODULE, Pid),
    {ok, Pid}.

refresher() ->
    IntervalSec = case application:get_env(riak_cs, bwlimiter_interval) of
                      {ok, V} when is_integer(V) andalso V > 0 -> V;
                      _ -> ?DEFAULT_REFRESH_INTERVAL_SEC
                  end,
    receive
        reset ->
            ?LOG_DEBUG("reset received: ~p", [?MODULE]),
            refresher();
        _ ->
            ets:delete(?MODULE)
    after IntervalSec * 1000 ->
            ?LOG_DEBUG("~p refresh in ~p secs", [?MODULE, IntervalSec]),
            ets:delete_all_objects(?MODULE),
            refresher()
    end.

-spec allow(rcs_user(), access(), #rcs_s3_context{}) -> {ok, #wm_reqdata{}, #rcs_s3_context{}}.
allow(Owner, #access_v1{req = RD} = _Access, Ctx) ->

    OwnerKey = list_to_binary(riak_cs_user:key_id(Owner)),
    ?LOG_DEBUG("access => ~p", [OwnerKey]),
    UserState = case ets:lookup(?MODULE, OwnerKey) of
                    [UserState0] -> UserState0;
                    [] -> new_user_state(OwnerKey)
                end,
    IntervalSec =
        case application:get_env(riak_cs, bwlimiter_interval) of
            {ok, V} when is_integer(V) -> V;
            _ -> ?DEFAULT_REFRESH_INTERVAL_SEC
        end,
    case test_bandwidth_state(IntervalSec, UserState) of
        ok ->
            case test_access_state(IntervalSec, UserState) of
                ok ->
                    {ok, RD, Ctx};
                {error, {_, Current, Threshold}} = Error ->
                    logger:info("User ~p has exceeded access limit: usage, limit = ~p, ~p (/sec)",
                                [OwnerKey, Current, Threshold]),
                    Error
            end;
        {error, {_, Current, Threshold}} = Error2 ->
            logger:info("User ~p has exceeded bandwidth limit: usage, limit = ~p, ~p (bytes/sec)",
                        [OwnerKey, Current, Threshold]),
            Error2
    end.

-spec new_user_state(binary()) -> #user_state{}.
new_user_state(User) ->
    UserState = #user_state{user = User},
    ?LOG_DEBUG("quota init: ~p => ~p", [User, UserState]),
    %% Here's a race condition where if so many concurrent access
    %% come, each access can yield a new fresh state and then
    %% receives not access limitation.
    catch ets:insert_new(?MODULE, UserState),
    UserState.

test_bandwidth_state(DurationSec,
                     #user_state{bandwidth_acc = BandwidthAcc}) ->
    BandwidthMax =
        case application:get_env(riak_cs, bwlimiter_bandwidth_limit) of
            {ok, V} when is_integer(V) -> V;
            _ -> -1
        end,
    case BandwidthAcc / DurationSec < BandwidthMax  of
        true -> ok;
        false when BandwidthMax < 0 -> ok; %% No quota set
        false -> {error, {bandwidth_acc, BandwidthAcc / DurationSec, BandwidthMax}}
    end.

test_access_state(IntervalSec,
                  #user_state{access_count = AccessCount}) ->
    AccessCountMax =
        case application:get_env(riak_cs, bwlimiter_rate_limit) of
            {ok, V} when is_integer(V) -> V;
            _ -> -1
        end,
    case AccessCount / IntervalSec < AccessCountMax of
        true -> ok;
        false when AccessCountMax < 0 -> ok; %% No quota set
        false -> {error, {access_count, AccessCount / IntervalSec, AccessCountMax}}
    end.

-spec update(binary(), wm_log_data()) -> ok | {error, term()}.
update(User,
       #wm_log_data{response_length = ResponseLength,
                    headers = Headers} = _LogData) ->

    Bytes0 = case is_integer(ResponseLength) of
                true -> ResponseLength;
                _ -> 0
            end,
    RequestLength = mochiweb_headers:get_value("content-length", Headers),
    Bytes = case catch list_to_integer(RequestLength) of
                Len when is_integer(Len) -> Len + Bytes0;
                _ -> Bytes0
            end,
    UpdateOps = [{#user_state.bandwidth_acc, Bytes},
                 {#user_state.access_count, 1}],
    try
        case ets:update_counter(?MODULE, User, UpdateOps) of
            [_, _] ->
                ok;
            Error0 ->
                logger:warning("something wrong? ~p", [Error0]),
                {error, Error0}
        end
    catch
        error:badarg -> %% record not just found here
            ?LOG_DEBUG("Cache of ~p (maybe not found)", [User]),
            ok;
        Type:Error ->
            %% TODO: show out stacktrace heah
            logger:warning("something wrong? ~p", [Error]),
            {error, {Type, Error}}
    end.

error_response({TypeAtom, Current, Limit}, RD, Ctx) ->
    %% TODO: Webmachine currently does not handle 429, so this yields
    %% Internal Server Error, which is odd.
    StatusCode = 429,
    Type = case TypeAtom of
               bandwidth_acc -> "bandwidth limit";
               access_count -> "access rate limit"
           end,
    Message = io_lib:format("You have exceeded your ~p.", [Type]),
    XmlDoc = {'Error',
              [
               {'Code', ["TooManyRequests"]},
               {'Message', [Message]},
               {'CurrentValueOfRate', [Current]},
               {'AllowedLimitOfRate', [Limit]}
              ]},
    Body = riak_cs_xml:to_xml([XmlDoc]),
    ReqData = wrq:set_resp_header("Content-Type", ?XML_TYPE, RD),
    UpdReqData = wrq:set_resp_body(Body, ReqData),
    {{StatusCode, "Too Many Requests"}, UpdReqData, Ctx}.
