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

%% @doc Simple node-level Quota / QoS implementation
%% This is experimental feature.
%%
%% riak_cs_simple_quota help
%%
%%   This module enables cluster-wide soft quota control.
%%
%% set_quotas(DurationSec, UserQuota)
%%
%%   Set User Quota. DurationSec is its refresh rate.
%%   `set_quotas(100, -1)' will remove all quota. But it is highly
%%   recommended to disable this module by not setting `quota_modules`.
%%
%% state()
%%
%%   Shows cached list of users being limited.
%%
%% reset()
%%
%%   Resets current quota cache.
%%
%% --
%% Known limitations (TODO):
%% - user's latest storage usage from moss.storage won't scale
%%   for many CS node
%% - no integration tests

-module(riak_cs_simple_quota).

-behaviour(riak_cs_quota).

-export([state/0, reset/0, set_params/2]).
-export([start_link/0, allow/3, update/2, error_response/3]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine_logger.hrl").
-include_lib("webmachine/include/wm_reqdata.hrl").
-include_lib("kernel/include/logger.hrl").

%% Quota not set
-define(DEFAULT_SIMPLE_QUOTA, -1).
%% A day
-define(DEFAULT_REFRESH_INTERVAL_SEC, 86400).

-spec set_params(integer(), non_neg_integer()) -> true.
set_params(IntervalSec, UserQuota) when IntervalSec > 0 ->
    ok = application:set_env(riak_cs, simple_quota_amount, UserQuota),

    ok = application:set_env(riak_cs, simple_quota_interval, IntervalSec),
    reset().

-spec state() -> tuple().
state() ->
    {{simple_quota_amount,
      application:get_env(riak_cs, simple_quota_amount)},
     {simple_quota_interval,
      application:get_env(riak_cs, simple_quota_interval)},
     {cached_quotas,
      ets:tab2list(?MODULE)}}.

-spec reset() -> true.
reset() ->
    ets:delete_all_objects(?MODULE),
    ?MODULE ! reset,
    true.

-spec start_link() -> {ok, pid()}.
start_link() ->
    TableName = ?MODULE,
    TableName = ets:new(TableName, [{write_concurrency, true},
                                    public, set, named_table]),
    ok = application:set_env(riak_cs, simple_quota_amount, ?DEFAULT_SIMPLE_QUOTA),
    Pid = proc_lib:spawn_link(fun() -> refresher() end),
    true = register(?MODULE, Pid),
    {ok, Pid}.

refresher() ->
    IntervalSec = case application:get_env(riak_cs, simple_quota_interval) of
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

%% @doc Only puts affected

-spec allow(rcs_user(), access(), #rcs_s3_context{}) ->
                   {ok, #wm_reqdata{}, #rcs_s3_context{}} |
                   {error, {disk_quota, non_neg_integer(), non_neg_integer()}}.
allow(Owner, #access_v1{req = RD, method = 'PUT'} = _Access, Ctx) ->
    OwnerKey = iolist_to_binary(riak_cs_user:key_id(Owner)),
    ?LOG_DEBUG("access => ~p", [OwnerKey]),

    {_, Usage} = case ets:lookup(?MODULE, OwnerKey) of
                     [{OwnerKey, _Usage} = UserState0] -> UserState0;
                     [] -> {OwnerKey, maybe_usage(OwnerKey, Ctx)}
                 end,

    Quota = case application:get_env(riak_cs, simple_quota_amount) of
                {ok, V} when is_integer(V) -> V;
                _ -> ?DEFAULT_SIMPLE_QUOTA
            end,
    if
        Quota < 0 -> {ok, RD, Ctx};
        Usage < Quota -> {ok, RD, Ctx};
        true ->
            logger:info("User ~s has exceeded it's quota: usage, quota = ~p, ~p (bytes)",
                        [OwnerKey, Usage, Quota]),
            {error, {disk_quota, Usage, Quota}}
    end;
allow(_Owner, #access_v1{req = RD} = _Access, Ctx) ->
    {ok, RD, Ctx}.

-spec maybe_usage(binary(), #rcs_s3_context{}) -> non_neg_integer().
maybe_usage(_, _Ctx = #rcs_s3_context{riak_client=undefined}) ->
    %% can't happen here
    error(no_riak_client);
maybe_usage(User0, _Ctx = #rcs_s3_context{riak_client=RiakClient}) ->
    User = binary_to_list(User0),
    Usage = case get_latest_usage(RiakClient, User) of
                {error, notfound} ->
                    logger:warning("No storage stats data was found. Starting as no usage."),
                    0;
                {ok, Usages} ->
                    sum_all_buckets(lists:last(Usages))
            end,
    %% Here's a race condition where if so many concurrent access
    %% come, each access can yield a new fresh state and then
    %% receives not access limitation.
    try
        ets:insert_new(?MODULE, {User0, Usage}),
        Usage
    catch _:_ ->
            Usage
    end.

update(User,
       #wm_log_data{method = Method,
                    headers = Headers} = _LogData)
  when Method =:= 'PUT' orelse Method =:= 'DELETE' ->

    RequestLength = mochiweb_headers:get_value("content-length", Headers),
    Bytes = case catch list_to_integer(RequestLength) of
                Len when is_integer(Len) ->
                    case Method of
                        'PUT' -> Len;
                        %% In case of DELETE, the quota should be
                        %% reclaimed. But it will need content size of
                        %% deleted object, which is not included in
                        %% the request or context. So, just using 0.
                        'DELETE' -> 0
                    end;
                _ -> 0
            end,
    try
        ets:update_counter(?MODULE, User, Bytes)
    catch Type:Error ->
            %% TODO: show out stacktrace heah
            logger:warning("something wrong? ~p", [Error]),
            {error, {Type, Error}}
    end;
update(_, _) ->
    ok.

error_response({disk_quota, Current, Limit}, RD, Ctx) ->
    StatusCode = 403,
    XmlDoc = {'Error',
              [
               {'Code', [StatusCode]},
               {'Message', ["You have Exceeded your quota. Please delete your data."]},
               {'CurrentValueOfQuota', [Current]},
               {'AllowedLimitOfQuota', [Limit]}
              ]},
    Body = riak_cs_xml:to_xml([XmlDoc]),
    ReqData = wrq:set_resp_header("Content-Type", ?XML_TYPE, RD),
    UpdReqData = wrq:set_resp_body(Body, ReqData),
    {StatusCode, UpdReqData, Ctx}.

-spec get_latest_usage(pid(), string()) -> {ok, list()} | {error, notfound}.
get_latest_usage(Pid, User) ->
    Now = calendar:now_to_datetime(os:timestamp()),
    NowASec = calendar:datetime_to_gregorian_seconds(Now),
    ArchivePeriod = case riak_cs_storage:archive_period() of
                        {ok, Period} -> Period;
                        _ -> 86400 %% A day is hard coded
                    end,
    get_latest_usage(Pid, User, ArchivePeriod, NowASec, 0, 10).

-spec get_latest_usage(pid(), string(), non_neg_integer(),
                       non_neg_integer(), non_neg_integer(), non_neg_integer()) ->
                              {ok, list()} | {error, notfound}.
get_latest_usage(_Pid, _User, _, _, N, N) -> {error, notfound};
get_latest_usage(Pid, User, ArchivePeriod, EndSec, N, Max) ->
    End = calendar:gregorian_seconds_to_datetime(EndSec),
    EndSec2 = EndSec - ArchivePeriod,
    ADayAgo = calendar:gregorian_seconds_to_datetime(EndSec2),
    case riak_cs_storage:get_usage(Pid, User, false, ADayAgo, End) of
        {[], _} ->
            get_latest_usage(Pid, User, ArchivePeriod, EndSec2, N+1, Max);
        {Res, _} ->
            {ok, Res}
    end.

-spec sum_all_buckets(list()) -> non_neg_integer().
sum_all_buckets(UsageList) ->
    lists:foldl(fun({<<"StartTime">>, _}, Sum) -> Sum;
                   ({<<"EndTime">>, _}, Sum) -> Sum;
                   ({_BucketName, {struct, Data}}, Sum) ->
                        case proplists:get_value(<<"Bytes">>, Data) of
                            I when is_integer(I) -> Sum + I;
                            _ -> Sum
                        end;
                   (_, Sum) ->
                        Sum
                end, 0, UsageList).
