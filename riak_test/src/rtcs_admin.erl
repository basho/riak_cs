%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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

-module(rtcs_admin).

-export([storage_stats_json_request/4,
         create_user/2,
         create_user/3,
         create_user/4,
         create_admin_user/1,
         update_user/5,
         list_users/4]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("erlcloud/include/erlcloud_aws.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-spec storage_stats_json_request(#aws_config{}, #aws_config{}, string(), string()) ->
                                        [{string(), {non_neg_integer(), non_neg_integer()}}].
storage_stats_json_request(AdminConfig, UserConfig, Begin, End) ->
    Samples = samples_from_json_request(AdminConfig, UserConfig, {Begin, End}),
    lager:debug("Storage samples[json]: ~p", [Samples]),
    {struct, Slice} = latest(Samples, undefined),
    by_bucket_list(Slice, []).

-spec(create_admin_user(atom()) -> {string(), string()}).
create_admin_user(Node) ->
    User = "admin",
    Email = "admin@me.com",
    {KeyId, Secret, Id} = create_user(rtcs_config:cs_port(Node), Email, User),
    lager:info("Riak CS Admin account created with ~p",[Email]),
    lager:info("KeyId = ~p",[KeyId]),
    lager:info("KeySecret = ~p",[Secret]),
    lager:info("Id = ~p",[Id]),
    {KeyId, Secret}.

-spec(create_user(atom(), non_neg_integer()) -> {string(), string()}).
create_user(Node, UserIndex) ->
    {A, B, C} = erlang:now(),
    User = "Test User" ++ integer_to_list(UserIndex),
    Email = lists:flatten(io_lib:format("~p~p~p@basho.com", [A, B, C])),
    {KeyId, Secret, _Id} = create_user(rtcs_config:cs_port(Node), Email, User),
    lager:info("Created user ~p with keys ~p ~p", [Email, KeyId, Secret]),
    {KeyId, Secret}.

-spec(create_user(non_neg_integer(), string(), string()) -> {string(), string(), string()}).
create_user(Port, EmailAddr, Name) ->
    create_user(Port, undefined, EmailAddr, Name).

-spec(create_user(non_neg_integer(), string(), string(), string()) -> {string(), string(), string()}).
create_user(Port, UserConfig, EmailAddr, Name) ->
    lager:debug("Trying to create user ~p", [EmailAddr]),
    Resource = "/riak-cs/user",
    Date = httpd_util:rfc1123_date(),
    Cmd="curl -s -H 'Content-Type: application/json' " ++
        "-H 'Date: " ++ Date ++ "' " ++
        case UserConfig of
            undefined -> "";
            _ ->
                "-H 'Authorization: " ++
                    make_authorization("POST", Resource, "application/json",
                                       UserConfig, Date) ++
                    "' "
        end ++
        "http://localhost:" ++
        integer_to_list(Port) ++
        Resource ++
        " --data '{\"email\":\"" ++ EmailAddr ++  "\", \"name\":\"" ++ Name ++"\"}'",
    lager:debug("Cmd: ~p", [Cmd]),
    Delay = rt_config:get(rt_retry_delay),
    Retries = rt_config:get(rt_max_wait_time) div Delay,
    OutputFun = fun() -> rt:cmd(Cmd) end,
    Condition = fun({Status, Res}) ->
                        lager:debug("Return (~p), Res: ~p", [Status, Res]),
                        Status =:= 0 andalso Res /= []
                end,
    {_Status, Output} = rtcs:wait_until(OutputFun, Condition, Retries, Delay),
    lager:debug("Create user output=~p~n",[Output]),
    {struct, JsonData} = mochijson2:decode(Output),
    KeyId = binary_to_list(proplists:get_value(<<"key_id">>, JsonData)),
    KeySecret = binary_to_list(proplists:get_value(<<"key_secret">>, JsonData)),
    Id = binary_to_list(proplists:get_value(<<"id">>, JsonData)),
    {KeyId, KeySecret, Id}.

-spec(update_user(#aws_config{}, non_neg_integer(), string(), string(), string()) -> string()).
update_user(UserConfig, Port, Resource, ContentType, UpdateDoc) ->
    Date = httpd_util:rfc1123_date(),
    Cmd="curl -s -X PUT -H 'Date: " ++ Date ++
        "' -H 'Content-Type: " ++ ContentType ++
        "' -H 'Authorization: " ++
        make_authorization("PUT", Resource, ContentType, UserConfig, Date) ++
        "' http://localhost:" ++ integer_to_list(Port) ++
        Resource ++ " --data-binary " ++ UpdateDoc,
    Delay = rt_config:get(rt_retry_delay),
    Retries = rt_config:get(rt_max_wait_time) div Delay,
    OutputFun = fun() -> os:cmd(Cmd) end,
    Condition = fun(Res) -> Res /= [] end,
    Output = rtcs:wait_until(OutputFun, Condition, Retries, Delay),
    lager:debug("Update user output=~p~n",[Output]),
    Output.

-spec(list_users(#aws_config{}, non_neg_integer(), string(), string()) -> string()).
list_users(UserConfig, Port, Resource, AcceptContentType) ->
    Date = httpd_util:rfc1123_date(),
    Cmd="curl -s -H 'Date: " ++ Date ++
        "' -H 'Accept: " ++ AcceptContentType ++
        "' -H 'Authorization: " ++
        make_authorization("GET", Resource, "", UserConfig, Date) ++
        "' http://localhost:" ++ integer_to_list(Port) ++
        Resource,
    Delay = rt_config:get(rt_retry_delay),
    Retries = rt_config:get(rt_max_wait_time) div Delay,
    OutputFun = fun() -> os:cmd(Cmd) end,
    Condition = fun(Res) -> Res /= [] end,
    Output = rtcs:wait_until(OutputFun, Condition, Retries, Delay),
    lager:debug("List users output=~p~n",[Output]),
    Output.

%% private

latest([], {_, Candidate}) ->
    Candidate;
latest([Sample | Rest], undefined) ->
    StartTime = rtcs:json_get([<<"StartTime">>], Sample),
    latest(Rest, {StartTime, Sample});
latest([Sample | Rest], {CandidateStartTime, Candidate}) ->
    StartTime = rtcs:json_get([<<"StartTime">>], Sample),
    NewCandidate = case StartTime < CandidateStartTime of
                       true -> {CandidateStartTime, Candidate};
                       _    -> {StartTime, Sample}
                   end,
    latest(Rest, NewCandidate).

by_bucket_list([], Acc) ->
    lists:sort(Acc);
by_bucket_list([{<<"StartTime">>, _} | Rest], Acc) ->
    by_bucket_list(Rest, Acc);
by_bucket_list([{<<"EndTime">>, _} | Rest], Acc) ->
    by_bucket_list(Rest, Acc);
by_bucket_list([{BucketBin, {struct,[{<<"Objects">>, Objs},
                                     {<<"Bytes">>, Bytes}]}} | Rest],
               Acc) ->
    by_bucket_list(Rest, [{binary_to_list(BucketBin), {Objs, Bytes}}|Acc]).

samples_from_json_request(AdminConfig, UserConfig, {Begin, End}) ->
    KeyId = UserConfig#aws_config.access_key_id,
    StatsKey = string:join(["usage", KeyId, "bj", Begin, End], "/"),
    GetResult = erlcloud_s3:get_object("riak-cs", StatsKey, AdminConfig),
    Usage = mochijson2:decode(proplists:get_value(content, GetResult)),
    rtcs:json_get([<<"Storage">>, <<"Samples">>], Usage).

make_authorization(Method, Resource, ContentType, Config, Date) ->
    make_authorization(Method, Resource, ContentType, Config, Date, []).

make_authorization(Method, Resource, ContentType, Config, Date, AmzHeaders) ->
    make_authorization(s3, Method, Resource, ContentType, Config, Date, AmzHeaders).

make_authorization(Type, Method, Resource, ContentType, Config, Date, AmzHeaders) ->
    Prefix = case Type of
                 s3 -> "AWS";
                 velvet -> "MOSS"
             end,
    StsAmzHeaderPart = [[K, $:, V, $\n] || {K, V} <- AmzHeaders],
    StringToSign = [Method, $\n, [], $\n, ContentType, $\n, Date, $\n,
                    StsAmzHeaderPart, Resource],
    lager:debug("StringToSign~n~s~n", [StringToSign]),
    Signature =
        base64:encode_to_string(rtcs:sha_mac(Config#aws_config.secret_access_key, StringToSign)),
    lists:flatten([Prefix, " ", Config#aws_config.access_key_id, $:, Signature]).
