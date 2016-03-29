%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.
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
%% -------------------------------------------------------------------

%% @doc Common functions for testing Riak CS.
%%  Several date/time types are exported.
%%
%%
%% @todo Maybe document how this stuff works?
%% @end
-module(rtcs).

-export([
    assert_error_log_empty/1,
    assert_error_log_empty/2,
    assert_versions/3,
    check_error_response/5,
    check_no_such_bucket/2,
    cs_node/1,
    datetime/0,
    datetime/1,
    datetime_to_gs/1,
    datetime_to_ts/1,
    deploy_nodes/3,
    error_child_element_verifier/3,
    flavored_setup/4,
    iso8601/1,
    json_get/2,
    maybe_load_intercepts/1,
    md5/1,
    node_id/1,
    pass/0,
    pbc/3,
    pbc/4,
    reset_log/1,
    riak_id_per_cluster/1,
    riak_node/1,
    set_advanced_conf/2,
    set_conf/2,
    setup/1,
    setup/2,
    setup/3,
    setup2x2/0,
    setup2x2/1,
    setupNxMsingles/2,
    setupNxMsingles/4,
    setup_admin_user/2,
    setup_clusters/4,
    sha/1,
    sha_mac/2,
    ssl_options/1,
    stanchion_node/0,
    teardown/0,
    truncate_error_log/1,
    valid_timestamp/1,
    wait_until/4
]).
-export_type([
    bad_date/0,
    cs_datetime/0,
    datetime/0,
    datetime_str/0,
    dt_error/0,
    epochsecs/0,
    gregorian/0,
    iso8601ts/0,
    rfc2616ts/0,
    timestamp/0
]).
-import(rt, [
    join/2,
    wait_until_nodes_ready/1,
    wait_until_no_pending_changes/1
]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("erlcloud/include/erlcloud_aws.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-define(DEVS(N),    lists:concat(["dev", N, "@127.0.0.1"])).
-define(DEV(N),     erlang:list_to_atom(?DEVS(N))).
-define(CSDEVS(N),  lists:concat(["rcs-dev", N, "@127.0.0.1"])).
-define(CSDEV(N),   erlang:list_to_atom(?CSDEVS(N))).

-ifndef(paranoid_dates).
%% comment out either or both of the following to be paranoid about
%% timestamp validation regardless of whether paranoid_dates is defined
-define(validated_timestamp(TS), TS).
-define(validated_timestamp_op(TS, Func), Func(TS)).
-endif.

% string versions of timestamps have distinct types only for clarity of code
-type bad_date()    :: bad_date | {bad_date, incomplete}
                     | {bad_date, extra_characters}.
-type cs_datetime() :: iso8601ts().
-type datetime()    :: timestamp() | gregorian() | datetime_str().
-type datetime_str():: iso8601ts() | rfc2616ts().
-type dt_error()    :: {error, bad_date() | term()}.
-type epochsecs()   :: integer().
-type gregorian()   :: non_neg_integer().
-type iso8601ts()   :: nonempty_string().
-type rfc2616ts()   :: nonempty_string().
-type timestamp()   :: calendar:datetime().

-define(gregorian_guard(Secs), erlang:is_integer(Secs) andalso Secs >= 0).
%% maybe define paranoid versions
-ifndef(validated_timestamp).
-define(validated_timestamp(TS),
    case valid_timestamp(TS) of
        true ->
            TS;
        _ ->
            {error, bad_date}
    end
).
-endif.
-ifndef(validated_timestamp_op).
-define(validated_timestamp_op(TS, Func),
    case valid_timestamp(TS) of
        true ->
            Func(TS);
        _ ->
            {error, bad_date}
    end
).
-endif.

setup(NumNodes) ->
    setup(NumNodes, [], current).

setup(NumNodes, Configs) ->
    setup(NumNodes, Configs, current).

setup(NumNodes, Configs, Vsn) ->
    Flavor = rt_config:get(flavor, basic),
    lager:info("Flavor : ~p", [Flavor]),
    flavored_setup(NumNodes, Flavor, Configs, Vsn).

setup2x2() ->
    setup2x2([]).

setup2x2(Configs) ->
    JoinFun = fun(Nodes) ->
                      [A,B,C,D] = Nodes,
                      join(B,A),
                      join(D,C)
              end,
    setup_clusters(Configs, JoinFun, 4, current).

%% 1 cluster with N nodes + M cluster with 1 node
setupNxMsingles(N, M) ->
    setupNxMsingles(N, M, [], current).

setupNxMsingles(N, M, Configs, Vsn)
  when Vsn =:= current orelse Vsn =:= previous ->
    JoinFun = fun(Nodes) ->
                      [Target | Joiners] = lists:sublist(Nodes, N),
                      [join(J, Target) || J <- Joiners]
              end,
    setup_clusters(Configs, JoinFun, N + M, Vsn).

flavored_setup(NumNodes, basic, Configs, Vsn) ->
    JoinFun = fun(Nodes) ->
                      [First|Rest] = Nodes,
                      [join(Node, First) || Node <- Rest]
              end,
    setup_clusters(Configs, JoinFun, NumNodes, Vsn);
flavored_setup(NumNodes, {multibag, _} = Flavor, Configs, Vsn)
  when Vsn =:= current orelse Vsn =:= previous ->
    rtcs_bag:flavored_setup(NumNodes, Flavor, Configs, Vsn).

setup_clusters(Configs, JoinFun, NumNodes, Vsn) ->
    %% Start the erlcloud app
    erlcloud:start(),

    %% STFU sasl
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, false),

    {RiakNodes, _CSNodes, _Stanchion} = Nodes =
        deploy_nodes(NumNodes, rtcs_config:configs(Configs, Vsn), Vsn),
    rt:wait_until_nodes_ready(RiakNodes),
    lager:info("Make cluster"),
    JoinFun(RiakNodes),
    ?assertEqual(ok, wait_until_nodes_ready(RiakNodes)),
    ?assertEqual(ok, wait_until_no_pending_changes(RiakNodes)),
    rt:wait_until_ring_converged(RiakNodes),
    AdminConfig =
        case ssl_options(Configs) of
            [] ->
                setup_admin_user(NumNodes, Vsn);
            _SSLOpts ->
                rtcs_admin:create_user_rpc(hd(_CSNodes), "admin-key", "admin-secret")
        end,

    {AdminConfig, Nodes}.

ssl_options(Config) ->
    case proplists:get_value(cs, Config) of
        undefined -> [];
        RiakCS ->
           case proplists:get_value(riak_cs, RiakCS) of
               undefined -> [];
               CSConfig ->
                   proplists:get_value(ssl, CSConfig, [])
           end
    end.

pass() ->
    teardown(),
    pass.

teardown() ->
    %% catch application:stop(sasl),
    catch application:stop(erlcloud),
    catch application:stop(ibrowse).

%% Return Riak node IDs, one per cluster.
%% For example, in basic single cluster case, just return [1].
-spec riak_id_per_cluster(pos_integer()) -> [pos_integer()].
riak_id_per_cluster(NumNodes) ->
    case rt_config:get(flavor, basic) of
        basic -> [1];
        {multibag, _} = Flavor -> rtcs_bag:riak_id_per_cluster(NumNodes, Flavor)
    end.

-spec deploy_nodes(list(), list(), current|previous) -> any().
deploy_nodes(NumNodes, InitialConfig, Vsn)
  when Vsn =:= current orelse Vsn =:= previous ->
    lager:info("Initial Config: ~p", [InitialConfig]),
    {RiakNodes, CSNodes, StanchionNode} = Nodes = {riak_nodes(NumNodes),
                                                   cs_nodes(NumNodes),
                                                   stanchion_node()},

    NodeMap = orddict:from_list(lists:zip(RiakNodes, lists:seq(1, NumNodes))),
    rt_config:set(rt_nodes, NodeMap),
    CSNodeMap = orddict:from_list(lists:zip(CSNodes, lists:seq(1, NumNodes))),
    rt_config:set(rt_cs_nodes, CSNodeMap),

    {_RiakRoot, RiakVsn} = rtcs_dev:riak_root_and_vsn(Vsn, rt_config:get(build_type, oss)),
    lager:debug("setting rt_versions> ~p =>", [Vsn]),

    VersionMap = lists:zip(lists:seq(1, NumNodes), lists:duplicate(NumNodes, RiakVsn)),
    rt_config:set(rt_versions, VersionMap),

    rtcs_exec:stop_all_nodes(node_list(NumNodes), Vsn),

    rtcs_dev:create_dirs(RiakNodes),

    %% Set initial config
    rtcs_config:set_configs(NumNodes,
                            InitialConfig,
                            Vsn),
    rtcs_exec:start_all_nodes(node_list(NumNodes), Vsn),

    [ok = rt:wait_until_pingable(N) || N <- RiakNodes ++ CSNodes ++ [StanchionNode]],
    [ok = rt:check_singleton_node(N) || N <- RiakNodes],
    rt:wait_until_nodes_ready(RiakNodes),

    lager:info("NodeMap: ~p", [ NodeMap ]),
    lager:info("VersionMap: ~p", [VersionMap]),
    lager:info("Deployed nodes: ~p", [Nodes]),

    Nodes.

node_id(Node) ->
    NodeMap = rt_config:get(rt_cs_nodes),
    orddict:fetch(Node, NodeMap).

setup_admin_user(NumNodes, Vsn)
  when Vsn =:= current orelse Vsn =:= previous ->

    %% Create admin user and set in cs and stanchion configs
    AdminCreds = rtcs_admin:create_admin_user(1),
    #aws_config{access_key_id=KeyID,
                secret_access_key=KeySecret} = AdminCreds,

    AdminConf = [{admin_key, KeyID}]
        ++ case Vsn of
               current -> [];
               previous -> [{admin_secret, KeySecret}]
           end,
    rt:pmap(fun(N) ->
                    rtcs:set_advanced_conf({cs, Vsn, N}, [{riak_cs, AdminConf}])
            end, lists:seq(1, NumNodes)),
    rtcs:set_advanced_conf({stanchion, Vsn}, [{stanchion, AdminConf}]),

    UpdateFun = fun({Node, App}) ->
                        ok = rpc:call(Node, application, set_env,
                                      [App, admin_key, KeyID]),
                        ok = rpc:call(Node, application, set_env,
                                      [App, admin_secret, KeySecret])
                end,
    ZippedNodes = [{stanchion_node(), stanchion} |
                  [{CSNode, riak_cs} || CSNode <- cs_nodes(NumNodes) ]],
    lists:foreach(UpdateFun, ZippedNodes),

    lager:info("AdminCreds: ~p", [AdminCreds]),
    AdminCreds.

-spec set_conf(atom() | {atom(), atom()} | string(), [{string(), string()}]) -> ok.
set_conf(all, NameValuePairs) ->
    lager:info("rtcs:set_conf(all, ~p)", [NameValuePairs]),
    [ set_conf(DevPath, NameValuePairs) || DevPath <- rtcs_dev:devpaths()],
    ok;
set_conf(Name, NameValuePairs) when Name =:= riak orelse
                                    Name =:= cs orelse
                                    Name =:= stanchion ->
    set_conf({Name, current}, NameValuePairs),
    ok;
set_conf({Name, Vsn}, NameValuePairs) ->
    lager:info("rtcs:set_conf({~p, ~p}, ~p)", [Name, Vsn, NameValuePairs]),
    set_conf(rtcs_dev:devpath(Name, Vsn), NameValuePairs),
    ok;
set_conf({Name, Vsn, N}, NameValuePairs) ->
    lager:info("rtcs:set_conf({~p, ~p, ~p}, ~p)", [Name, Vsn, N, NameValuePairs]),
    rtdev:append_to_conf_file(rtcs_dev:get_conf(rtcs_dev:devpath(Name, Vsn), N), NameValuePairs),
    ok;
set_conf(Node, NameValuePairs) when is_atom(Node) ->
    rtdev:append_to_conf_file(rtcs_dev:get_conf(Node), NameValuePairs),
    ok;
set_conf(DevPath, NameValuePairs) ->
    lager:info("rtcs:set_conf(~p, ~p)", [DevPath, NameValuePairs]),
    [rtdev:append_to_conf_file(RiakConf, NameValuePairs) || RiakConf <- rtcs_dev:all_the_files(DevPath, "etc/*.conf")],
    ok.

-spec set_advanced_conf(atom() | {atom(), atom()} | string(), [{string(), string()}]) -> ok.
set_advanced_conf(all, NameValuePairs) ->
    lager:info("rtcs:set_advanced_conf(all, ~p)", [NameValuePairs]),
    [ set_advanced_conf(DevPath, NameValuePairs) || DevPath <- rtcs_dev:devpaths()],
    ok;
set_advanced_conf(Name, NameValuePairs) when Name =:= riak orelse
                                             Name =:= cs orelse
                                             Name =:= stanchion ->
    set_advanced_conf({Name, current}, NameValuePairs),
    ok;
set_advanced_conf({Name, Vsn}, NameValuePairs) ->
    lager:info("rtcs:set_advanced_conf({~p, ~p}, ~p)", [Name, Vsn, NameValuePairs]),
    set_advanced_conf(rtcs_dev:devpath(Name, Vsn), NameValuePairs),
    ok;
set_advanced_conf({Name, Vsn, N}, NameValuePairs) ->
    lager:info("rtcs:set_advanced_conf({~p, ~p, ~p}, ~p)", [Name, Vsn, N, NameValuePairs]),
    rtcs_dev:update_app_config_file(rtcs_dev:get_app_config(rtcs_dev:devpath(Name, Vsn), N), NameValuePairs),
    ok;
set_advanced_conf(Node, NameValuePairs) when is_atom(Node) ->
    rtcs_dev:update_app_config_file(rtcs_dev:get_app_config(Node), NameValuePairs),
    ok;
set_advanced_conf(DevPath, NameValuePairs) ->
    AdvancedConfs = case rtcs_dev:all_the_files(DevPath, "etc/a*.config") of
                        [] ->
                            %% no advanced conf? But we _need_ them, so make 'em
                            rtdev:make_advanced_confs(DevPath);
                        Confs ->
                            Confs
                    end,
    lager:info("AdvancedConfs = ~p~n", [AdvancedConfs]),
    [rtcs_dev:update_app_config_file(RiakConf, NameValuePairs) || RiakConf <- AdvancedConfs],
    ok.

assert_error_log_empty(N) ->
    assert_error_log_empty(current, N).

assert_error_log_empty(Vsn, N) ->
    ErrorLog = rtcs_config:riakcs_logpath(rtcs_config:devpath(cs, Vsn), N, "error.log"),
    case file:read_file(ErrorLog) of
        {error, enoent} -> ok;
        {ok, <<>>} -> ok;
        {ok, Errors} ->
            lager:warning("Not empty error.log (~s): the first few lines are...~n~s",
                          [ErrorLog,
                           lists:map(
                             fun(L) -> io_lib:format("cs dev~p error.log: ~s\n", [N, L]) end,
                             lists:sublist(binary:split(Errors, <<"\n">>, [global]), 3))]),
            error(not_empty_error_log)
    end.

truncate_error_log(N) ->
    Cmd = os:find_executable("rm"),
    ErrorLog = rtcs_config:riakcs_logpath(rt_config:get(rtcs_config:cs_current()), N, "error.log"),
    ok = rtcs_exec:cmd(Cmd, [{args, ["-f", ErrorLog]}]).

wait_until(_, _, 0, _) ->
    fail;
wait_until(Fun, Condition, Retries, Delay) ->
    Result = Fun(),
    case Condition(Result) of
        true ->
            Result;
        false ->
            timer:sleep(Delay),
            wait_until(Fun, Condition, Retries-1, Delay)
    end.

%% Kind = objects | blocks | users | buckets ...
pbc(RiakNodes, ObjectKind, Opts) ->
    pbc(rt_config:get(flavor, basic), ObjectKind, RiakNodes, Opts).

pbc(basic, _ObjectKind, RiakNodes, _Opts) ->
    rt:pbc(hd(RiakNodes));
pbc({multibag, _} = Flavor, ObjectKind, RiakNodes, Opts) ->
    rtcs_bag:pbc(Flavor, ObjectKind, RiakNodes, Opts).

sha_mac(Key,STS) -> crypto:hmac(sha, Key,STS).
sha(Bin) -> crypto:hash(sha, Bin).
md5(Bin) -> crypto:hash(md5, Bin).


-spec datetime() -> cs_datetime().
%% @doc Returns the current time as a string in ISO-8601 format.
%%
%%  Output is formatted as by {@link datetime/1}.
%% @end
datetime() ->
    iso8601datetime(calendar:universal_time()).

-spec datetime(DateTime :: datetime()) -> cs_datetime() | dt_error().
%% @doc Convert a date/time value into a string in ISO 8601 format.
%%
%%  Output is formatted as the most compact version specified by ISO 8601:
%%  `YYYYMMDDTHHMMSSZ'.
%%
%%  Input may be either a {@link calendar:datetime()} representing UTC (as
%%  returned by {@link calendar:universal_time/0}) or a non-negative number
%%  of Gregorian seconds representing UTC as interpretted by the
%%  {@link calendar} module.
%%
%%  For convenience, this function can also be used to convert between
%%  formats, as if `rtcs:datetime(rtcs:datetime_to_ts(DateTime))' were
%%  invoked.
%%
%% @see datetime_to_ts/1
%% @end
datetime({{_,_,_},{_,_,_}} = DateTime) ->
    ?validated_timestamp_op(DateTime, iso8601datetime);

datetime(GregorianSecs) when ?gregorian_guard(GregorianSecs) ->
    iso8601datetime(calendar:gregorian_seconds_to_datetime(GregorianSecs));

datetime([_|_] = DateTime) ->
    case datetime_to_ts(DateTime) of
        {{_,_,_},{_,_,_}} = Result ->
            datetime(Result);
        {error, _} = Error ->
            Error;
        Other ->
            {error, Other}
    end.

-spec iso8601(EpochSecs :: epochsecs()) -> cs_datetime() | dt_error().
%% @doc Convert the number of seconds since the Unix epoch into a timestamp.
%%  Output is formatted as the most compact version specified by ISO 8601:
%%  `YYYYMMDDTHHMMSSZ'.
%% @see datetime/1
%% @end
iso8601(EpochSecs) when is_integer(EpochSecs) ->
    datetime(EpochSecs + (719528 * 86400)).

-spec datetime_to_gs(DateTime :: datetime()) -> gregorian() | dt_error().
%% @doc Convert a date/time string into Gregorian seconds.
%%
%%  Input is formatted as specified by ISO 8601 or RFC 2616. Note that all
%%  times are UTC, though RFC 2616 insists on the timezone 'GMT'.
%%
%%  Output is the number of Gregorian seconds representing the input as
%%  interpretted by the {@link calendar} module.
%%
%%  For convenience, input in the form of a {@link calendar:datetime()} is
%%  also accepted, functioning identically to
%%  {@link calendar:datetime_to_gregorian_seconds/1}.
%% @end
datetime_to_gs({{_,_,_},{_,_,_}} = DateTime) ->
    calendar:datetime_to_gregorian_seconds(DateTime);

datetime_to_gs(GregorianSecs) when ?gregorian_guard(GregorianSecs) ->
    GregorianSecs;

datetime_to_gs([_|_] = DateTime) ->
    case datetime_to_ts(DateTime) of
        {{_,_,_},{_,_,_}} = TS ->
            calendar:datetime_to_gregorian_seconds(TS);
        Error ->
            Error
    end.

-spec datetime_to_ts(DateTime :: datetime()) -> timestamp() | dt_error().
%% @doc Convert a date/time string into a {@link timestamp()} tuple.
%%
%%  Input is formatted as specified by ISO 8601 or RFC 2616. Note that all
%%  times are UTC, though RFC 2616 insists on the timezone 'GMT'.
%%
%%  Output is the time as represented by a `calendar:datetime()'.
%%
%%  For convenience, input in the form of a `calendar:datetime()' is
%%  also accepted, effectively returning the input unchanged ... though
%%  possibly validated.
%% @end
datetime_to_ts({{_,_,_},{_,_,_}} = DateTime) ->
    ?validated_timestamp(DateTime);

datetime_to_ts([_,_,_,_,_,_,_,_,$T,_,_,_,_,_,_,$Z] = DateTime) ->
    datetime_to_ts("~4d~2d~2dT~2d~2d~2dZ", DateTime);

datetime_to_ts([_,_,_,_,$-,_,_,$-,_,_,$T,_,_,$:,_,_,$:,_,_,$Z] = DateTime) ->
    datetime_to_ts("~4d-~2d-~2dT~2d:~2d:~2dZ", DateTime);

datetime_to_ts([_,_,_,_,$-,_,_,$-,_,_,$T,_,_,$:,_,_,$:,_,_,$+,$0,$0,$:,$0,$0]
        = DateTime) ->
    datetime_to_ts("~4d-~2d-~2dT~2d:~2d:~2d+00:00", DateTime);

datetime_to_ts([_|_] = DateTime) ->
    case httpd_util:convert_request_date(DateTime) of
        {{_,_,_},{_,_,_}} = Result ->
            Result;
        {error, _} = Error ->
            Error;
        Other ->
            {error, Other}
    end.

-spec valid_timestamp(timestamp()) -> boolean().
%% @doc Check whether the specified timestamp is valid.
%%  Input is a {@link calendar:datetime()}.  The result is `true' if all
%%  elements conform to the limits specified in the {@link calendar} module,
%%  `false' otherwise.
%% @see calendar:valid_date/1
%% @end
valid_timestamp({{_,_,_} = D, {H, M, S}}) ->
    calendar:valid_date(D)
    andalso erlang:is_integer(H)
    andalso erlang:is_integer(M)
    andalso erlang:is_integer(S)
    andalso H >= 0 andalso H < 24
    andalso M >= 0 andalso M < 60
    andalso S >= 0 andalso S < 60.

json_get(Key, Json) when is_binary(Key) ->
    json_get([Key], Json);
json_get([], Json) ->
    Json;
json_get([Key | Keys], {struct, JsonProps}) ->
    case lists:keyfind(Key, 1, JsonProps) of
        false ->
            notfound;
        {Key, Value} ->
            json_get(Keys, Value)
    end.

check_no_such_bucket(Response, Resource) ->
    check_error_response(Response,
                         404,
                         "NoSuchBucket",
                         "The specified bucket does not exist.",
                         Resource).

check_error_response({_, Status, _, RespStr} = _Response,
                     Status,
                     Code, Message, Resource) ->
    {RespXml, _} = xmerl_scan:string(RespStr),
    lists:all(error_child_element_verifier(Code, Message, Resource),
              RespXml#xmlElement.content).

error_child_element_verifier(Code, Message, Resource) ->
    fun(#xmlElement{name='Code', content=[Content]}) ->
            Content#xmlText.value =:= Code;
       (#xmlElement{name='Message', content=[Content]}) ->
            Content#xmlText.value =:= Message;
       (#xmlElement{name='Resource', content=[Content]}) ->
            Content#xmlText.value =:= Resource;
       (_) ->
            true
    end.

assert_versions(App, Nodes, Regexp) ->
    [begin
         {ok, Vsn} = rpc:call(N, application, get_key, [App, vsn]),
         lager:debug("~s's vsn at ~s: ~s", [App, N, Vsn]),
         {match, _} = re:run(Vsn, Regexp)
     end ||
        N <- Nodes].

reset_log(Node) ->
    {ok, _Logs} = rpc:call(Node, gen_event, delete_handler,
                           [lager_event, riak_test_lager_backend, normal]),
    ok = rpc:call(Node, gen_event, add_handler,
                  [lager_event, riak_test_lager_backend,
                   [rt_config:get(lager_level, info), false]]).

riak_node(N) ->
    ?DEV(N).

cs_node(N) ->
    ?CSDEV(N).

stanchion_node() ->
    'stanchion@127.0.0.1'.

maybe_load_intercepts(Node) ->
    case rt_intercept:are_intercepts_loaded(Node, [intercept_path()]) of
        false ->
            ok = rt_intercept:load_intercepts([Node], [intercept_path()]);
        true ->
            ok
    end.

%% private

riak_nodes(NumNodes) ->
    [?DEV(N) || N <- lists:seq(1, NumNodes)].

cs_nodes(NumNodes) ->
    [?CSDEV(N) || N <- lists:seq(1, NumNodes)].

node_list(NumNodes) ->
    NL0 = lists:zip(cs_nodes(NumNodes),
                    riak_nodes(NumNodes)),
    {CS1, R1} = hd(NL0),
    [{CS1, R1, stanchion_node()} | tl(NL0)].

intercept_path() ->
      filename:join([rtcs_dev:srcpath(cs_src_root),
                     "riak_test", "intercepts", "*.erl"]).

-spec datetime_to_ts(Format :: io:format(), DateTime :: datetime_str())
        -> timestamp() | dt_error().
%% @private
%% @doc Internal scanning function.
%%  Parses DateTime according to Format into a {@link timestamp()}. If the
%%  DateTime is not an exact match for the Format, an error is returned.
%%  Additional validations are also performed to assure the result is valid.
%%  If the result of this operation is not an error, the returned timestamp
%%  has been fully validated.
%%
%%  The Format MUST yield exactly six integers representin, in order, Year,
%%  Month, Day, Hour, Minute, and Second, which are validated against the
%%  constraints specified in the {@link calendar} module.
%% @end
datetime_to_ts([_|_] = Format, [_|_] = DateTime) ->
    case io_lib:fread(Format, DateTime) of
        {ok, [Year, Mon, Day, Hour, Min, Sec], []} ->
            TS = {{Year, Mon, Day}, {Hour, Min, Sec}},
            % don't trust the format to have specified integers, so perform
            % a complete validation, including types
            case valid_timestamp(TS) of
                true ->
                    TS;
                _ ->
                    {error, bad_date}
            end;
        {ok, [_,_,_,_,_,_], [_|_]} ->
            {error, {bad_date, extra_characters}};
        {more, _, _, _} ->
            {error, {bad_date, incomplete}};
        {error, _} = Error ->
            Error;
        Other ->
            {error, Other}
    end.

-spec iso8601datetime(DateTime :: timestamp()) -> iso8601ts() | dt_error().
%% @private
%% @doc Internal formatting function.
%%  Converts a {@link timestamp()} value into a string in ISO 8601 format.
%%  As long as the inputs are all integers, a result is rendered WITHOUT
%%  value validation!
%%  Output is formatted as the most compact version specified by ISO 8601:
%%  `YYYYMMDDTHHMMSSZ'.
%% @end
iso8601datetime({{YYYY, MM, DD}, {H ,M ,S}}) ->
    try
        lists:flatten(io_lib:format(
            "~4..0B~2..0B~2..0BT~2..0B~2..0B~2..0BZ",
            [YYYY, MM, DD, H, M, S]))
    catch
        error:Reason ->
            {error, Reason}
    end.
