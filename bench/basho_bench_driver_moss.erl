%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%%
%% Please note that the in order
%% to run basho_bench_driver_moss
%% `auth_bypass` must be set to true
%% on the riak_moss app.config

-module(basho_bench_driver_moss).

-define(BLOCK, (1024*1024)).
-define(VERYLONG_TIMEOUT, (300*1000)).

-export([new/1,
         run/4]).

-record(state, { client_id,
                 hosts,
                 bucket,
                 report_fun,
                 working_op,
                 req_id
               }).

%% ====================================================================
%% API
%% ====================================================================

-spec new(integer()) -> {ok, term()}.
new(ID) ->
    application:start(ibrowse),
    application:start(crypto),

    %% The IP, port and path we'll be testing

    %% TODO:
    %% We'll start with a single
    %% IP for now
    Ip  = basho_bench_config:get(moss_raw_ip, "127.0.0.1"),
    Port = basho_bench_config:get(moss_raw_port, 8080),
    Disconnect = basho_bench_config:get(moss_disconnect_frequency, infinity),
    erlang:put(disconnect_freq, Disconnect),

    %% Get our measurement units: op/sec, Byte/sec, KByte/sec, MByte/sec
    {RF_name, ReportFun} =
        %% We need to be really careful with these custom units things.
        %% Use floats for everything.
        case (catch basho_bench_config:get(moss_measurement_units)) of
            N = byte_sec  -> {N,      fun(X) -> X / 1 end};
            N = kbyte_sec -> {N,      fun(X) -> X / 1024 end};
            N = mbyte_sec -> {N,      fun(X) -> X / (1024 * 1024) end};
            _             -> {op_sec, fun(_) -> 1.0 end}
        end,
    if ID == 1 ->
            basho_bench_log:log(info, "Reporting factor = ~p\n", [RF_name]);
       true ->
            ok
    end,
    OpsList = basho_bench_config:get(operations, []),
    case RF_name /= op_sec andalso
         lists:keymember(delete, 1, OpsList) andalso
         length(OpsList) > 1 of
        true ->
            basho_bench_log:log(
              warn,
              "Mixing delete and non-delete operations together with "
              "~p measurements unit can yield nonsense results!\n\n",
              [RF_name]);
        false ->
            ok
    end,

    Hosts = [{Ip, Port}],

    {ok, #state{client_id=ID, hosts=Hosts,
                bucket = basho_bench_config:get(moss_bucket, "test"),
                report_fun = ReportFun}}.

%% This module does some crazy stuff, but it's there for a reason.
%% The reason is that basho_bench is expecting the run() function to
%% do something that takes a few/several seconds at most.  It is not
%% expeciting run() to finish after minutes/hours/days of runtime,
%% which is quite possible when testing 5GB files or larger.
%%
%% So, we adopt a strategy where run() will return after ?BLOCK of
%% data received/sent.  That will allow R to create graphs of the
%% latency of ?BLOCK chunk retrievals.  We will do our own throughput
%% reporting to allow R to graph total throughput in terms of
%% KByte/sec or MByte/sec -- we do it via a kludge, but at least it
%% works.
%%
%% This scheme will also allow b_b's built-in rate-limiting scheme to
%% work, though on a granularity of ?BLOCK increments per second,
%% which is probably still too sucky and may need finer control.

-spec run(atom(), fun(), fun(), term()) -> {{ok, number()}, term()} | {error, term(), term()}.

run(get, KeyGen, _ValueGen, #state{working_op = undefined,
                                   bucket = Bucket} = State) ->
    case get(qwerty) of undefined -> timer:sleep(State#state.client_id * 25), io:format("x"); _ -> ok end,
    put(qwerty, yo_keep_going),
    {NextHost, S2} = next_host(State),
    {Host, Port} = NextHost,
    Key = KeyGen(),
    Url = url(Host, Port, Bucket, Key),
    case do_get_first_unit({Host, Port}, Url, [], S2) of
        {ok, S3} ->
            {{silent, ok}, S3};
        Else ->
            Else
    end;
run(_, _KeyGen, _ValueGen, #state{working_op = get} = State) ->
    case do_get_loop(State) of
        {ok, S2} ->
            {{silent, ok}, S2};
        Else ->
            Else
    end;
run(Op, KeyGen, ValueGen, State) ->
    run2(Op, KeyGen, ValueGen, State).

run2(insert, KeyGen, ValueGen, #state{bucket = Bucket,
                                      report_fun = ReportFun} = State) ->
    {NextHost, S2} = next_host(State),
    ConnInfo = NextHost,
    case insert(KeyGen, ValueGen, ConnInfo, Bucket, ReportFun) of
        {ok, _Size} = Good ->
            {Good, S2};
        {error, Reason} ->
            {error, Reason, S2}
    end;
run2(delete, KeyGen, _ValueGen, #state{bucket = Bucket} = State) ->
    {NextHost, S2} = next_host(State),
    {Host, Port} = NextHost,
    Key = KeyGen(),
    Url = url(Host, Port, Bucket, Key),
    case do_delete({Host, Port}, Url, []) of
        ok ->
            {ok, S2};
        {error, Reason} ->
            {error, Reason, S2}
    end;
run2(Op, _KeyGen, _ValueGen, State) ->
    {error, {unknown_op, Op}, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

insert(KeyGen, ValueGen, {Host, Port}, Bucket, ReportFun) ->
    Key = KeyGen(),
    Url = url(Host, Port, Bucket, Key),
    Value = ValueGen(),
    do_put({Host, Port}, Url, [], Value, ReportFun).

url(Host, Port, Bucket, Key) ->
    UnparsedUrl = lists:concat(["http://", Host, ":", Port, "/", Bucket, "/", Key]),
    Url = ibrowse_lib:parse_url(UnparsedUrl),
    Url.

-spec next_host(term()) -> {term(), term()}.
%% TODO:
%% Currently we only support
%% one host, so just return
%% that
next_host(State=#state{hosts=Hosts}) ->
    [Host] = Hosts,
    {Host, State}.

%% This is ripped from basho_bench_driver_http_raw.erl
connect(Url={Host, Port}) ->
    case erlang:get({ibrowse_pid, Url}) of
        undefined ->
            {ok, Pid} = ibrowse_http_client:start({Host, Port}),
            erlang:put({ibrowse_pid, Url}, Pid),
            Pid;
        Pid ->
            case is_process_alive(Pid) of
                true ->
                    Pid;
                false ->
                    erlang:erase({ibrowse_pid, Url}),
                    connect(Url)
            end
    end.


%% This is ripped from basho_bench_driver_http_raw.erl
disconnect(Url) ->
    case erlang:get({ibrowse_pid, Url}) of
        undefined ->
            ok;
        OldPid ->
            catch(ibrowse_http_client:stop(OldPid))
    end,
    erlang:erase({ibrowse_pid, Url}),
    ok.

maybe_disconnect(Url) ->
    case erlang:get(disconnect_freq) of
        infinity -> ok;
        {ops, Count} -> should_disconnect_ops(Count,Url) andalso disconnect(Url);
        Seconds -> should_disconnect_secs(Seconds,Url) andalso disconnect(Url)
    end.

should_disconnect_ops(Count, {Host, Port}) ->
    Key = {ops_since_disconnect, {Host, Port}},
    case erlang:get(Key) of
        undefined ->
            erlang:put(Key, 1),
            false;
        Count ->
            erlang:put(Key, 0),
            true;
        Incr ->
            erlang:put(Key, Incr + 1),
            false
    end.

should_disconnect_secs(Seconds, {Host, Port}) ->
    Key = {last_disconnect, {Host, Port}},
    case erlang:get(Key) of
        undefined ->
            erlang:put(Key, erlang:now()),
            false;
        Time when is_tuple(Time) andalso size(Time) == 3 ->
            Diff = timer:now_diff(erlang:now(), Time),
            if
                Diff >= Seconds * 1000000 ->
                    erlang:put(Key, erlang:now()),
                    true;
                true -> false
            end
    end.

clear_disconnect_freq(ConnInfo) ->
    case erlang:get(disconnect_freq) of
        infinity -> ok;
        {ops, _Count} -> erlang:put({ops_since_disconnect, ConnInfo}, 0);
        _Seconds -> erlang:put({last_disconnect, ConnInfo}, erlang:now())
    end.

send_request(Host, Url, Headers, Method, Body, Options) ->
    send_request(Host, Url, Headers, Method, Body, Options, 3).

send_request(_Host, _Url, _Headers, _Method, _Body, _Options, 0) ->
    {error, max_retries};
send_request(Host, Url, Headers0, Method, Body, Options, Count) ->
    case (catch initiate_request(Host, Url, Headers0, Method, Body, Options)) of
        {ok, Status, RespHeaders, RespBody} ->
            maybe_disconnect(Host),
            {ok, Status, RespHeaders, RespBody};

        Error ->
            clear_disconnect_freq(Host),
            disconnect(Host),
            case should_retry(Error) of
                true ->
                    send_request(Host, Url, Headers0, Method, Body, Options,
                                 Count-1);
                false ->
                    normalize_error(Method, Error)
            end
    end.

do_put(Host, Url, Headers, Value, ReportFun) ->
    case send_request(Host, Url, Headers ++ [{'Content-Type', 'application/octet-stream'}],
                      put, Value, proxy_opts()) of
        {ok, "200", _Header, _Body} ->
            {ok, ReportFun(byte_size(Value))};
        {ok, "201", _Header, _Body} ->
            {ok, ReportFun(byte_size(Value))};
        {ok, "204", _Header, _Body} ->
            {ok, ReportFun(byte_size(Value))};
        {ok, Code, _Header, _Body} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            {error, Reason}
    end.

do_delete(Host, Url, Headers) ->
    case send_request(Host, Url, Headers, delete, <<>>, proxy_opts()) of
        {ok, "200", _Header, _Body} ->
            ok;
        {ok, "204", _Header, _Body} ->
            ok;
        {ok, Code, _Header, _Body} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            {error, Reason}
    end.

do_get_first_unit(Host, Url, Headers, State) ->
    BufSize = 128*1024,
    Opts = [{max_pipeline_size, 9999999},
            {socket_options, [{recbuf, BufSize}]},
            {stream_chunk_size, BufSize},
            {stream_to, {self(), once}},
            {response_format, binary},
            {connect_timeout, 300*1000},
            {inactivity_timeout, 300*1000}],
    case initiate_request(Host, Url, Headers, get, <<>>, Opts++proxy_opts()) of
        {ibrowse_req_id, ReqId} ->
            receive
                {ibrowse_async_headers, ReqId, HTTPCode, _}
                  when HTTPCode == "200"; HTTPCode == "404" ->
                    do_get_loop(State#state{req_id = ReqId});
                {ibrowse_async_headers, ReqId, HTTPCode, _} ->
                    {error, {http_error, HTTPCode},
                     State#state{working_op = undefined}}
                %% Shouldn't happen at this time?
                %% {ibrowse_async_response, ReqId, Bin} ->
                %%     size(B);
                %% Shouldn't happen at this time?
                %% {ibrowse_async_response_end, ReqId} ->
                %%     all_done
            after ?VERYLONG_TIMEOUT ->
                    {error, ibrowse_timed_out,
                     State#state{working_op = undefined}}
            end;
        Else ->
            {error, Else, State#state{working_op = undefined}}
    end.

do_get_loop(#state{req_id = ReqId} = State) ->
    ibrowse:stream_next(ReqId),
    do_get_loop(0, now(), State).

do_get_loop(Sum, StartT,
            #state{req_id = ReqId, report_fun = ReportFun} = State) ->
    receive
        {ibrowse_async_response, ReqId, Bin} ->
            ibrowse:stream_next(ReqId),
            NewSum = Sum + size(Bin),
            if NewSum > ?BLOCK ->
                    DiffT = timer:now_diff(now(), StartT),
                    basho_bench_stats:op_complete(
                      {get,get}, {ok, ReportFun(NewSum)}, DiffT),
                    {ok, State#state{working_op = get}};
               true ->
                    do_get_loop(NewSum, StartT, State)
            end;
        {ibrowse_async_response_end, ReqId} ->
    %%%%%record extra stats ehere!!!!!!!!!!!!!
            DiffT = timer:now_diff(now(), StartT),
            basho_bench_stats:op_complete(
              {get,get}, {ok, ReportFun(Sum)}, DiffT),
            {ok, State#state{working_op = undefined}}
    after ?VERYLONG_TIMEOUT ->
            {error, do_get_loop_timed_out, State#state{working_op = undefined}}
    end.

should_retry({error, send_failed})       -> true;
should_retry({error, connection_closed}) -> true;
should_retry({'EXIT', {normal, _}})      -> true;
should_retry({'EXIT', {noproc, _}})      -> true;
should_retry(_)                          -> false.

normalize_error(Method, {'EXIT', {timeout, _}})  -> {error, {Method, timeout}};
normalize_error(Method, {'EXIT', Reason})        -> {error, {Method, 'EXIT', Reason}};
normalize_error(Method, {error, Reason})         -> {error, {Method, Reason}}.

proxy_opts() ->
    [{response_format, binary},
     {proxy_host, basho_bench_config:get(moss_http_proxy_host)},
     {proxy_port, basho_bench_config:get(moss_http_proxy_port)}].

uppercase_verb(put) ->
    'PUT';
uppercase_verb(get) ->
    'GET';
uppercase_verb(delete) ->
    'DELETE'.

to_list(A) when is_atom(A) ->
    atom_to_list(A);
to_list(L) when is_list(L) ->
    L.

initiate_request(Host, Url, Headers0, Method, Body, Options) ->
    Pid = connect(Host),
    ContentTypeStr = to_list(proplists:get_value(
                               'Content-Type', Headers0,
                               'application/octet-stream')),
    Date = httpd_util:rfc1123_date(),
    Headers = [{'Content-Type', ContentTypeStr},
               {'Date', Date}|lists:keydelete('Content-Type', 1, Headers0)],
    Uri = element(7, Url),
    Sig = stanchion_auth:request_signature(
            uppercase_verb(Method), Headers, Uri,
            basho_bench_config:get(moss_secret_key)),
    AuthStr = ["AWS ", basho_bench_config:get(moss_access_key), ":", Sig],
    HeadersWithAuth = [{'Authorization', AuthStr}|Headers],
    Timeout = basho_bench_config:get(moss_request_timeout, 5000),
    ibrowse_http_client:send_req(Pid, Url, HeadersWithAuth, Method,
                                 Body, Options, Timeout).
