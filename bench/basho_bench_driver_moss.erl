%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(basho_bench_driver_moss).

-export([new/1,
         run/4]).

-record(state, { client_id,
                 hosts}).

%% ====================================================================
%% API
%% ====================================================================

-spec new(integer()) -> {ok, term()}.
new(ID) ->
    application:start(ibrowse),

    %% The IP, port and path we'll be testing

    %% TODO:
    %% We'll start with a single
    %% IP for now
    Ip  = basho_bench_config:get(moss_raw_ip, "127.0.0.1"),
    Port = basho_bench_config:get(moss_raw_port, 8080),

    Hosts = [{Ip, Port}],

    {ok, #state{client_id=ID, hosts=Hosts}}.

-spec run(atom(), fun(), fun(), term()) -> {ok, term()}.
run(insert, KeyGen, ValueGen, State) ->
    {NextHost, S2} = next_host(State),
    ConnInfo = NextHost,
    case insert(KeyGen, ValueGen, ConnInfo) of
        ok ->
            {ok, S2};
        {Error, Reason} ->
            {Error, Reason, S2}
    end;
run(update, KeyGen, ValueGen, State) ->
    Bucket = "test",
    {NextHost, S2} = next_host(State),
    {Host, Port} = NextHost,
    Key = KeyGen(),
    Url = url(Host, Port, Bucket, Key),
    case do_get({Host, Port}, Url, []) of
        ok ->
            case insert(KeyGen, ValueGen, {Host, Port}) of
                ok ->
                    {ok, State};
                {Error, Reason} ->
                    {Error, Reason, S2}
            end;
        {Error, Reason} ->
            {Error, Reason, S2}
    end;
run(delete, KeyGen, _ValueGen, State) ->
    Bucket = "test",
    {NextHost, S2} = next_host(State),
    {Host, Port} = NextHost,
    Key = KeyGen(),
    Url = url(Host, Port, Bucket, Key),
    case do_delete({Host, Port}, Url, []) of
        ok ->
            {ok, S2};
        {Error, Reason} ->
            {Error, Reason, S2}
    end;
run(get, KeyGen, _ValueGen, State) ->
    Bucket = "test",
    {NextHost, S2} = next_host(State),
    {Host, Port} = NextHost,
    Key = KeyGen(),
    Url = url(Host, Port, Bucket, Key),
    case do_get({Host, Port}, Url, []) of
        ok ->
            {ok, S2};
        {Error, Reason} ->
            {Error, Reason, S2}
    end;
run(_Operation, _KeyGen, _ValueGen, State) ->
    {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

insert(KeyGen, ValueGen, {Host, Port}) ->
    %% TODO:
    %% bucket needs to be
    %% configurable/generatable
    Bucket = "test",
    Key = KeyGen(),
    Url = url(Host, Port, Bucket, Key),
    Value = ValueGen(),
    do_put({Host, Port}, Url, [], Value).

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
send_request(Host, Url, Headers, Method, Body, Options, Count) ->
    Pid = connect(Host),
    HeadersWithAuth = [{'Authorization', basho_bench_config:get(moss_authorization)}|Headers],
    case catch(ibrowse_http_client:send_req(Pid, Url, HeadersWithAuth, Method, Body, Options, basho_bench_config:get(moss_request_timeout, 5000))) of
        {ok, Status, RespHeaders, RespBody} ->
            maybe_disconnect(Host),
            {ok, Status, RespHeaders, RespBody};

        Error ->
            clear_disconnect_freq(Host),
            disconnect(Host),
            case should_retry(Error) of
                true ->
                    send_request(Host, Url, Headers, Method, Body, Options, Count-1);

                false ->
                    normalize_error(Method, Error)
            end
    end.

do_put(Host, Url, Headers, Value) ->
    case send_request(Host, Url, Headers ++ [{'Content-Type', 'application/octet-stream'}],
                      put, Value, [{response_format, binary}]) of
        {ok, "201", _Header, _Body} ->
            ok;
        {ok, "204", _Header, _Body} ->
            ok;
        {ok, Code, _Header, _Body} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            {error, Reason}
    end.

do_delete(Host, Url, Headers) ->
    case send_request(Host, Url, Headers,
                      delete, <<>>, [{response_format, binary}]) of
        {ok, "200", _Header, _Body} ->
            ok;
        {ok, "204", _Header, _Body} ->
            ok;
        {ok, Code, _Header, _Body} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            {error, Reason}
    end.

do_get(Host, Url, Headers) ->
    case send_request(Host, Url, Headers,
                      get, <<>>, [{response_format, binary}]) of
        {ok, "200", _Header, _Body} ->
            ok;
        {ok, "404", _Header, _Body} ->
            ok;
        {ok, Code, _Header, _Body} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            {error, Reason}
    end.

should_retry({error, send_failed})       -> true;
should_retry({error, connection_closed}) -> true;
should_retry({'EXIT', {normal, _}})      -> true;
should_retry({'EXIT', {noproc, _}})      -> true;
should_retry(_)                          -> false.

normalize_error(Method, {'EXIT', {timeout, _}})  -> {error, {Method, timeout}};
normalize_error(Method, {'EXIT', Reason})        -> {error, {Method, 'EXIT', Reason}};
normalize_error(Method, {error, Reason})         -> {error, {Method, Reason}}.
