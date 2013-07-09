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

%% @doc Quickcheck test module for `erlcloud' S3 API interaction with Riak CS.

-module(erlcloud_eqc).

-include("riak_cs.hrl").
-include("riak_cs_gc_d.hrl").

-include_lib("erlcloud/include/erlcloud_aws.hrl").

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

%% eqc properties
-export([prop_api_test/4
         %% prop_parallel_api_test/4
        ]).

%% States
-export([start/1,
         user_created/1,
         bucket_created/1,
         bucket_deleted/1,
         object_created/1,
         object_deleted/1]).

%% eqc_fsm callbacks
-export([initial_state/0,
         initial_state_data/0,
         initial_state_data/4,
         next_state_data/5,
         precondition/4,
         postcondition/5]).

%% Helpers
-export([test/0,
         test/2,
         test/5,
         create_user/3,
         user_name/0,
         user_email/0]).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).
-define(TEST_ITERATIONS, 100).

-define(P(EXPR), PPP = (EXPR), case PPP of true -> ok; _ -> io:format(user, "PPP ~p at line ~p\n", [PPP, ?LINE]) end, PPP).

-define(S3_MODULE, erlcloud_s3).
-define(DEFAULT_HOST, "s3.amazonaws.com").
-define(DEFAULT_PORT, 80).
-define(DEFAULT_PROXY_HOST, "localhost").
-define(DEFAULT_PROXY_PORT, 8080).
-define(VALUE, "test value").

-record(api_state, {aws_config :: aws_config(),
                    bucket :: string(),
                    keys :: [string()]}).

%%====================================================================
%% Eunit tests
%%====================================================================

eqc_test_() ->
    {spawn,
     [
      {timeout, 600, ?_assertEqual(true,
                                   quickcheck(numtests(?TEST_ITERATIONS,
                                                       ?QC_OUT(prop_api_test(?DEFAULT_HOST,
                                                                             ?DEFAULT_PORT,
                                                                             ?DEFAULT_PROXY_HOST,
                                                                             cs_port())))))}
      %% {timeout, 60, ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_parallel_api_test(?DEFAULT_HOST, ?DEFAULT_PORT)))))}
     ]
    }.

%% ====================================================================
%% EQC Properties
%% ====================================================================

prop_api_test(Host, Port, ProxyHost, ProxyPort) ->
    ?FORALL(Cmds,
            eqc_gen:noshrink(commands(?MODULE, {start, initial_state_data(Host, Port, ProxyHost, ProxyPort)})),
            begin
                RunningApps = application:which_applications(),
                case lists:keymember(erlcloud, 1, RunningApps) of
                    true ->
                        ok;
                    false ->
                        erlcloud:start(),
                        timer:sleep(200),
                        ok
                end,
                {H, {_F, _S}, Res} = run_commands(?MODULE, Cmds),
                %% application:stop(erlcloud),

                aggregate(zip(state_names(H), command_names(Cmds)),
                          ?WHENFAIL(
                             begin
                                 ?debugFmt("Cmds: ~p~n",
                                           [zip(state_names(H),
                                                command_names(Cmds))]),
                                 ?debugFmt("Result: ~p~n", [Res]),
                                 ?debugFmt("History: ~p~n", [H])
                             end,
                             equals(ok, Res)))
            end
           ).

%% prop_parallel_api_test(Host, Port, ProxyHost, ProxyPort) ->
%%     ?FORALL(Cmds={Seq, Par},
%%             parallel_commands(?MODULE, {start, initial_state_data(Host, Port)}),
%%             begin
%%                 erlcloud:start(),
%%                 {H, _ParH, Res} = run_parallel_commands(?MODULE, {Seq, Par}),
%%                 %% aggregate(zip(state_names(H), command_names(Cmds)),
%%                 aggregate(command_names(Cmds),
%%                           ?WHENFAIL(
%%                              begin
%%                                  ?debugFmt("Cmds: ~p~n",
%%                                            [zip(state_names(H),
%%                                                 command_names(Cmds))]),
%%                                  ?debugFmt("Result: ~p~n", [Res]),
%%                                  ?debugFmt("History: ~p~n", [H])
%%                              end,
%%                              equals(ok, Res)))
%%             end
%%            ).

%%====================================================================
%% eqc_fsm callbacks
%%====================================================================

start(#api_state{aws_config=Config}) ->
    [
     {user_created, {call, ?MODULE, create_user, [user_name(), user_email(), Config]}}
    ].

user_created(#api_state{aws_config=Config}) ->
    [
     {history, {call, ?S3_MODULE, list_buckets, [Config]}},
     {bucket_created, {call, ?S3_MODULE, create_bucket, [bucket(), Config]}}
    ].

bucket_created(#api_state{aws_config=Config,
                          bucket=Bucket}) ->
    [
     {history, {call, ?S3_MODULE, list_buckets, [Config]}},
     {history, {call, ?S3_MODULE, list_objects, [Bucket, Config]}},
     {object_created, {call, ?S3_MODULE, put_object, [Bucket, key(), ?VALUE, Config]}},
     {bucket_deleted, {call, ?S3_MODULE, delete_bucket, [Bucket, Config]}}
    ].

bucket_deleted(#api_state{aws_config=Config}) ->
    [
     {history, {call, ?S3_MODULE, list_buckets, [Config]}},
     {bucket_created, {call, ?S3_MODULE, create_bucket, [bucket(), Config]}}
    ].

object_created(#api_state{aws_config=Config,
                          bucket=Bucket,
                          keys=[Key | _]}) ->
    [
     {history, {call, ?MODULE, get_object, [Bucket, Key, Config]}},
     {history, {call, ?S3_MODULE, list_objects, [Bucket, Config]}},
     {object_deleted, {call, ?S3_MODULE, delete_object, [Bucket, Key, Config]}}
    ].

object_deleted(#api_state{aws_config=Config,
                          bucket=Bucket,
                          keys=[Key | _]}) ->
    [
     {history, {call, ?S3_MODULE, list_objects, [Bucket, Config]}},
     {bucket_created, {call, ?MODULE, get_object, [Bucket, Key, Config]}}
    ].

initial_state() ->
    start.

initial_state_data() ->
    #api_state{}.

initial_state_data(Host, Port, ProxyHost, ProxyPort) ->
    #api_state{aws_config=#aws_config{s3_host=Host,
                                      s3_port=Port,
                                      s3_prot="http",
                                      http_options=[{proxy_host, ProxyHost},
                                                    {proxy_port, ProxyPort}]}}.

next_state_data(start, user_created, S, AwsConfig, _C) ->
    S#api_state{aws_config=AwsConfig};
next_state_data(user_created, bucket_created, S, _R, {call, ?S3_MODULE, create_bucket, [Bucket, _]}) ->
    S#api_state{bucket=Bucket};
next_state_data(bucket_created, bucket_deleted, S, _R, {call, ?S3_MODULE, delete_bucket, [_, _]}) ->
    S#api_state{bucket=undefined};
next_state_data(bucket_deleted, bucket_created, S, _R, {call, ?S3_MODULE, create_bucket, [Bucket, _]}) ->
    S#api_state{bucket=Bucket};
next_state_data(object_deleted, bucket_created, S, _R, {call, ?MODULE, get_object, [_, _, _]}) ->
    [_ | RestKeys] =  S#api_state.keys,
    S#api_state{keys=RestKeys};
next_state_data(bucket_created, object_created, S, _R, {call, ?S3_MODULE, put_object, [_, Key, _, _]}) ->
    Keys = update_keys(Key, S#api_state.keys),
    S#api_state{keys=Keys};
next_state_data(object_created, object_deleted, S, _R, {call, ?S3_MODULE, delete_object, [_, _, _]}) ->
    S;
next_state_data(_From, _To, S, _R, _C) ->
    S.

precondition(bucket_created, bucket_deleted, #api_state{keys=undefined}, _C) ->
    true;
precondition(bucket_created, bucket_deleted, #api_state{keys=[]}, _C) ->
    true;
precondition(bucket_created, bucket_deleted, _S, _C) ->
    false;
precondition(_From, _To, _S, _C) ->
    true.

postcondition(start, user_created, _S, _C, {error, _}) ->
    ?P(false);
postcondition(start, user_created, _S, _C, Config) ->
    ?P(is_record(Config, aws_config));
postcondition(user_created, user_created, _S, _C, [{buckets, []}]) ->
    true;
postcondition(user_created, bucket_created, _S, _C, ok) ->
    true;
postcondition(user_created, bucket_created, _S, _C, _) ->
    ?P(false);
postcondition(bucket_created, bucket_created, #api_state{bucket=Bucket}, {call, _, list_buckets, _}, [{buckets, [Bucket]}]) ->
    true;
postcondition(bucket_created, bucket_created, #api_state{bucket=Bucket}, {call, _, list_objects, _}, R) ->
    ?P(is_empty_object_list(Bucket, R));
postcondition(bucket_created, bucket_deleted, _S, _C, ok) ->
    true;
postcondition(bucket_created, bucket_deleted, _S, _C, _) ->
    ?P(false);
postcondition(bucket_created, object_created, _S, _C, [{version_id, _}]) ->
    true;
postcondition(bucket_created, object_created, _S, _C, _) ->
    ?P(false);
postcondition(object_created, object_created, _S, {call, _, get_object, _}, R) ->
    ContentLength = proplists:get_value(content_length, R),
    Content = proplists:get_value(content, R),
    ContentLength =:= "10" andalso Content =:= <<"test value">>;
postcondition(object_created, object_created, #api_state{bucket=Bucket, keys=Keys}, {call, _, list_objects, _}, R) ->
    Name = proplists:get_value(name, R),
    Contents = proplists:get_value(contents, R),
    ?P(Name =:= Bucket andalso verify_object_list_contents(Keys, Contents));
postcondition(object_created, object_deleted, _S, _C, [{delete_marker, _}, {version_id, _}]) ->
    true;
postcondition(object_deleted, object_deleted, #api_state{keys=[Key | _]}, _C, R) ->
    Contents = proplists:get_value(contents, R, []),
    ?P(not lists:member(Key, Contents));
postcondition(object_created, object_deleted, _S, _C, _) ->
    ?P(false);
%% Catch all
postcondition(_From, _To, _S, _C, _R) ->
    true.

%%====================================================================
%% Helpers
%%====================================================================

test() ->
    test(?DEFAULT_HOST, ?DEFAULT_PORT).

test(Host, Port) ->
    test(Host, Port, ?DEFAULT_PROXY_HOST, cs_port(), 500).

test(Host, Port, ProxyHost, ProxyPort, Iterations) ->
    eqc:quickcheck(eqc:numtests(Iterations, prop_api_test(Host, Port, ProxyHost, ProxyPort))).
    %% eqc:quickcheck(eqc:numtests(Iterations, prop_parallel_api_test(Host, Port, ProxyHost, ProxyPort))).

create_user(Name, Email, Config) ->
    process_post(
      post_user_request(
        compose_url(Config),
        compose_request(Name, Email)),
     Config).

get_object(Bucket, Key, Config) ->
    try
        ?S3_MODULE:get_object(Bucket, Key, Config)
    catch _:_ ->
            {error, not_found}
    end.

post_user_request(Url, RequestDoc) ->
    Request = {Url, [], "application/json", RequestDoc},
    httpc:request(post, Request, [], []).

process_post({ok, {{_, 201, _}, _RespHeaders, RespBody}}, Config) ->
    User = json_to_user_record(mochijson2:decode(RespBody)),
    Config#aws_config{access_key_id=User?RCS_USER.key_id,
                      secret_access_key=User?RCS_USER.key_secret};
process_post(Error, _) ->
    Error.

json_to_user_record({struct, UserItems}) ->
    lists:foldl(fun item_to_record_field/2, ?RCS_USER{}, UserItems);
json_to_user_record(_) ->
    {error, received_invalid_json}.

item_to_record_field({<<"email">>, Email}, User) ->
    User?RCS_USER{email=binary_to_list(Email)};
item_to_record_field({<<"name">>, Name}, User) ->
    User?RCS_USER{name=binary_to_list(Name)};
item_to_record_field({<<"display_name">>, DispName}, User) ->
    User?RCS_USER{display_name=binary_to_list(DispName)};
item_to_record_field({<<"id">>, Id}, User) ->
    User?RCS_USER{canonical_id=binary_to_list(Id)};
item_to_record_field({<<"key_id">>, KeyId}, User) ->
    User?RCS_USER{key_id=binary_to_list(KeyId)};
item_to_record_field({<<"key_secret">>, KeySecret}, User) ->
    User?RCS_USER{key_secret=binary_to_list(KeySecret)};
item_to_record_field(_, User) ->
    User.

compose_url(#aws_config{http_options=HTTPOptions}) ->
    {_, Host} = lists:keyfind(proxy_host, 1, HTTPOptions),
    {_, Port} = lists:keyfind(proxy_port, 1, HTTPOptions),
    compose_url(Host, Port).

compose_url(Host, Port) ->
    lists:flatten(["http://", Host, ":", integer_to_list(Port), "/riak-cs/user"]).

compose_request(Name, Email) ->
    binary_to_list(
      iolist_to_binary(
        mochijson2:encode({struct, [{<<"email">>, list_to_binary(Email)},
                                    {<<"name">>, list_to_binary(Name)}]}))).

user_name() ->
    ?LET(X, timestamp(), X).

user_email() ->
    ?LET(X, timestamp(), lists:flatten([X, $@, "me.com"])).

bucket() ->
    ?LET(X, timestamp(), X).

key() ->
    ?LET(X, timestamp(), X).

%% @doc Generator for strings that need to be unique within the VM
timestamp() ->
    {MegaSecs, Secs, MicroSecs} = erlang:now(),
    to_list(MegaSecs) ++ to_list(Secs) ++ to_list(MicroSecs).

to_list(X) when X < 10 ->
    lists:flatten([$0, X]);
to_list(X) ->
    integer_to_list(X).

update_keys(Key, undefined) ->
    [Key];
update_keys(Key, ExistingKeys) ->
    [Key | ExistingKeys].

is_empty_object_list(Bucket, ObjectList) ->
    Bucket =:= proplists:get_value(name, ObjectList)
        andalso [] =:= proplists:get_value(contents, ObjectList).

verify_object_list_contents([], []) ->
    true;
verify_object_list_contents(_, []) ->
    false;
verify_object_list_contents(ExpectedKeys, [HeadContent | RestContents]) ->
    Key = proplists:get_value(key, HeadContent),
    verify_object_list_contents(lists:delete(Key, ExpectedKeys), RestContents).

cs_port() ->
    case os:getenv("CS_HTTP_PORT") of
        false ->
            ?DEFAULT_PROXY_PORT;
        Str ->
            list_to_integer(Str)
    end.

-endif.
