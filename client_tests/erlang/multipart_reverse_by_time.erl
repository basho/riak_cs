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

-module(multipart_reverse_by_time).

-include("riak_cs.hrl").
-include("riak_cs_gc_d.hrl").

-include_lib("erlcloud/include/erlcloud_aws.hrl").

-compile(export_all).

-define(S3_MODULE, erlcloud_s3).
-define(S3_MULTI, erlcloud_s3_multipart).
-define(DEFAULT_HOST, "s3.amazonaws.com").
-define(DEFAULT_PORT, 80).
-define(DEFAULT_PROXY_HOST, "localhost").
-define(DEFAULT_PROXY_PORT, 8080).
-define(VALUE, "test value").

reverse_test() ->
    RunningApps = application:which_applications(),
    case lists:keymember(erlcloud, 1, RunningApps) of
        true ->
            ok;
        false ->
            erlcloud:start(),
            timer:sleep(200),
            ok
    end,
    Config = #aws_config{s3_host=?DEFAULT_HOST,
                         s3_port=?DEFAULT_PORT,
                         s3_prot="http",
                         http_options=[{proxy_host, ?DEFAULT_PROXY_HOST},
                                       {proxy_port, cs_port()}]},
    %% create_user("foo", "foo", Config).          % 400 Bad Request
    C2 = create_user(user_name(), user_email(), Config),
    Bucket = bucket(),
    io:format("Bucket: ~s\n", [Bucket]),
    ok = ?S3_MODULE:create_bucket(Bucket, C2),
    File1 = key(),
    V1 = <<"AAAAAAAAAA">>,
    V2 = <<"bbbbbbbbbb">>,
    V3 = <<"CCCCCCCCCC">>,
    ExpectedV = list_to_binary([V1, V2, V3]),
    Num1 = 5,
    Num2 = 50,
    Num3 = 500,
    X1 = ?S3_MULTI:initiate_upload(Bucket, File1, "text/plain", [], C2),
    UploadId = ?S3_MULTI:upload_id(X1),

    %% Upload the parts in the reverse order *timewise* from their part
    %% numbers, using timer:sleep() to rearrange for us.
    Parent = self(),
    P1 = spawn_link(fun() ->
                            timer:sleep(500),
                            {R2, _} = ?S3_MULTI:upload_part(Bucket, File1, UploadId, Num1, V1, C2),
                            Tag1 = proplists:get_value("ETag", R2),
                            Parent ! {self(), Tag1},
                            exit(normal)
                    end),
    P2 = spawn_link(fun() ->
                            timer:sleep(200),
                            {R3, _} = ?S3_MULTI:upload_part(Bucket, File1, UploadId, Num2, V2, C2),
                            Tag2 = proplists:get_value("ETag", R3),
                            Parent ! {self(), Tag2},
                            exit(normal)
                    end),
    P3 = spawn_link(fun() ->
                            timer:sleep(1),
                            {R4, _} = ?S3_MULTI:upload_part(Bucket, File1, UploadId, Num3, V3, C2),
                            Tag3 = proplists:get_value("ETag", R4),
                            Parent ! {self(), Tag3},
                            exit(normal)
                    end),
    receive {P1, Tag1} -> ok end,
    receive {P2, Tag2} -> ok end,
    receive {P3, Tag3} -> ok end,
    ok = ?S3_MULTI:complete_upload(Bucket, File1, UploadId,
                                   [{Num1, Tag1}, {Num2, Tag2}, {Num3,Tag3}], C2),
    R99 = ?S3_MODULE:get_object(Bucket, File1, C2),

    %% Despite the reordering (via time) of the parts, we should get
    %% the value back in the order that we expect.
    ExpectedV = proplists:get_value(content, R99),

    %% {C2, Tag1, Tag2, Tag3, ExpectedV}.
    ok.

%%====================================================================
%% Helpers
%%====================================================================

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
    timestamp().

user_email() ->
    X = timestamp(),
    lists:flatten([X, $@, "me.com"]).

bucket() ->
    timestamp().

key() ->
    timestamp().

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
