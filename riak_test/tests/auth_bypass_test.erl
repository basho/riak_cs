%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(auth_bypass_test).

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("erlcloud/include/erlcloud_aws.hrl").

confirm() ->
    Config = [{cs, rtcs:cs_config([{admin_auth_enabled, false}])}],
    {UserConfig, {RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup(1, Config),
    KeyId = UserConfig#aws_config.access_key_id,
    Port = rtcs:cs_port(hd(RiakNodes)),

    confirm_auth_bypass("riak-cs", "stats", UserConfig, Port),
    confirm_auth_bypass("riak-cs", "users", UserConfig, Port),
    confirm_auth_bypass("riak-cs", "user/"  ++ KeyId, UserConfig, Port),
    confirm_auth_bypass("riak-cs", "usage/" ++ KeyId ++ "/ab/" ++ rtcs:datetime() ++ "/" ++ rtcs:datetime(),
                        UserConfig, Port),
    pass.

confirm_auth_bypass(Bucket, Key, UserConfig, Port) ->
    S3Result = erlcloud_s3:get_object(Bucket, Key, UserConfig),
    S3Content = proplists:get_value(content, S3Result),
    lager:debug("erlcloud output: ~p~n", [S3Content]),

    CurlContent = curl_request(Bucket, Key, Port),
    lager:debug("curl output: ~p~n", [CurlContent]),
    ?assertEqual(extract_contents(S3Content), extract_contents(CurlContent)).

curl_request(Bucket, Key, Port) ->
    Cmd = "curl -s http://localhost:" ++ integer_to_list(Port)
        ++ "/" ++ Bucket ++ "/" ++ Key,
    lager:debug("cmd: ~p", [Cmd]),
    os:cmd(Cmd).

extract_contents(Output) when is_binary(Output) ->
    extract_contents(binary_to_list(Output));
extract_contents(Output) ->
    [MaybeBoundary | Tokens] = string:tokens(Output, "\r\n"),
    extract_contents(Tokens, MaybeBoundary, []).

extract_contents([], NonMultipartContent, []) ->
    lager:debug("extracted contents: ~p~n", [NonMultipartContent]),
    NonMultipartContent;
extract_contents([], _Boundary, Contents) ->
    lager:debug("extracted contents: ~p~n", [Contents]),
    Contents;
extract_contents(["Content-Type: application/xml", Content | Tokens],
                Boundary, Contents) ->
    extract_contents(Tokens, Boundary, Contents ++ [Content]);
extract_contents([Boundary | Tokens], Boundary, Contents) ->
    extract_contents(Tokens, Boundary, Contents);
extract_contents([_ | Tokens], Boundary, Contents) ->
    extract_contents(Tokens, Boundary, Contents).
