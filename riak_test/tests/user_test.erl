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

-module(user_test).

-compile(export_all).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("erlcloud/include/erlcloud_aws.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-define(TEST_BUCKET, "riak-test-bucket").
-define(JSON, "application/json").
-define(XML, "application/xml").

confirm() ->
    {AdminUserConfig, {RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup(4),
    update_user_json_test_case(AdminUserConfig, hd(RiakNodes)),
    update_user_xml_test_case(AdminUserConfig, hd(RiakNodes)),
    pass.

update_user_json_test_case(AdminConfig, Node) ->
    Users = [{"fergus@brave.sco", "Fergus"},
             {"merida@brave.sco", "Merida"}],
    update_user_test(AdminConfig, Node, ?JSON, Users).

update_user_xml_test_case(AdminConfig, Node) ->
    Users = [{"gru@despicable.me", "Gru"},
             {"minion@despicable.me", "Minion"}],
    update_user_test(AdminConfig, Node, ?XML, Users).

update_user_test(AdminConfig, Node, ContentType, Users) ->
    [{Email1, User1}, {Email2, User2}]= Users,
    Port = rtcs:cs_port(Node),
    {Key, Secret, _} = rtcs:create_user(Port, Email1, User1),

    UserConfig = rtcs:config(Key, Secret, Port),

    UserResource = "/riak-cs/user",
    AdminResource = UserResource ++ "/" ++ UserConfig#aws_config.access_key_id,

    %% Fetch the user record using the user's own credentials
    UserResult1 = parse_user_record(
                   get_user_record(UserConfig, Port, UserResource, ContentType),
                   ContentType),
    %% Fetch the user record using the admin credentials
    UserResult2 = parse_user_record(
                        get_user_record(AdminConfig, Port, AdminResource, ContentType),
                        ContentType),
    ?assertEqual({Email1, User1}, UserResult1),
    ?assertEqual({Email1, User1}, UserResult2),

    %% @TODO Also test that the admin credentials can update another user record
    UpdateDoc = update_user_doc(ContentType, Email2, User2, true),
    Resource = "/riak-cs/user/" ++ UserConfig#aws_config.access_key_id,
    ok = rtcs:update_user(UserConfig, Port, Resource, ContentType, UpdateDoc),

    %% Fetch the user record using the user's own credentials
    UserResult3 = parse_user_record(
                    get_user_record(UserConfig, Port, UserResource, ContentType),
                    ContentType),
    %% Fetch the user record using the admin credentials
    UserResult4 = parse_user_record(
                    get_user_record(AdminConfig, Port, AdminResource, ContentType),
                    ContentType),
    ?assertEqual({Email2, User2}, UserResult3),
    ?assertEqual({Email2, User2}, UserResult4).

get_user_record(UserConfig, Port, Resource, ContentType) ->
    lager:debug("Retreiving user record"),
    Date = httpd_util:rfc1123_date(),
    Cmd="curl -s -H 'Date: " ++ Date ++
        "' -H 'Accept: " ++ ContentType ++
        "' -H 'Content-Type: " ++ ContentType ++
        "' -H 'Authorization: " ++
        rtcs:make_authorization("GET", Resource, ContentType, UserConfig, Date) ++
        "' http://localhost:" ++
        integer_to_list(Port) ++ Resource,
    lager:info("User retrieval cmd: ~p", [Cmd]),
    Output = os:cmd(Cmd),
    lager:debug("User record=~p~n",[Output]),
    Output.

update_user_doc(?JSON, Email, Name, _Status) ->
    "'{\"email\":\"" ++ Email ++  "\", \"name\":\"" ++ Name ++"\"}'";
update_user_doc(?XML, Email, Name, _Status) ->
    "'<?xml version=\"1.0\" encoding=\"UTF-8\"?><User><Email>" ++ Email ++
        "</Email><Name>" ++ Name ++ "</Name><Status>enabled</Status></User>'".

parse_user_record(Output, ?JSON) ->
    {struct, JsonData} = mochijson2:decode(Output),
    Email = binary_to_list(proplists:get_value(<<"email">>, JsonData)),
    Name = binary_to_list(proplists:get_value(<<"name">>, JsonData)),
    {Email, Name};
parse_user_record(Output, ?XML) ->
    {ParsedData, _Rest} = xmerl_scan:string(Output, []),
    lists:foldl(fun email_and_name_from_xml/2,
                {[], []},
                ParsedData#xmlElement.content).


-spec email_and_name_from_xml(#xmlText{} | #xmlElement{}, [{atom(), term()}]) -> [{atom(), term()}].
email_and_name_from_xml(#xmlText{}, Acc) ->
    Acc;
email_and_name_from_xml(Element, {Email, Name}=Acc) ->
    case Element#xmlElement.name of
        'Email' ->
            [Content | _] = Element#xmlElement.content,
            case is_record(Content, xmlText) of
                true ->
                    {Content#xmlText.value, Name};
                false ->
                    Acc
            end;
        'Name' ->
            [Content | _] = Element#xmlElement.content,
            case is_record(Content, xmlText) of
                true ->
                    {Email, Content#xmlText.value};
                false ->
                    Acc
            end;
        _ ->
            Acc
    end.
