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
    {AdminUserConfig, {RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup(1),

    HeadRiakNode = hd(RiakNodes),
    AdminUser = {"admin@me.com", "admin",
                 AdminUserConfig#aws_config.access_key_id,
                 AdminUserConfig#aws_config.secret_access_key,
                 "enabled"},
    user_listing_json_test_case([AdminUser], AdminUserConfig, HeadRiakNode),
    user_listing_xml_test_case([AdminUser], AdminUserConfig, HeadRiakNode),

    %% Create other 1003 users and re-run user listing test cases
    Port = rtcs_config:cs_port(HeadRiakNode),
    Users1 = [AdminUser |
              create_users(Port, [{"bart@simpsons.com", "bart"},
                                  {"homer@simpsons.com", "homer"},
                                  {"taro@example.co.jp", japanese_aiueo()}], [])],
    Users2 = Users1 ++ create_200_users(Port),
    ?assertEqual(1 + 3 + 200, length(Users2)),

    user_listing_json_test_case(Users2, AdminUserConfig, HeadRiakNode),
    user_listing_xml_test_case(Users2, AdminUserConfig, HeadRiakNode),
    user_listing_many_times(Users2, AdminUserConfig, HeadRiakNode),
    update_user_json_test_case(AdminUserConfig, HeadRiakNode),
    update_user_xml_test_case(AdminUserConfig, HeadRiakNode),
    rtcs:pass().

japanese_aiueo() ->
    %% To avoid dependency on source code encoding, create list from chars.
    %% These five numbers represents "あいうえお" (A-I-U-E-O in Japanese).
    %% unicode:characters_to_binary([12354,12356,12358,12360,12362]).
    Chars = [12354,12356,12358,12360,12362],
    binary_to_list(unicode:characters_to_binary(Chars)).

create_200_users(Port) ->
    From = self(),
    Processes = 10,
    PerProcess = 20,
    [spawn(fun() ->
                   Users = create_users(
                             Port,
                             [begin
                                  Name = "zzz-" ++ integer_to_list(I * PerProcess + J),
                                  {Name ++ "@thousand.example.com", Name}
                              end || J <- lists:seq(1, PerProcess)],
                             []),
                   From ! Users
           end) ||
        I <- lists:seq(1, Processes)],
    collect_users(Processes, []).

collect_users(0, Acc) ->
    lists:usort(lists:flatten(Acc));
collect_users(N, Acc) ->
    receive
        Users -> collect_users(N-1, [Users | Acc])
    end.

create_users(_Port, [], Acc) ->
    ordsets:from_list(Acc);
create_users(Port, [{Email, Name} | Users], Acc) ->
    {Key, Secret, _Id} = rtcs_admin:create_user(Port, Email, Name),
    create_users(Port, Users, [{Email, Name, Key, Secret, "enabled"} | Acc]).

user_listing_json_test_case(Users, UserConfig, Node) ->
    user_listing_test(Users, UserConfig, Node, ?JSON).

user_listing_xml_test_case(Users, UserConfig, Node) ->
    user_listing_test(Users, UserConfig, Node, ?XML).

user_listing_many_times(Users, UserConfig, Node) ->
    [user_listing_test(Users, UserConfig, Node, ?JSON) ||
        _I <- lists:seq(1, 15)],
    ok.

user_listing_test(ExpectedUsers, UserConfig, Node, ContentType) ->
    Resource = "/riak-cs/users",
    Port = rtcs_config:cs_port(Node),
    Users = parse_user_info(
              rtcs:list_users(UserConfig, Port, Resource, ContentType)),
    ?assertEqual(ExpectedUsers, Users).

update_user_json_test_case(AdminConfig, Node) ->
    Users = [{"fergus@brave.sco", "Fergus"},
             {"merida@brave.sco", "Merida"},
             {"seamus@brave.sco", "Seamus"}],
    update_user_test(AdminConfig, Node, ?JSON, Users).

update_user_xml_test_case(AdminConfig, Node) ->
    Users = [{"gru@despicable.me", "Gru"},
             {"minion@despicable.me", "Minion  Minion"},
             {"dr.nefario@despicable.me", "DrNefario"}],
    update_user_test(AdminConfig, Node, ?XML, Users).

update_user_test(AdminConfig, Node, ContentType, Users) ->
    [{Email1, User1}, {Email2, User2}, {Email3, User3}]= Users,
    Port = rtcs_config:cs_port(Node),
    {Key, Secret, _} = rtcs_admin:create_user(Port, Email1, User1),
    {BadUserKey, BadUserSecret, _} = rtcs_admin:create_user(Port, Email3, User3),

    UserConfig = rtcs_config:config(Key, Secret, Port),
    BadUserConfig = rtcs_config:config(BadUserKey, BadUserSecret, Port),

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
    ?assertMatch({Email1, User1, _, Secret, "enabled"}, UserResult1),
    ?assertMatch({Email1, User1, _, Secret, "enabled"}, UserResult2),

    %% Attempt to update the user's email to be the same as the admin
    %% user and verify that the update attempt returns an error.
    Resource = "/riak-cs/user/" ++ UserConfig#aws_config.access_key_id,
    InvalidUpdateDoc = update_email_and_name_doc(ContentType, "admin@me.com", "admin"),

    ErrorResult = parse_error_code(
                    rtcs:update_user(UserConfig,
                                     Port,
                                     Resource,
                                     ContentType,
                                     InvalidUpdateDoc)),
    ?assertEqual("UserAlreadyExists", ErrorResult),

    %% Test updating the user's name and email
    UpdateDoc = update_email_and_name_doc(ContentType, Email2, User2),
    _ = rtcs:update_user(UserConfig, Port, Resource, ContentType, UpdateDoc),

    %% Fetch the user record using the user's own credentials
    UserResult3 = parse_user_record(
                    get_user_record(UserConfig, Port, UserResource, ContentType),
                    ContentType),
    %% Fetch the user record using the admin credentials
    UserResult4 = parse_user_record(
                    get_user_record(AdminConfig, Port, AdminResource, ContentType),
                    ContentType),
    ?assertMatch({Email2, User2, _, Secret, "enabled"}, UserResult3),
    ?assertMatch({Email2, User2, _, Secret, "enabled"}, UserResult4),

    %% Test that attempting to update another user's status with a
    %% non-admin account is disallowed
    UpdateDoc2 = update_status_doc(ContentType, "disabled"),
    Resource = "/riak-cs/user/" ++ UserConfig#aws_config.access_key_id,
    ErrorResult2 = parse_error_code(
                    rtcs:update_user(BadUserConfig,
                                     Port,
                                     Resource,
                                     ContentType,
                                     UpdateDoc2)),
    ?assertEqual("AccessDenied", ErrorResult2),

    %% Test updating a user's own status
    Resource = "/riak-cs/user/" ++ UserConfig#aws_config.access_key_id,
    _ = rtcs:update_user(UserConfig, Port, Resource, ContentType, UpdateDoc2),

    %% Fetch the user record using the user's own credentials. Since
    %% the user is now disabled this should return an error.
    UserResult5 = parse_error_code(
                    get_user_record(UserConfig, Port, UserResource, ContentType)),

    %% Fetch the user record using the admin credentials. The user is
    %% not able to retrieve their own account information now that the
    %% account is disabled.
    UserResult6 = parse_user_record(
                    get_user_record(AdminConfig, Port, AdminResource, ContentType),
                    ContentType),
    ?assertEqual("AccessDenied", UserResult5),
    ?assertMatch({Email2, User2, _, Secret, "disabled"}, UserResult6),

    %% Re-enable the user
    UpdateDoc3 = update_status_doc(ContentType, "enabled"),
    Resource = "/riak-cs/user/" ++ UserConfig#aws_config.access_key_id,
    _ = rtcs:update_user(AdminConfig, Port, Resource, ContentType, UpdateDoc3),

    %% Test issuing a new key_secret
    UpdateDoc4 = new_key_secret_doc(ContentType),
    Resource = "/riak-cs/user/" ++ UserConfig#aws_config.access_key_id,
    UpdateResult = rtcs:update_user(AdminConfig,
                                    Port,
                                    Resource,
                                    ContentType,
                                    UpdateDoc4),
    {_, _, _, UpdSecret1, _} = parse_user_record(UpdateResult, ContentType),

    %% Generate an updated user config with the new secret
    UserConfig2 = rtcs_config:config(Key, UpdSecret1, Port),

    %% Fetch the user record using the user's own credentials
    UserResult7 = parse_user_record(
                    get_user_record(UserConfig2, Port, UserResource, ContentType),
                    ContentType),
    %% Fetch the user record using the admin credentials
    UserResult8 = parse_user_record(
                    get_user_record(AdminConfig, Port, AdminResource, ContentType),
                    ContentType),
    ?assertMatch({_, _, _, UpdSecret1, _}, UserResult7),
    ?assertMatch({_, _, _, UpdSecret1, _}, UserResult8),
    ?assertMatch({Email2, User2, _, _, "enabled"}, UserResult7),
    ?assertMatch({Email2, User2, _, _, "enabled"}, UserResult8).

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

new_key_secret_doc(?JSON) ->
    "'{\"new_key_secret\": true}'";
new_key_secret_doc(?XML) ->
    "'<?xml version=\"1.0\" encoding=\"UTF-8\"?><User><NewKeySecret>true</NewKeySecret></User>'".

update_status_doc(?JSON, Status) ->
    "'{\"status\":\"" ++ Status ++ "\"}'";
update_status_doc(?XML, Status) ->
    "'<?xml version=\"1.0\" encoding=\"UTF-8\"?><User><Status>" ++ Status ++ "</Status></User>'".

update_email_and_name_doc(?JSON, Email, Name) ->
    "'{\"email\":\"" ++ Email ++  "\", \"name\":\"" ++ Name ++"\"}'";
update_email_and_name_doc(?XML, Email, Name) ->
    "'<?xml version=\"1.0\" encoding=\"UTF-8\"?><User><Email>" ++ Email ++
        "</Email><Name>" ++ Name ++ "</Name><Status>enabled</Status></User>'".

parse_user_record(Output, ?JSON) ->
    {struct, JsonData} = mochijson2:decode(Output),
    Email = binary_to_list(proplists:get_value(<<"email">>, JsonData)),
    Name = binary_to_list(proplists:get_value(<<"name">>, JsonData)),
    KeyId = binary_to_list(proplists:get_value(<<"key_id">>, JsonData)),
    KeySecret = binary_to_list(proplists:get_value(<<"key_secret">>, JsonData)),
    Status = binary_to_list(proplists:get_value(<<"status">>, JsonData)),
    {Email, Name, KeyId, KeySecret, Status};
parse_user_record(Output, ?XML) ->
    {ParsedData, _Rest} = xmerl_scan:string(Output, []),
    lists:foldl(fun user_fields_from_xml/2,
                {[], [], [], [], []},
                ParsedData#xmlElement.content).

parse_user_records(Output, ?JSON) ->
    JsonData = mochijson2:decode(Output),
    [begin
         Email = binary_to_list(proplists:get_value(<<"email">>, UserJson)),
         Name = binary_to_list(proplists:get_value(<<"name">>, UserJson)),
         KeyId = binary_to_list(proplists:get_value(<<"key_id">>, UserJson)),
         KeySecret = binary_to_list(proplists:get_value(<<"key_secret">>, UserJson)),
         Status = binary_to_list(proplists:get_value(<<"status">>, UserJson)),
         {Email, Name, KeyId, KeySecret, Status}
     end || {struct, UserJson} <- JsonData];
parse_user_records(Output, ?XML) ->
    {ParsedData, _Rest} = xmerl_scan:string(Output, []),
    [lists:foldl(fun user_fields_from_xml/2,
                 {[], [], [], [], []},
                 UserXml#xmlElement.content)
     || UserXml <- ParsedData#xmlElement.content].

-spec user_fields_from_xml(#xmlText{} | #xmlElement{}, tuple()) -> tuple().
user_fields_from_xml(#xmlText{}, Acc) ->
    Acc;
user_fields_from_xml(Element, {Email, Name, KeyId, Secret, Status}=Acc) ->
    case Element#xmlElement.name of
        'Email' ->
            [Content | _] = Element#xmlElement.content,
            case is_record(Content, xmlText) of
                true ->
                    {xml_text_value(Content), Name, KeyId, Secret, Status};
                false ->
                    Acc
            end;
        'Name' ->
            [Content | _] = Element#xmlElement.content,
            case is_record(Content, xmlText) of
                true ->
                    {Email, xml_text_value(Content), KeyId, Secret, Status};
                false ->
                    Acc
            end;
        'KeyId' ->
            [Content | _] = Element#xmlElement.content,
            case is_record(Content, xmlText) of
                true ->
                    {Email, Name, Content#xmlText.value, Secret, Status};
                false ->
                    Acc
            end;
        'KeySecret' ->
            [Content | _] = Element#xmlElement.content,
            case is_record(Content, xmlText) of
                true ->
                    {Email, Name, KeyId, Content#xmlText.value, Status};
                false ->
                    Acc
            end;
        'Status' ->
            [Content | _] = Element#xmlElement.content,
            case is_record(Content, xmlText) of
                true ->
                    {Email, Name, KeyId, Secret, Content#xmlText.value};
                false ->
                    Acc
            end;
        _ ->
            Acc
    end.

xml_text_value(XmlText) ->
    %% xmerl return list of UTF-8 characters, each element of it represent
    %% one character (or codepoint), not one byte.
    binary_to_list(unicode:characters_to_binary(XmlText#xmlText.value)).

parse_error_code(Output) ->
    {ParsedData, _Rest} = xmerl_scan:string(Output, []),
    lists:foldl(fun error_code_from_xml/2,
                undefined,
                ParsedData#xmlElement.content).

error_code_from_xml(#xmlText{}, Acc) ->
    Acc;
error_code_from_xml(Element, Acc) ->
    case Element#xmlElement.name of
        'Code' ->
            [Content | _] = Element#xmlElement.content,
            case is_record(Content, xmlText) of
                true ->
                    Content#xmlText.value;
                false ->
                    Acc
            end;
        _ ->
            Acc
    end.

parse_user_info(Output) ->
    [Boundary | Tokens] = string:tokens(Output, "\r\n"),
    parse_user_info(Tokens, Boundary, []).

parse_user_info([_LastToken], _, Users) ->
    ordsets:from_list(Users);
parse_user_info(["Content-Type: application/xml", RawXml | RestTokens],
                 Boundary, Users) ->
    UpdUsers = parse_user_records(RawXml, ?XML) ++ Users,
    parse_user_info(RestTokens, Boundary, UpdUsers);
parse_user_info(["Content-Type: application/json", RawJson | RestTokens],
                 Boundary, Users) ->
    UpdUsers = parse_user_records(RawJson, ?JSON) ++ Users,
    parse_user_info(RestTokens, Boundary, UpdUsers);
parse_user_info([_ | RestTokens], Boundary, Users) ->
    parse_user_info(RestTokens, Boundary, Users).
