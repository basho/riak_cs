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

    HeadRiakNode = hd(RiakNodes),
    AdminUserCreds = {AdminUserConfig#aws_config.access_key_id,
                      AdminUserConfig#aws_config.secret_access_key},
    user_listing_json_test_case([AdminUserCreds], AdminUserConfig, HeadRiakNode),
    user_listing_xml_test_case([AdminUserCreds], AdminUserConfig, HeadRiakNode),

    %% Create 2 users and re-run user listing test cases
    Port = rtcs:cs_port(HeadRiakNode),
    {TestKey1, TestSecret1, _} = rtcs:create_user(Port, "bart@simpsons.com", "bart"),
    {TestKey2, TestSecret2, _} = rtcs:create_user(Port, "homer@simpsons.com", "homer"),
    UserCreds = ordsets:from_list([AdminUserCreds,
                                   {TestKey1, TestSecret1},
                                   {TestKey2, TestSecret2}]),

    user_listing_json_test_case(UserCreds, AdminUserConfig, HeadRiakNode),
    user_listing_xml_test_case(UserCreds, AdminUserConfig, HeadRiakNode),

    update_user_json_test_case(AdminUserConfig, HeadRiakNode),
    update_user_xml_test_case(AdminUserConfig, HeadRiakNode),
    pass.

user_listing_json_test_case(UserCreds, UserConfig, Node) ->
    user_listing_test(UserCreds, UserConfig, Node, ?JSON).

user_listing_xml_test_case(UserCreds, UserConfig, Node) ->
    user_listing_test(UserCreds, UserConfig, Node, ?XML).

user_listing_test(ExpectedUserCreds, UserConfig, Node, ContentType) ->
    Resource = "/riak-cs/users",
    Port = rtcs:cs_port(Node),
    UserCreds = parse_user_creds(
                  rtcs:list_users(UserConfig, Port, Resource, ContentType)),
    ?assertEqual(ExpectedUserCreds, UserCreds).

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

    %% Test updating a user's status
    UpdateDoc2 = update_status_doc(ContentType, "disabled"),
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
    UserConfig2 = rtcs:config(Key, UpdSecret1, Port),

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
     end || UserJson <- JsonData];
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
                    {Content#xmlText.value, Name, KeyId, Secret, Status};
                false ->
                    Acc
            end;
        'Name' ->
            [Content | _] = Element#xmlElement.content,
            case is_record(Content, xmlText) of
                true ->
                    {Email, Content#xmlText.value, KeyId, Secret, Status};
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

parse_user_creds(Output) ->
    [Boundary | Tokens] = string:tokens(Output, "\r\n"),
    parse_user_creds(Tokens, Boundary, []).

parse_user_creds([_LastToken], _, UserCreds) ->
    ordsets:from_list(UserCreds);
parse_user_creds(["Content-Type: application/xml", RawXml | RestTokens],
                 Boundary, UserCreds) ->
    UpdUserCreds = update_user_creds(parse_user_records(RawXml, ?XML),
                                    UserCreds),
    parse_user_creds(RestTokens, Boundary, UpdUserCreds);
parse_user_creds(["Content-Type: application/json", RawJson | RestTokens],
                 Boundary, UserCreds) ->
    UpdUserCreds = update_user_creds(parse_user_records(RawJson, ?JSON),
                                     UserCreds),
    parse_user_creds(RestTokens, Boundary, UpdUserCreds);
parse_user_creds([_ | RestTokens], Boundary, UserCreds) ->
    parse_user_creds(RestTokens, Boundary, UserCreds).

update_user_creds(UserRecords, UserCreds) ->
    FoldFun = fun({_, _, KeyId, Secret, _}, Acc) ->
                      [{KeyId, Secret} | Acc]
              end,
    lists:foldl(FoldFun, UserCreds, UserRecords).
