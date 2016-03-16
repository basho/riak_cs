%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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

-module(xml_response_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("xmerl/include/xmerl.hrl").
-include_lib("erlcloud/include/erlcloud_aws.hrl").

-define(test_bucket(Label), "test-bucket-" Label).
-define(test_key(Label),    "test-key-" Label).

% use something that shouldn't occur naturally
-define(root_host_alt,      "foo.bar.example.com").

confirm() ->
    verify_multipart_upload_response().

verify_multipart_upload_response() ->
    Host    = ?root_host_alt,
    Bucket  = ?test_bucket("vrhr"),
    Key     = ?test_key("vrhr"),
    NumParts = 3,
    PartSize = (5 * 1024 * 1024),

    rtcs:set_conf({cs, current, 1}, [{root_host, Host}]),
    {AdminConfig, _} = rtcs:setup(1),
    % erlcloud requires that 's3_host' matches our 'root_host'
    % TODO: rtcs:setup should read this from the conf file
    Config = AdminConfig#aws_config{s3_host = Host},

    lager:info("creating bucket ~p", [Bucket]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(Bucket, Config)),

    {ok, {_, Result}} = perform_multipart_upload(
        Bucket, Key, NumParts, PartSize, Config),
    % lager:info("Response Data of '~s/~s':\n~p\n", [Bucket, Key, Result]),

    verify_multipart_upload_response(Result, Host, Bucket, Key),

    rtcs:pass().

perform_multipart_upload(Bucket, Key, NumParts, PartSize, Config) ->
    lager:info("initiating multipart upload of '~s/~s'", [Bucket, Key]),
    UploadId = erlcloud_s3_multipart:upload_id(
        erlcloud_s3_multipart:initiate_upload(Bucket, Key, [], [], Config)),

    lager:info("uploading parts of '~s/~s'", [Bucket, Key]),
    EtagList = upload_and_assert_parts(
        Bucket, Key, UploadId, NumParts, PartSize, Config),
    % lager:info("ETags of '~s/~s': ~p", [Bucket, Key, EtagList]),

    lager:info("completing upload of '~s/~s'", [Bucket, Key]),
    complete_multipart_upload(Bucket, Key, UploadId, EtagList, Config).

verify_multipart_upload_response(ResponseBody, RootHost, Bucket, Key) ->
    [#xmlText{value = ResBucket}] = get_response_value(
        ResponseBody, 'CompleteMultipartUploadResult', 'Bucket'),
    [#xmlText{value = ResKey}] = get_response_value(
        ResponseBody, 'CompleteMultipartUploadResult', 'Key'),
    [#xmlText{value = ResLocation}] = get_response_value(
        ResponseBody, 'CompleteMultipartUploadResult', 'Location'),

    Location = lists:flatten(
        io_lib:format("http://~s.~s/~s", [Bucket, RootHost, Key])),

    ?assertEqual(Bucket, ResBucket),
    ?assertEqual(Key, ResKey),
    ?assertEqual(Location, ResLocation).

get_response_value(
        #xmlElement{name = TopLevel, content = Content}, TopLevel, Field) ->
    get_response_value(Content, Field);
get_response_value(ResponseBody, TopLevel, Field)
        when erlang:is_tuple(ResponseBody)
        andalso erlang:tuple_size(ResponseBody) > 0 ->
    get_response_value(erlang:element(1, ResponseBody), TopLevel, Field).

get_response_value([#xmlElement{name = Field, content = Value} | _], Field) ->
    Value;
get_response_value([_ | Content], Field) ->
    get_response_value(Content, Field).

%
% Use erlcloud_s3:s3_request instead of erlcloud_s3_multipart:complete_upload
% to be able to process the full result XML.
%
complete_multipart_upload(Bucket, Key, UploadId, EtagList, Config) ->
    Response = erlcloud_s3:s3_request(
        Config, post, Bucket,
        [$/ | Key],                             % URL
        [{"uploadId", UploadId}],               % sub-resources
        [],                                     % params
        etags_to_multipart_request(EtagList),   % POST data
        []),                                    % headers
    % lager:info("Raw Response: ~p", [Response]),
    case Response of
        {_, []} ->
            {ok, Response};
        {Headers, BodyXML} ->
            Body = xmerl_scan:string(BodyXML),
            case erlang:element(1, Body) of
                #xmlElement{name = 'Error'} ->
                    {error, {msg, text, {Headers, Body}}};
                _ ->
                    {ok, {Headers, Body}}
            end
    end.

etags_to_multipart_request(EtagList) ->
    ReqData = [{'Part', [
        {'PartNumber', [erlang:integer_to_list(N)]},
        {'ETag', [T]}]} || {N, T} <- EtagList],
    % lager:info("Request Data: ~p", [ReqData]),
    Request = {'CompleteMultipartUpload', ReqData},
    erlang:list_to_binary(xmerl:export_simple([Request], xmerl_xml)).


upload_and_assert_parts(Bucket, Key, UploadId, NumParts, PartSize, Config) ->
    upload_and_assert_parts(
        Bucket, Key, UploadId, NumParts, PartSize, Config, []).

upload_and_assert_parts(_, _, _, 0, _, _, Result) ->
    Result;
upload_and_assert_parts(Bucket, Key, UploadId, PartNum, PartSize, Config, Result) ->
    upload_and_assert_parts(
        Bucket, Key, UploadId, (PartNum - 1), PartSize, Config,
        [{PartNum, rtcs_multipart:upload_and_assert_part(
            Bucket, Key, UploadId, PartNum,
            generate_part_data(PartNum, PartSize), Config)} | Result]).

%
% assume PartNum is < 256 and PartSize is not negative
%
generate_part_data(PartNum, PartSize) ->
    generate_part_data(PartNum, PartSize, <<>>).

generate_part_data(_, 0, Result) ->
    Result;
generate_part_data(PartNum, Remain, Result) ->
    generate_part_data(PartNum, (Remain - 1), <<Result/binary, PartNum>>).


