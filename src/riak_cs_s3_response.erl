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

-module(riak_cs_s3_response).
-export([api_error/3,
         status_code/1,
         respond/4,
         export_xml/1,
         error_response/1,
         error_response/5,
         list_bucket_response/5,
         list_all_my_buckets_response/3,
         copy_object_response/3,
         no_such_upload_response/3,
         error_code_to_atom/1]).

-include("riak_cs.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-type xmlElement() :: #xmlElement{}.

-spec error_message(atom() | {'riak_connect_failed', term()}) -> string().
-spec error_code(atom() | {'riak_connect_failed', term()}) -> string().
-spec status_code(atom() | {'riak_connect_failed', term()}) -> pos_integer().

error_message(invalid_access_key_id) ->
    "The AWS Access Key Id you provided does not exist in our records.";
error_message(invalid_email_address) ->
    "The email address you provided is not a valid.";
error_message(access_denied) ->
    "Access Denied";
error_message(bucket_not_empty) ->
    "The bucket you tried to delete is not empty.";
error_message(bucket_already_exists) ->
    "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.";
error_message(user_already_exists) ->
    "The specified email address has already been registered. Email addresses must be unique among all users of the system. Please try again with a different email address.";
error_message(entity_too_large) ->
    "Your proposed upload exceeds the maximum allowed object size.";
error_message(entity_too_small) ->
    "Your proposed upload is smaller than the minimum allowed object size. Each part must be at least 5 MB in size, except the last part.";
error_message(invalid_user_update) ->
    "The user update you requested was invalid.";
error_message(no_such_bucket) ->
    "The specified bucket does not exist.";
error_message({riak_connect_failed, Reason}) ->
    io_lib:format("Unable to establish connection to Riak. Reason: ~p", [Reason]);
error_message(admin_key_undefined) -> "Please reduce your request rate.";
error_message(admin_secret_undefined) -> "Please reduce your request rate.";
error_message(bucket_owner_unavailable) -> "The user record for the bucket owner was unavailable. Try again later.";
error_message(econnrefused) -> "Please reduce your request rate.";
error_message(malformed_policy_json) -> "JSON parsing error";
error_message(malformed_policy_missing) -> "Policy is missing required element";
error_message(malformed_policy_resource) -> "Policy has invalid resource";
error_message(malformed_policy_principal) -> "Invalid principal in policy";
error_message(malformed_policy_action) -> "Policy has invalid action";
error_message(no_such_bucket_policy) -> "The bucket policy does not exist";
error_message(no_such_upload) ->
    "The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.";
error_message(bad_request) -> "Bad Request";
error_message(invalid_argument) -> "Invalid Argument";
error_message(unresolved_grant_email) -> "The e-mail address you provided does not match any account on record.";
error_message(invalid_range) -> "The requested range is not satisfiable";
error_message(invalid_bucket_name) -> "The specified bucket is not valid.";
error_message(_) -> "Please reduce your request rate.".

error_code(invalid_access_key_id) -> "InvalidAccessKeyId";
error_code(access_denied) -> "AccessDenied";
error_code(bucket_not_empty) -> "BucketNotEmpty";
error_code(bucket_already_exists) -> "BucketAlreadyExists";
error_code(user_already_exists) -> "UserAlreadyExists";
error_code(entity_too_large) -> "EntityTooLarge";
error_code(entity_too_small) -> "EntityTooSmall";
error_code(bad_etag) -> "InvalidPart";
error_code(bad_etag_order) -> "InvalidPartOrder";
error_code(invalid_user_update) -> "InvalidUserUpdate";
error_code(no_such_bucket) -> "NoSuchBucket";
error_code({riak_connect_failed, _}) -> "RiakConnectFailed";
error_code(admin_key_undefined) -> "ServiceUnavailable";
error_code(admin_secret_undefined) -> "ServiceUnavailable";
error_code(bucket_owner_unavailable) -> "ServiceUnavailable";
error_code(econnrefused) -> "ServiceUnavailable";
error_code(malformed_policy_json) -> "MalformedPolicy";
error_code(malformed_policy_missing) -> "MalformedPolicy";
error_code(malformed_policy_resource) -> "MalformedPolicy";
error_code(malformed_policy_principal) -> "MalformedPolicy";
error_code(malformed_policy_action) -> "MalformedPolicy";
error_code(no_such_bucket_policy) -> "NoSuchBucketPolicy";
error_code(no_such_upload) -> "NoSuchUpload";
error_code(bad_request) -> "BadRequest";
error_code(invalid_argument) -> "InvalidArgument";
error_code(invalid_range) -> "InvalidRange";
error_code(invalid_bucket_name) -> "InvalidBucketName";
error_code(unresolved_grant_email) -> "UnresolvableGrantByEmailAddress";
error_code(ErrorName) ->
    ok = lager:debug("Unknown Error Name: ~p", [ErrorName]),
    "ServiceUnavailable".

%% These should match:
%% http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html

status_code(invalid_access_key_id) -> 403;
status_code(invalid_email_address) -> 400;
status_code(access_denied) ->  403;
status_code(bucket_not_empty) ->  409;
status_code(bucket_already_exists) -> 409;
status_code(user_already_exists) -> 409;
%% yes, 400, really, not 413
status_code(entity_too_large) -> 400;
status_code(entity_too_small) -> 400;
status_code(bad_etag) -> 400;
status_code(bad_etag_order) -> 400;
status_code(invalid_user_update) -> 400;
status_code(no_such_bucket) -> 404;
status_code({riak_connect_failed, _}) -> 503;
status_code(admin_key_undefined) -> 503;
status_code(admin_secret_undefined) -> 503;
status_code(bucket_owner_unavailable) -> 503;
status_code(multiple_bucket_owners) -> 503;
status_code(econnrefused) -> 503;
status_code(malformed_policy_json) -> 400;
status_code(malformed_policy_missing) -> 400;
status_code(malformed_policy_resource) -> 400;
status_code(malformed_policy_principal) -> 400;
status_code(malformed_policy_action) -> 400;
status_code(no_such_bucket_policy) -> 404;
status_code(no_such_upload) -> 404;
status_code(bad_request) -> 400;
status_code(invalid_argument) -> 400;
status_code(unresolved_grant_email) -> 400;
status_code(invalid_range) -> 416;
status_code(invalid_bucket_name) -> 400;
status_code(_) -> 503.


respond(StatusCode, Body, ReqData, Ctx) ->
     UpdReqData = wrq:set_resp_body(Body,
                                    wrq:set_resp_header("Content-Type",
                                                        ?XML_TYPE,
                                                        ReqData)),
    {{halt, StatusCode}, UpdReqData, Ctx}.

api_error(Error, ReqData, Ctx) when is_atom(Error); is_tuple(Error) ->
    error_response(status_code(Error), error_code(Error), error_message(Error),
                   ReqData, Ctx).

error_response(ErrorDoc) when length(ErrorDoc) =:= 0 ->
    {error, error_code_to_atom("BadRequest")};
error_response(ErrorDoc) ->
    {error, error_code_to_atom(xml_error_code(ErrorDoc))}.

error_response(StatusCode, Code, Message, RD, Ctx) ->
    {OrigResource, _} = riak_cs_s3_rewrite:original_resource(RD),
    XmlDoc = [{'Error', [{'Code', [Code]},
                         {'Message', [Message]},
                         {'Resource', [string:strip(OrigResource, right, $/)]},
                         {'RequestId', [""]}]}],
    respond(StatusCode, export_xml(XmlDoc), RD, Ctx).

list_all_my_buckets_response(User, RD, Ctx) ->
    BucketsDoc =
        [begin
             case is_binary(B?RCS_BUCKET.name) of
                 true ->
                     Name = binary_to_list(B?RCS_BUCKET.name);
                 false ->
                     Name = B?RCS_BUCKET.name
             end,
             {'Bucket',
              [{'Name', [Name]},
               {'CreationDate', [B?RCS_BUCKET.creation_date]}]}
         end || B <- riak_cs_utils:get_buckets(User)],
    Contents =  [user_to_xml_owner(User)] ++ [{'Buckets', BucketsDoc}],
    XmlDoc = [{'ListAllMyBucketsResult', [{'xmlns', ?S3_XMLNS}], Contents}],
    respond(200, export_xml(XmlDoc), RD, Ctx).

list_bucket_response(User, Bucket, KeyObjPairs, RD, Ctx) ->
    %% @TODO Once the optimization for storing small objects
    %% is completed, a check will be required either here or
    %% in `riak_cs_lfs_utils' to determine if the object
    %% associated with each key is an `lfs_manifest' or not.
    Contents = [begin
                    % Key is just not a unicode string but just a
                    % raw binary like <<229,159,188,231,142,137>>.
                    % we need to convert to unicode string like [22524,29577].
                    % this does not affect ascii characters.
                    KeyString = unicode:characters_to_list(Key, unicode),
                    case ObjResp of
                        {ok, Manifest} ->
                            Size = integer_to_list(
                                     Manifest?MANIFEST.content_length),
                            LastModified =
                                riak_cs_wm_utils:to_iso_8601(
                                  Manifest?MANIFEST.created),
                            ETag = riak_cs_utils:etag_from_binary(Manifest?MANIFEST.content_md5),
                            {'Contents', [{'Key', [KeyString]},
                                          {'Size', [Size]},
                                          {'LastModified', [LastModified]},
                                          {'ETag', [ETag]},
                                          user_to_xml_owner(User)]};
                        {error, Reason} ->
                            _ = lager:debug("Unable to fetch manifest for ~p. Reason: ~p",
                                            [Key, Reason]),
                            undefined
                    end
                end
                || {Key, ObjResp} <- KeyObjPairs],
    BucketProps = [{'Name', [Bucket?RCS_BUCKET.name]},
                   {'Prefix', []},
                   {'Marker', []},
                   {'MaxKeys', ["1000"]},
                   {'Delimiter', ["/"]},
                   {'IsTruncated', ["false"]}],
    XmlDoc = [{'ListBucketResult', BucketProps++
                   lists:filter(fun(E) -> E /= undefined end,
                                Contents)}],
    respond(200, export_xml(XmlDoc), RD, Ctx).

user_to_xml_owner(?RCS_USER{canonical_id=CanonicalId, display_name=Name}) ->
    {'Owner', [{'ID', [CanonicalId]},
               {'DisplayName', [Name]}]}.

copy_object_response(Manifest, RD, Ctx) ->
    LastModified = riak_cs_wm_utils:to_iso_8601(Manifest?MANIFEST.created),
    ETag = riak_cs_utils:etag_from_binary(Manifest?MANIFEST.content_md5),
    XmlDoc = [{'CopyObjectResponse',
               [{'LastModified', [LastModified]},
                {'ETag', [ETag]}]}],
    respond(200, export_xml(XmlDoc), RD, Ctx).

export_xml(XmlDoc) ->
    unicode:characters_to_binary(
      xmerl:export_simple(XmlDoc, xmerl_xml, [{prolog, ?XML_PROLOG}]), unicode, unicode).

no_such_upload_response(UploadId, RD, Ctx) ->
    XmlDoc = {'Error',
              [
               {'Code', [error_code(no_such_upload)]},
               {'Message', [error_message(no_such_upload)]},
               {'UploadId', [binary_to_list(base64url:encode(UploadId))]},
               {'HostId', ["host-id"]}
              ]},
    Body = export_xml([XmlDoc]),
    respond(status_code(no_such_upload), Body, RD, Ctx).

%% @doc Convert an error code string into its corresponding atom
-spec error_code_to_atom(string()) -> atom().
error_code_to_atom(ErrorCode) ->
    case ErrorCode of
        "BadRequest" ->
            bad_request;
        "InvalidAccessKeyId" ->
            invalid_access_key_id;
        "AccessDenied" ->
            access_denied;
        "BucketNotEmpty" ->
            bucket_not_empty;
        "BucketAlreadyExists" ->
            bucket_already_exists;
        "UserAlreadyExists" ->
            user_already_exists;
        "NoSuchBucket" ->
            no_such_bucket;
        _ ->
            unknown
    end.

%% @doc Get the value of the `Code' element from
%% and XML document.
-spec xml_error_code(string()) -> string().
xml_error_code(Xml) ->
    {ParsedData, _Rest} = xmerl_scan:string(Xml, []),
    process_xml_error(ParsedData#xmlElement.content).

%% @doc Process the top-level elements of the
-spec process_xml_error([xmlElement()]) -> string().
process_xml_error([]) ->
    [];
process_xml_error([HeadElement | RestElements]) ->
    _ = lager:debug("Element name: ~p", [HeadElement#xmlElement.name]),
    ElementName = HeadElement#xmlElement.name,
    case ElementName of
        'Code' ->
            [Content] = HeadElement#xmlElement.content,
            Content#xmlText.value;
        _ ->
            process_xml_error(RestElements)
    end.
