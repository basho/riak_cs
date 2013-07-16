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

-module(riak_cs_oos_response).
-export([api_error/3,
         error_code_to_atom/1,
         error_response/5,
         invalid_digest_response/3,
         respond/3,
         status_code/1]).

-include("riak_cs.hrl").
-include("riak_cs_api.hrl").
-include("list_objects.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-spec respond(term(), #wm_reqdata{}, #context{}) ->
                     {string() | {halt, non_neg_integer()} , #wm_reqdata{}, #context{}}.
respond(?LBRESP{}=Response, RD, Ctx) ->
    BucketsDoc =
        [begin
             case is_binary(B?RCS_BUCKET.name) of
                 true ->
                     binary_to_list(B?RCS_BUCKET.name) ++ "\n";
                 false ->
                     B?RCS_BUCKET.name ++ "\n"
             end
         end || B <- Response?LBRESP.buckets],
    UpdRD = wrq:set_resp_header("Content-Type",
                                "text/plain",
                                RD),
    {BucketsDoc, UpdRD, Ctx};
respond({ok, ?LORESP{}=Response}, RD, Ctx) ->
    %% @TODO Expand to handle common prefixes
    UpdRD = wrq:set_resp_header("Content-Type",
                                "text/plain",
                                RD),
    ResponseBody = [binary_to_list(KeyContent?LOKC.key) ++ "\n" ||
                       KeyContent <- Response?LORESP.contents,
                       KeyContent /= undefined],
    {ResponseBody, UpdRD, Ctx};
respond({error, _}=Error, RD, Ctx) ->
    api_error(Error, RD, Ctx).

api_error(Error, RD, Ctx) when is_atom(Error); is_tuple(Error) ->
    error_response(status_code(Error), error_code(Error), error_message(Error),
                   RD, Ctx);
api_error({riak_connect_failed, _}=Error, RD, Ctx) ->
    error_response(status_code(Error), error_code(Error), error_message(Error),
                   RD, Ctx);
api_error({error, Reason}, RD, Ctx) ->
    api_error(Reason, RD, Ctx).

error_response(StatusCode, _Code, _Message, RD, Ctx) ->
    {{halt, StatusCode}, RD, Ctx}.

%% @TODO Figure out how this should actually be formatted
invalid_digest_response(ContentMd5, RD, Ctx) ->
    XmlDoc = {'Error',
              [
               {'Code', [error_code(invalid_digest)]},
               {'Message', [error_message(invalid_digest)]},
               {'Content-MD5', [ContentMd5]},
               {'HostId', ["host-id"]}
              ]},
    Body = riak_cs_xml:export_xml([XmlDoc]),
    UpdRD = wrq:set_resp_body(Body,
                              wrq:set_resp_header("Content-Type",
                                                  ?XML_TYPE,
                                                  RD)),
    {{halt, status_code(invalid_digest)}, UpdRD, Ctx}.

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
error_message(malformed_policy_condition) -> "Policy has invalid condition";
error_message(no_such_bucket_policy) -> "The bucket policy does not exist";
error_message(no_such_upload) ->
    "The specified upload does not exist. The upload ID may be invalid, "
        "or the upload may have been aborted or completed.";
error_message(invalid_digest) ->
    "The Content-MD5 you specified was invalid.";
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
error_code(malformed_policy_condition) -> "MalformedPolicy";
error_code(no_such_bucket_policy) -> "NoSuchBucketPolicy";
error_code(no_such_upload) -> "NoSuchUpload";
error_code(invalid_digest) -> "InvalidDigest";
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
status_code(malformed_policy_condition) -> 400;
status_code(no_such_bucket_policy) -> 404;
status_code(no_such_upload) -> 404;
status_code(invalid_digest) -> 400;
status_code(bad_request) -> 400;
status_code(invalid_argument) -> 400;
status_code(unresolved_grant_email) -> 400;
status_code(invalid_range) -> 416;
status_code(invalid_bucket_name) -> 400;
status_code(_) -> 503.
