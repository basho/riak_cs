%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.  All Rights Reserved,
%%               2021-2023 TI Tokyo    All Rights Reserved.
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

-module(riak_cs_aws_response).

-export([api_error/3,
         status_code/1,
         respond/3,
         respond/4,
         error_message/1,
         error_code/1,
         velvet_response/1,
         copy_object_response/3,
         copy_part_response/3,
         no_such_upload_response/3,
         invalid_digest_response/3]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("xmerl/include/xmerl.hrl").
-include_lib("kernel/include/logger.hrl").

-spec error_message(error_reason()) -> string().
error_message(invalid_access_key_id) ->
    "The AWS Access Key Id you provided does not exist in our records.";
error_message(invalid_email_address) ->
    "The email address you provided is not a valid.";
error_message(access_denied) ->
    "Access Denied";
error_message(copy_source_access_denied) ->
    "Access Denied";
error_message(reqtime_tooskewed) ->
    "The difference between the request time and the current time is too large.";
error_message(bucket_not_empty) ->
    "The bucket you tried to delete is not empty.";
error_message(bucket_already_exists) ->
    "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.";
error_message(toomanybuckets) ->
    "You have attempted to create more buckets than allowed";
error_message({key_too_long, _}) ->
    "Your key is too long";
error_message(user_already_exists) ->
    "The specified email address has already been registered. Email addresses must be unique among all users of the system. Please try again with a different email address.";
error_message(entity_too_large) ->
    "Your proposed upload exceeds the maximum allowed object size.";
error_message(entity_too_small) ->
    "Your proposed upload is smaller than the minimum allowed object size. Each part must be at least 5 MB in size, except the last part.";
error_message(invalid_user_update) ->
    "The user update you requested was invalid.";
error_message(invalid_role_parameters) ->
    "Incomplete or invalid role parameters.";
error_message(no_such_bucket) ->
    "The specified bucket does not exist.";
error_message({riak_connect_failed, Reason}) ->
    io_lib:format("Unable to establish connection to Riak. Reason: ~p", [Reason]);
error_message(admin_key_undefined) -> "Please reduce your request rate.";
error_message(admin_secret_undefined) -> "Please reduce your request rate.";
error_message(bucket_owner_unavailable) -> "The user record for the bucket owner was unavailable. Try again later.";
error_message(econnrefused) -> "Please reduce your request rate.";
error_message(malformed_policy_json) -> "JSON parsing error";
error_message({malformed_policy_version, Version}) ->
    io_lib:format("Document is invalid: Invalid Version ~s", [Version]);
error_message({auth_not_supported, AuthType}) ->
    io_lib:format("The authorization mechanism you have provided (~s) is not supported.", [AuthType]);
error_message(malformed_policy_missing) -> "Policy is missing required element";
error_message(malformed_policy_resource) -> "Policy has invalid resource";
error_message(malformed_policy_principal) -> "Invalid principal in policy";
error_message(malformed_policy_action) -> "Policy has invalid action";
error_message(malformed_policy_condition) -> "Policy has invalid condition";
error_message(no_such_key) -> "The specified key does not exist.";
error_message(no_copy_source_key) -> "The specified key does not exist.";
error_message(no_such_bucket_policy) -> "The specified bucket does not have a bucket policy.";
error_message(no_such_upload) ->
    "The specified upload does not exist. The upload ID may be invalid, "
        "or the upload may have been aborted or completed.";
error_message(invalid_digest) ->
    "The Content-MD5 you specified was invalid.";
error_message(bad_request) -> "Bad Request";
error_message(invalid_argument) -> "Invalid Argument";
error_message({invalid_argument, "x-amz-metadata-directive"}) ->
    "Unknown metadata directive.";
error_message(unresolved_grant_email) -> "The e-mail address you provided does not match any account on record.";
error_message(invalid_range) -> "The requested range is not satisfiable";
error_message(invalid_bucket_name) -> "The specified bucket is not valid.";
error_message(invalid_part_number) -> "Part number must be an integer between 1 and 10000, inclusive";
error_message(unexpected_content) -> "This request does not support content";
error_message(canned_acl_and_header_grant) -> "Specifying both Canned ACLs and Header Grants is not allowed";
error_message(malformed_xml) -> "The XML you provided was not well-formed or did not validate against our published schema";
error_message(no_such_role) -> "No such role";
error_message(no_such_saml_provider) -> "No such SAML provider";
error_message(role_already_exists) -> "Role already exists";
error_message(policy_already_exists) -> "Policy already exists";
error_message(policy_not_attachable) -> "Service role policies can only be attached to the service-linked role for that service";
error_message(unmodifiable_entity) -> "Service-linked roles are protected";
error_message(saml_provider_already_exists) -> "SAML provider already exists";
error_message(invalid_metadata_document) -> "IdP provided invalid or unacceptable metadata document";
error_message(idp_rejected_claim) -> "The IdP reported that authentication failed, or the certificate in the SAML assertion is invalid";
error_message(remaining_multipart_upload) -> "Concurrent multipart upload initiation detected. Please stop it to delete bucket.";
error_message(disconnected) -> "Please contact administrator.";
error_message(stanchion_recovery_failure) -> "Bucket and user operations are temporarily unavailable because the node running stanchion is currently unreachable. Please report this to your administrator.";
error_message(invalid_action) -> "This Action is invalid or not yet supported";
error_message(invalid_parameter_value) -> "Unacceptable parameter value";
error_message(missing_parameter) -> "Missing parameter";
error_message(temp_users_create_bucket_restriction) -> "Federated users with assumed roles cannot create buckets";
error_message({unsatisfied_constraint, Constraint}) ->
    io_lib:format("Unable to complete operation due to ~s constraint violation.", [Constraint]);
error_message(not_implemented) -> "A request you provided implies functionality that is not implemented";
error_message(ErrorName) ->
    logger:warning("Unknown error: ~p", [ErrorName]),
    "Please reduce your request rate.".

-spec error_code(error_reason()) -> string().
error_code(invalid_access_key_id) -> "InvalidAccessKeyId";
error_code(access_denied) -> "AccessDenied";
error_code(copy_source_access_denied) -> "AccessDenied";
error_code(reqtime_tooskewed) -> "RequestTimeTooSkewed";
error_code(bucket_not_empty) -> "BucketNotEmpty";
error_code(bucket_already_exists) -> "BucketAlreadyExists";
error_code(toomanybuckets) -> "TooManyBuckets";
error_code({key_too_long, _}) -> "KeyTooLongError";
error_code(user_already_exists) -> "UserAlreadyExists";
error_code(entity_too_large) -> "EntityTooLarge";
error_code(entity_too_small) -> "EntityTooSmall";
error_code(bad_etag) -> "InvalidPart";
error_code(bad_etag_order) -> "InvalidPartOrder";
error_code(invalid_user_update) -> "InvalidUserUpdate";
error_code(no_such_bucket) -> "NoSuchBucket";
error_code(no_such_key) -> "NoSuchKey";
error_code(no_copy_source_key) -> "NoSuchKey";
error_code({riak_connect_failed, _}) -> "RiakConnectFailed";
error_code({unsatisfied_constraint, _}) -> "UnsatisfiedConstraint";
error_code(admin_key_undefined) -> "ServiceUnavailable";
error_code(admin_secret_undefined) -> "ServiceUnavailable";
error_code(bucket_owner_unavailable) -> "ServiceUnavailable";
error_code(econnrefused) -> "ServiceUnavailable";
error_code(malformed_policy_json) -> "MalformedPolicy";
error_code(malformed_policy_missing) -> "MalformedPolicy";
error_code({malformed_policy_version, _}) -> "MalformedPolicy";
error_code({auth_not_supported, _}) -> "InvalidRequest";
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
error_code(invalid_part_number) -> "InvalidArgument";
error_code(unresolved_grant_email) -> "UnresolvableGrantByEmailAddress";
error_code(unexpected_content) -> "UnexpectedContent";
error_code(canned_acl_and_header_grant) -> "InvalidRequest";
error_code(malformed_acl_error) -> "MalformedACLError";
error_code(malformed_xml) -> "MalformedXML";
error_code(no_such_role) -> "NoSuchEntity";
error_code(no_such_saml_provider) -> "NoSuchEntity";
error_code(role_already_exists) -> "EntityAlreadyExists";
error_code(policy_already_exists) -> "EntityAlreadyExists";
error_code(policy_not_attachable) -> "PolicyNotAttachable";
error_code(unmodifiable_entity) -> "UnmodifiableEntity";
error_code(saml_provider_already_exists) -> "EntityAlreadyExists";
error_code(invalid_metadata_document) -> "InvalidInput";
error_code(idp_rejected_claim) -> "IDPRejectedClaim";
error_code(remaining_multipart_upload) -> "MultipartUploadRemaining";
error_code(disconnected) -> "ServiceUnavailable";
error_code(stanchion_recovery_failure) -> "ServiceDegraded";
error_code(invalid_action) -> "InvalidAction";
error_code(invalid_parameter_value) -> "InvalidParameterValue";
error_code(missing_parameter) -> "MissingParameter";
error_code(temp_users_create_bucket_restriction) -> "NotImplemented";
error_code(not_implemented) -> "NotImplemented";
error_code(ErrorName) ->
    logger:warning("Unknown error: ~p", [ErrorName]),
    "ServiceUnavailable".

%% These should match:
%% http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html

-spec status_code(error_reason()) -> pos_integer().
status_code(invalid_access_key_id)         -> 403;
status_code(invalid_email_address)         -> 400;
status_code(access_denied)                 -> 403;
status_code(copy_source_access_denied)     -> 403;
status_code(reqtime_tooskewed)             -> 403;
status_code(bucket_not_empty)              -> 409;
status_code(bucket_already_exists)         -> 409;
status_code(user_already_exists)           -> 409;
status_code(toomanybuckets)                -> 400;
status_code({key_too_long, _})             -> 400;
%% yes, 400, really, not 413
status_code(entity_too_large)              -> 400;
status_code(entity_too_small)              -> 400;
status_code(bad_etag)                      -> 400;
status_code(bad_etag_order)                -> 400;
status_code(invalid_user_update)           -> 400;
status_code(no_such_bucket)                -> 404;
status_code(no_such_key)                   -> 404;
status_code(no_copy_source_key)            -> 404;
status_code(stanchion_recovery_failure)    -> 503;
status_code({riak_connect_failed, _})      -> 503;
status_code(admin_key_undefined)           -> 503;
status_code(admin_secret_undefined)        -> 503;
status_code(bucket_owner_unavailable)      -> 503;
status_code(multiple_bucket_owners)        -> 503;
status_code(econnrefused)                  -> 503;
status_code(unsatisfied_constraint)        -> 503;
status_code(malformed_policy_json)         -> 400;
status_code({malformed_policy_version, _}) -> 400;
status_code(malformed_policy_missing)      -> 400;
status_code(malformed_policy_resource)     -> 400;
status_code(malformed_policy_principal)    -> 400;
status_code(malformed_policy_action)       -> 400;
status_code(malformed_policy_condition)    -> 400;
status_code({auth_not_supported, _})       -> 400;
status_code(no_such_bucket_policy)         -> 404;
status_code(no_such_upload)                -> 404;
status_code(invalid_digest)                -> 400;
status_code(bad_request)                   -> 400;
status_code(invalid_argument)              -> 400;
status_code(unresolved_grant_email)        -> 400;
status_code(invalid_range)                 -> 416;
status_code(invalid_bucket_name)           -> 400;
status_code(invalid_part_number)           -> 400;
status_code(unexpected_content)            -> 400;
status_code(canned_acl_and_header_grant)   -> 400;
status_code(malformed_acl_error)           -> 400;
status_code(malformed_xml)                 -> 400;
status_code(no_such_role)                  -> 404;
status_code(no_such_saml_provider)         -> 404;
status_code(role_already_exists)           -> 409;
status_code(policy_already_exists)         -> 409;
status_code(policy_not_attachable)         -> 400;
status_code(unmodifiable_entity)           -> 400;
status_code(saml_provider_already_exists)  -> 409;
status_code(invalid_metadata_document)     -> 400;
status_code(idp_rejected_claim)            -> 403;
status_code(remaining_multipart_upload)    -> 409;
status_code(disconnected)                  -> 500;
status_code(invalid_action)                -> 400;
status_code(invalid_parameter_value)       -> 400;
status_code(missing_parameter)             -> 400;
status_code(temp_users_create_bucket_restriction) -> 403;
status_code(not_implemented)                      -> 501;
status_code(ErrorName) ->
    logger:warning("Unknown error: ~p", [ErrorName]),
    503.

-spec respond(term(), #wm_reqdata{}, #rcs_web_context{}) ->
                     {binary(), #wm_reqdata{}, #rcs_web_context{}}.
respond(?LBRESP{} = Response, RD, Ctx) ->
    {riak_cs_xml:to_xml(Response), RD, Ctx};
respond({ok, ?LORESP{} = Response}, RD, Ctx) ->
    {riak_cs_xml:to_xml(Response), RD, Ctx};
respond({ok, ?LOVRESP{} = Response}, RD, Ctx) ->
    {riak_cs_xml:to_xml(Response), RD, Ctx};
respond({error, _} = Error, RD, Ctx) ->
    api_error(Error, RD, Ctx).

respond(404 = _StatusCode, Body, ReqData, Ctx) ->
    respond({404, "Not Found"}, Body, ReqData, Ctx);
respond(StatusCode, Body, ReqData, Ctx) ->
     UpdReqData = wrq:set_resp_body(Body,
                                    wrq:set_resp_header("Content-Type",
                                                        ?XML_TYPE,
                                                        ReqData)),
    {{halt, StatusCode}, UpdReqData, Ctx}.

api_error(Error, RD, Ctx) when is_atom(Error) ->
    error_response(status_code(Error),
                   error_code(Error),
                   error_message(Error),
                   RD,
                   Ctx);
api_error({Tag, _} = Error, RD, Ctx)
  when Tag =:= riak_connect_failed orelse
       Tag =:= malformed_policy_version orelse
       Tag =:= auth_not_supported ->
    error_response(status_code(Error),
                   error_code(Error),
                   error_message(Error),
                   RD,
                   Ctx);
api_error({toomanybuckets, Current, BucketLimit}, RD, Ctx) ->
    toomanybuckets_response(Current, BucketLimit, RD, Ctx);
api_error({invalid_argument, Name, Value}, RD, Ctx) ->
    invalid_argument_response(Name, Value, RD, Ctx);
api_error({key_too_long, Len}, RD, Ctx) ->
    key_too_long(Len, RD, Ctx);
api_error(stanchion_recovery_failure, RD, Ctx) ->
    stanchion_recovery_failure(RD, Ctx);
api_error({error, Reason}, RD, Ctx) ->
    api_error(Reason, RD, Ctx).

error_response(StatusCode, Code, Message, RD, Ctx) ->
    XmlDoc = [{'Error', [{'Code', [Code]},
                         {'Message', [Message]}
                        ] ++ common_response_items(Ctx)}
             ],
    respond(StatusCode, riak_cs_xml:to_xml(XmlDoc), RD, Ctx).

-spec velvet_response(string()) -> {error, error_reason()}.
velvet_response(Response) ->
    ?LOG_DEBUG("AAAAaaa ~p", [Response]),
    #{error_tag := Tag,
      resource := _Resource} = jsx:decode(list_to_binary(Response), [{labels, atom}]),
    {error, binary_to_term(base64:decode(Tag))}.


%% error_resource(Tag, RD)
%%   when Tag =:= no_copy_source_key;
%%        Tag =:= copy_source_access_denied->
%%     {B, K, V} = riak_cs_copy_object:get_copy_source(RD),
%%     case V of
%%         ?LFS_DEFAULT_OBJECT_VERSION ->
%%             <<$/, B/binary, $/, K/binary>>;
%%         _ ->
%%             <<$/, B/binary, $/, K/binary, $/, V/binary>>
%%     end;

%% error_resource(_Tag, RD) ->
%%     {OrigResource, _} = riak_cs_rewrite:original_resource(RD),
%%     OrigResource.

toomanybuckets_response(Current, BucketLimit, RD, Ctx) ->
    XmlDoc = [{'Error', [{'Code', [error_code(toomanybuckets)]},
                         {'Message', [error_message(toomanybuckets)]},
                         {'CurrentNumberOfBuckets', [Current]},
                         {'AllowedNumberOfBuckets', [BucketLimit]}
                        ] ++ common_response_items(Ctx)}
             ],
    respond(status_code(toomanybuckets), riak_cs_xml:to_xml(XmlDoc), RD, Ctx).

invalid_argument_response(Name, Value, RD, Ctx) ->
    XmlDoc = [{'Error', [{'Code', [error_code(invalid_argument)]},
                         {'Message', [error_message({invalid_argument, Name})]},
                         {'ArgumentName', [Name]},
                         {'ArgumentValue', [Value]}
                        ] ++ common_response_items(Ctx)}
             ],
    respond(status_code(invalid_argument), riak_cs_xml:to_xml(XmlDoc), RD, Ctx).

key_too_long(Len, RD, Ctx) ->
    XmlDoc = [{'Error', [{'Code', [error_code({key_too_long, Len})]},
                         {'Message', [error_message({key_too_long, Len})]},
                         {'Size', [Len]},
                         {'MaxSizeAllowed', [riak_cs_config:max_key_length()]}
                        ] ++ common_response_items(Ctx)}
             ],
    respond(status_code(invalid_argument), riak_cs_xml:to_xml(XmlDoc), RD, Ctx).

stanchion_recovery_failure(RD, Ctx) ->
    XmlDoc = [{'Error', [{'Code', [error_code(stanchion_recovery_failure)]},
                         {'Message', [error_message(stanchion_recovery_failure)]}
                        ] ++ common_response_items(Ctx)}
             ],
    respond(status_code(invalid_argument), riak_cs_xml:to_xml(XmlDoc), RD, Ctx).

copy_object_response(Manifest, RD, Ctx) ->
    copy_response(Manifest, 'CopyObjectResult', RD, Ctx).

copy_part_response(Manifest, RD, Ctx) ->
    copy_response(Manifest, 'CopyPartResult', RD, Ctx).

copy_response(Manifest, TagName, RD, Ctx) ->
    LastModified = riak_cs_wm_utils:to_iso_8601(Manifest?MANIFEST.created),
    ETag = riak_cs_manifest:etag(Manifest),
    XmlDoc = [{TagName, [{'LastModified', [LastModified]},
                         {'ETag', [ETag]}
                        ] ++ common_response_items(Ctx)}
             ],
    respond(200, riak_cs_xml:to_xml(XmlDoc), RD, Ctx).


no_such_upload_response(InternalUploadId, RD, Ctx) ->
    UploadId = case InternalUploadId of
                   {raw, ReqUploadId} -> ReqUploadId;
                   _ -> base64url:encode(InternalUploadId)
               end,
    XmlDoc = [{'Error', [{'Code', [error_code(no_such_upload)]},
                         {'Message', [error_message(no_such_upload)]},
                         {'UploadId', [UploadId]}
                        ] ++ common_response_items(Ctx)}
             ],
    respond(status_code(no_such_upload), riak_cs_xml:to_xml(XmlDoc), RD, Ctx).

invalid_digest_response(ContentMd5, RD, Ctx) ->
    XmlDoc = [{'Error', [{'Code', [error_code(invalid_digest)]},
                         {'Message', [error_message(invalid_digest)]},
                         {'Content-MD5', [ContentMd5]}
                        ] ++ common_response_items(Ctx)}
             ],
    respond(status_code(invalid_digest), riak_cs_xml:to_xml(XmlDoc), RD, Ctx).


common_response_items(#rcs_web_context{request_id = RequestId,
                                       user = User}) ->
    [{'RequestId', [binary_to_list(RequestId)]},
     {'AWSAccessKeyId', [user_access_key(User)]},
     {'HostId', [riak_cs_config:host_id()]}
    ].

user_access_key(?RCS_USER{key_id = KeyId}) when KeyId /= undefined ->
    KeyId;
user_access_key(_) ->
    "NoKeyId".
