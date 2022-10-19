%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%               2021 TI Tokyo    All Rights Reserved.
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

-module(stanchion_response).

-export([api_error/3,
         respond/4,
         error_response/5,
         list_buckets_response/3]).

-define(xml_prolog, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>").

error_message(invalid_access_key_id) ->
    "The AWS Access Key Id you provided does not exist in our records.";
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
error_message(no_such_bucket) ->
    "The specified bucket does not exist.";
error_message({riak_connect_failed, Reason}) ->
    io_lib:format("Unable to establish connection to Riak. Reason: ~p", [Reason]);
error_message({unsatisfied_constraint, Constraint}) ->
    io_lib:format("Unable to complete operation due to ~s constraint violation.", [Constraint]);
error_message(multipart_upload_remains) ->
    "Multipart uploads still remaining.";
error_message(unknown_error) ->
    "Unexpected error occurred. Please see the stanchion error log for more details.".

error_code(invalid_access_key_id) -> "InvalidAccessKeyId";
error_code(access_denied) -> "AccessDenied";
error_code(bucket_not_empty) -> "BucketNotEmpty";
error_code(bucket_already_exists) -> "BucketAlreadyExists";
error_code(user_already_exists) -> "UserAlreadyExists";
error_code(entity_too_large) -> "EntityTooLarge";
error_code(no_such_bucket) -> "NoSuchBucket";
error_code({riak_connect_failed, _}) -> "RiakConnectFailed";
error_code({unsatisfied_constraint, _}) -> "UnsatisfiedConstraint";
error_code(multipart_upload_remains) -> "MultipartUploadRemaining";
error_code(unknown_error) -> "UnexpectedError".

status_code(access_denied) ->  403;
status_code(bucket_not_empty) ->  409;
status_code(bucket_already_exists) -> 409;
status_code(user_already_exists) -> 409;
status_code(entity_too_large) -> 400;
status_code(invalid_access_key_id) -> 403;
status_code(no_such_bucket) -> 404;
status_code({riak_connect_failed, _}) -> 503;
status_code({unsatisfied_constraint, _}) -> 500;
status_code(multipart_upload_remains) -> 409;
status_code(unknown_error) -> 500.

respond(StatusCode, Body, ReqData, Ctx) ->
    {{halt, StatusCode}, wrq:set_resp_body(Body, ReqData), Ctx}.

api_error(Error, ReqData, Ctx) when is_binary(Error) ->
    api_error(binary_to_list(Error), ReqData, Ctx);
api_error(Error, ReqData, Ctx) when is_list(Error) ->
    api_error(error_string_to_atom(Error), ReqData, Ctx);
api_error(Error, ReqData, Ctx) ->
    error_response(status_code(Error), error_code(Error), error_message(Error),
                   ReqData, Ctx).

error_response(StatusCode, Code, Message, RD, Ctx) ->
    XmlDoc = [{'Error', [{'Code', [Code]},
                        {'Message', [Message]},
                        {'Resource', [wrq:path(RD)]},
                        {'RequestId', [""]}]}],
    respond(StatusCode, export_xml(XmlDoc), RD, Ctx).


list_buckets_response(BucketData, RD, Ctx) ->
    BucketsDoc = [{'Bucket',
                   [{'Name', [binary_to_list(Bucket)]},
                    {'Owner', [binary_to_list(Owner)]}]}
                  || {Bucket, Owner} <- BucketData],
    Contents = [{'Buckets', BucketsDoc}],
    XmlDoc = [{'ListBucketsResult',  Contents}],
    respond(200, export_xml(XmlDoc), RD, Ctx).

export_xml(XmlDoc) ->
    unicode:characters_to_binary(
      xmerl:export_simple(XmlDoc, xmerl_xml, [{prolog, ?xml_prolog}])).

error_string_to_atom("{r_val_unsatisfied," ++ _) ->
    {unsatisfied_constraint, "r"};
error_string_to_atom("{w_val_unsatisfied," ++ _) ->
    {unsatisfied_constraint, "w"};
error_string_to_atom("{pr_val_unsatisfied," ++ _) ->
    {unsatisfied_constraint, "pr"};
error_string_to_atom("{pw_val_unsatisfied," ++ _) ->
    {unsatisfied_constraint, "pw"};
error_string_to_atom(_) ->
    unknown_error.
