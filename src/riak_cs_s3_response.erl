%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(riak_cs_s3_response).
-export([api_error/3,
         status_code/1,
         respond/4,
         export_xml/1,
         error_response/5,
         list_bucket_response/5,
         list_all_my_buckets_response/3,
         error_code_to_atom/1]).

-include("riak_cs.hrl").

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
error_message(invalid_user_update) ->
    "The user update you request was invalid.";
error_message(no_such_bucket) ->
    "The specified bucket does not exist.";
error_message({riak_connect_failed, Reason}) ->
    io_lib:format("Unable to establish connection to Riak. Reason: ~p", [Reason]);
error_message(admin_key_undefined) -> "Please reduce your request rate.";
error_message(admin_secret_undefined) -> "Please reduce your request rate.";
error_message(bucket_owner_unavailable) -> "The user record for the bucket owner was unavailable. Try again later.";
error_message(econnrefused) -> "Please reduce your request rate.";
error_message(_) -> "Please reduce your request rate.".

error_code(invalid_access_key_id) -> "InvalidAccessKeyId";
error_code(access_denied) -> "AccessDenied";
error_code(bucket_not_empty) -> "BucketNotEmpty";
error_code(bucket_already_exists) -> "BucketAlreadyExists";
error_code(user_already_exists) -> "UserAlreadyExists";
error_code(entity_too_large) -> "EntityTooLarge";
error_code(invalid_user_update) -> "InvalidUserUpdate";
error_code(no_such_bucket) -> "NoSuchBucket";
error_code({riak_connect_failed, _}) -> "RiakConnectFailed";
error_code(admin_key_undefined) -> "ServiceUnavailable";
error_code(admin_secret_undefined) -> "ServiceUnavailable";
error_code(bucket_owner_unavailable) -> "ServiceUnavailable";
error_code(econnrefused) -> "ServiceUnavailable";
error_code(_) -> "ServiceUnavailable".

status_code(invalid_access_key_id) -> 403;
status_code(invalid_email_address) -> 400;
status_code(access_denied) ->  403;
status_code(bucket_not_empty) ->  409;
status_code(bucket_already_exists) -> 409;
status_code(user_already_exists) -> 409;
status_code(entity_too_large) -> 400;
status_code(invalid_user_update) -> 400;
status_code(no_such_bucket) -> 404;
status_code({riak_connect_failed, _}) -> 503;
status_code(admin_key_undefined) -> 503;
status_code(admin_secret_undefined) -> 503;
status_code(bucket_owner_unavailable) -> 503;
status_code(econnrefused) -> 503;
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

error_response(StatusCode, Code, Message, RD, Ctx) ->
    XmlDoc = [{'Error', [{'Code', [Code]},
                         {'Message', [Message]},
                         {'Resource', [wrq:path(RD)]},
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
    XmlDoc = [{'ListAllMyBucketsResult',  Contents}],
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
                            ETag = "\"" ++ riak_cs_utils:binary_to_hexlist(
                                             Manifest?MANIFEST.content_md5)
                                ++ "\"",
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

export_xml(XmlDoc) ->
    unicode:characters_to_binary(
      xmerl:export_simple(XmlDoc, xmerl_xml, [{prolog, ?XML_PROLOG}]), unicode, unicode).

%% @doc Convert an error code string into its corresponding atom
-spec error_code_to_atom(string()) -> atom().
error_code_to_atom(ErrorCode) ->
    case ErrorCode of
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
