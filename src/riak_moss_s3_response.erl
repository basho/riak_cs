%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(riak_moss_s3_response).
-export([api_error/3,
         respond/4,
         error_response/5,
         list_bucket_response/5,
         list_all_my_buckets_response/3,
         error_code_to_atom/1]).

-include("riak_moss.hrl").

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
error_message(entity_too_large) ->
    "Your proposed upload exceeds the maximum allowed object size.";
error_message(no_such_bucket) ->
    "The specified bucket does not exist.";
error_message({riak_connect_failed, Reason}) ->
    io_lib:format("Unable to establish connection to Riak. Reason: ~p", [Reason]);
error_message(admin_key_undefined) -> "Please reduce your request rate.";
error_message(admin_secret_undefined) -> "Please reduce your request rate.";
error_message(econnrefused) -> "Please reduce your request rate.".


error_code(invalid_access_key_id) -> "InvalidAccessKeyId";
error_code(access_denied) -> "AccessDenied";
error_code(bucket_not_empty) -> "BucketNotEmpty";
error_code(bucket_already_exists) -> "BucketAlreadyExists";
error_code(entity_too_large) -> "EntityTooLarge";
error_code(no_such_bucket) -> "NoSuchBucket";
error_code({riak_connect_failed, _}) -> "RiakConnectFailed";
error_code(admin_key_undefined) -> "ServiceUnavailable";
error_code(admin_secret_undefined) -> "ServiceUnavailable";
error_code(econnrefused) -> "ServiceUnavailable".


status_code(invalid_access_key_id) -> 403;
status_code(invalid_email_address) -> 400;
status_code(access_denied) ->  403;
status_code(bucket_not_empty) ->  409;
status_code(bucket_already_exists) -> 409;
status_code(entity_too_large) -> 400;
status_code(no_such_bucket) -> 404;
status_code({riak_connect_failed, _}) -> 503;
status_code(admin_key_undefined) -> 503;
status_code(admin_secret_undefined) -> 503;
status_code(econnrefused) -> 503.


respond(StatusCode, Body, ReqData, Ctx) ->
    {{halt, StatusCode}, wrq:set_resp_body(Body, ReqData), Ctx}.

api_error(Error, ReqData, Ctx) when is_atom(Error) ->
    error_response(status_code(Error), error_code(Error), error_message(Error),
                   ReqData, Ctx).

error_response(StatusCode, Code, Message, RD, Ctx) ->
    XmlDoc = [{'Error', [{'Code', [Code]},
                         {'Message', [Message]},
                         {'Resource', [wrq:path(RD)]},
                         {'RequestId', [""]}]}],
    respond(StatusCode, export_xml(XmlDoc), RD, Ctx).


list_all_my_buckets_response(User, RD, Ctx) ->
    BucketsDoc = [{'Bucket',
                   [{'Name', [B?MOSS_BUCKET.name]},
                    {'CreationDate', [B?MOSS_BUCKET.creation_date]}]}
                  || B <- riak_moss_utils:get_buckets(User)],
    Contents =  [user_to_xml_owner(User)] ++ [{'Buckets', BucketsDoc}],
    XmlDoc = [{'ListAllMyBucketsResult',  Contents}],
    respond(200, export_xml(XmlDoc), RD, Ctx).

list_bucket_response(User, Bucket, KeyObjPairs, RD, Ctx) ->
    %% @TODO Once the optimization for storing small objects
    %% is completed, a check will be required either here or
    %% in `riak_moss_lfs_utils' to determine if the object
    %% associated with each key is an `lfs_manifest' or not.
    Contents = [begin
                    KeyString = binary_to_list(Key),
                    case ObjResp of
                        {ok, Obj} ->
                            Manifest = binary_to_term(riakc_obj:get_value(Obj)),
                            case riak_moss_lfs_utils:is_active(Manifest) of
                                true ->
                                    Size = integer_to_list(
                                             riak_moss_lfs_utils:content_length(Manifest)),
                                    LastModified =
                                        riak_moss_wm_utils:to_iso_8601(
                                          riak_moss_lfs_utils:created(Manifest)),
                                    ETag = "\"" ++ riak_moss_utils:binary_to_hexlist(
                                                     riak_moss_lfs_utils:content_md5(Manifest))
                                        ++ "\"",
                                    {'Contents', [{'Key', [KeyString]},
                                                  {'Size', [Size]},
                                                  {'LastModified', [LastModified]},
                                                  {'ETag', [ETag]},
                                                  {'Owner', [user_to_xml_owner(User)]}]};
                                false ->
                                    undefined
                            end;
                        {error, Reason} ->
                            lager:warning("Unable to fetch object for ~p. Reason: ~p",
                                          [Key, Reason]),
                            undefined
                    end
                end
                || {Key, ObjResp} <- KeyObjPairs],
    BucketProps = [{'Name', [Bucket?MOSS_BUCKET.name]},
                   {'Prefix', []},
                   {'Marker', []},
                   {'MaxKeys', ["1000"]},
                   {'Delimiter', ["/"]},
                   {'IsTruncated', ["false"]}],
    XmlDoc = [{'ListBucketResult', BucketProps++
                   lists:filter(fun(E) -> E /= undefined end,
                                Contents)}],
    respond(200, export_xml(XmlDoc), RD, Ctx).

user_to_xml_owner(?MOSS_USER{canonical_id=CanonicalId, display_name=Name}) ->
    {'Owner', [{'ID', [CanonicalId]},
               {'DisplayName', [Name]}]}.

export_xml(XmlDoc) ->
    unicode:characters_to_binary(
      xmerl:export_simple(XmlDoc, xmerl_xml, [{prolog, ?XML_PROLOG}])).

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
        "NoSuchBucket" ->
            no_such_bucket;
        _ ->
            unknown
    end.
