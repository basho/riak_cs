%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_wm_bucket).

-export([init/1,
         service_available/2,
         forbidden/2,
         content_types_provided/2,
         malformed_request/2,
         to_xml/2,
         allowed_methods/2,
         content_types_accepted/2,
         accept_body/2,
         delete_resource/2]).

-include("riak_moss.hrl").
-include_lib("webmachine/include/webmachine.hrl").


init(Config) ->
    %% Check if authentication is disabled and
    %% set that in the context.
    AuthBypass = proplists:get_value(auth_bypass, Config),
    {ok, #context{auth_bypass=AuthBypass}}.

-spec service_available(term(), term()) -> {true, term(), term()}.
service_available(RD, Ctx) ->
    riak_moss_wm_utils:service_available(RD, Ctx).

-spec malformed_request(term(), term()) -> {false, term(), term()}.
malformed_request(RD, Ctx) ->
    {false, RD, Ctx}.

%% @doc Check to see if the user is
%%      authenticated. Normally with HTTP
%%      we'd use the `authorized` callback,
%%      but this is how S3 does things.
forbidden(RD, Ctx=#context{auth_bypass=AuthBypass}) ->
    AuthHeader = wrq:get_req_header("authorization", RD),
    case riak_moss_wm_utils:parse_auth_header(AuthHeader, AuthBypass) of
        {ok, AuthMod, Args} ->
            case AuthMod:authenticate(RD, Args) of
                {ok, User} ->
                    %% Authentication succeeded
                    {false, RD, Ctx#context{user=User}};
                {error, _Reason} ->
                    %% Authentication failed, deny access
                    riak_moss_s3_response:api_error(access_denied, RD, Ctx)
            end
    end.

%% @doc Get the list of methods this resource supports.
-spec allowed_methods(term(), term()) -> {[atom()], term(), term()}.
allowed_methods(RD, Ctx) ->
    %% TODO: add POST
    %% TODO: make this list conditional on Ctx
    {['HEAD', 'GET', 'PUT', 'DELETE'], RD, Ctx}.

-spec content_types_provided(term(), term()) ->
    {[{string(), atom()}], term(), term()}.
content_types_provided(RD, Ctx) ->
    %% TODO:
    %% Add xml support later

    %% TODO:
    %% The subresource will likely affect
    %% the content-type. Need to look
    %% more into those.
    {[{"application/xml", to_xml}], RD, Ctx}.

%% @spec content_types_accepted(reqdata(), context()) ->
%%          {[{ContentType::string(), Acceptor::atom()}],
%%           reqdata(), context()}
content_types_accepted(RD, Ctx) ->
    case wrq:get_req_header("content-type", RD) of
        undefined ->
            {[{"application/octet-stream", accept_body}], RD, Ctx};
        CType ->
            {[{CType, accept_body}], RD, Ctx}
    end.


-spec to_xml(term(), term()) ->
    {iolist(), term(), term()}.
to_xml(RD, Ctx=#context{user=User}) ->
    BucketName = wrq:path_info(bucket, RD),
    Bucket = hd([B || B <- riak_moss_utils:get_buckets(User), B?MOSS_BUCKET.name =:= BucketName]),
    MOSSBucket = riak_moss_utils:to_bucket_name(objects, list_to_binary(Bucket?MOSS_BUCKET.name)),
    Prefix = list_to_binary(wrq:get_qs_value("prefix", "", RD)),
    case riak_moss_utils:get_keys_and_objects(MOSSBucket, Prefix) of
        {ok, KeyObjPairs} ->
            riak_moss_s3_response:list_bucket_response(User,
                                                       Bucket,
                                                       KeyObjPairs,
                                                       RD,
                                                       Ctx);
        {error, Reason} ->
            riak_moss_s3_response:api_error(Reason, RD, Ctx)
    end.

%% TODO:
%% Add content_types_accepted when we add
%% in PUT and POST requests.
accept_body(ReqData, Ctx=#context{user=User}) ->
    %% @TODO Check for `x-amz-acl' header to support
    %% non-default ACL at bucket creation time.
    ACL = riak_moss_acl_utils:default_acl(User?MOSS_USER.display_name,
                                          User?MOSS_USER.canonical_id),
    case riak_moss_utils:create_bucket(User?MOSS_USER.key_id,
                                       wrq:path_info(bucket, ReqData),
                                       ACL) of
        ok ->
            {{halt, 200}, ReqData, Ctx};
        ignore ->
            riak_moss_s3_response:api_error(bucket_already_exists, ReqData, Ctx);
        {error, Reason} ->
            riak_moss_s3_response:api_error(Reason, ReqData, Ctx)
    end.

%% @doc Callback for deleting a bucket.
-spec delete_resource(term(), term()) -> boolean().
delete_resource(ReqData, Ctx=#context{user=User}) ->
    BucketName = list_to_binary(wrq:path_info(bucket, ReqData)),
    case riak_moss_utils:delete_bucket(User?MOSS_USER.key_id,
                                       BucketName) of
        ok ->
            {true, ReqData, Ctx};
        {error, Reason} ->
            riak_moss_s3_response:api_error(Reason, ReqData, Ctx)
    end.
