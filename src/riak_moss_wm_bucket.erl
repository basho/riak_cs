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
forbidden(RD, Ctx) ->
    riak_moss_wm_utils:find_and_auth_user(RD, Ctx, fun check_permission/2).

check_permission(RD, #context{user=User}=Ctx) ->
    Method = wrq:method(RD),
    RequestedAccess =
        riak_moss_acl_utils:requested_access(Method, wrq:req_qs(RD)),
    Bucket = list_to_binary(wrq:path_info(bucket, RD)),
    PermCtx = Ctx#context{bucket=Bucket,
                          requested_perm=RequestedAccess},

    case {Method, RequestedAccess} of
        {_, 'WRITE'} when User == undefined ->
            %% unauthed users may neither create nor delete buckets
            riak_moss_wm_utils:deny_access(RD, PermCtx);
        {'PUT', 'WRITE'} ->
            %% authed users are always allowed to attempt bucket creation
            {false, RD, PermCtx};
        _ ->
            %% only owners are allowed to delete buckets
            case check_grants(PermCtx) of
                true ->
                    %% because users are not allowed to create/destroy
                    %% buckets, we can assume that User is not
                    %% undefined here
                    {false, RD, PermCtx};
                {true, _OwnerId} when RequestedAccess == 'WRITE' ->
                    %% grants lied: this is a delete, and only the
                    %% owner is allowed to do that; setting user for
                    %% the request anyway, so the error tally is
                    %% logged for them
                    riak_moss_wm_utils:deny_access(RD, PermCtx);
                {true, OwnerId} ->
                    %% this operation is allowed, but we need to get
                    %% the owner's record, and log the access against
                    %% them instead of the actor
                    shift_to_owner(RD, PermCtx, OwnerId);
                false ->
                    riak_moss_wm_utils:deny_access(RD, PermCtx)
            end
    end.

%% @doc Call the correct (anonymous or auth'd user) {@link
%% riak_moss_acl} function to check permissions for this request.
check_grants(#context{user=undefined,
                      bucket=Bucket,
                      requested_perm=RequestedAccess}) ->
    riak_moss_acl:anonymous_bucket_access(Bucket, RequestedAccess);
check_grants(#context{user=User,
                      bucket=Bucket,
                      requested_perm=RequestedAccess}) ->
    riak_moss_acl:bucket_access(Bucket,
                                RequestedAccess,
                                User?MOSS_USER.canonical_id).

%% @doc The {@link forbidden/2} decision passed, but the bucket
%% belongs to someone else.  Switch to it if the owner's record can be
%% retrieved.
shift_to_owner(RD, Ctx, OwnerId) ->
    case riak_moss_utils:get_user_by_index(?ID_INDEX,
                                           list_to_binary(OwnerId)) of
        {ok, {Owner, OwnerVClock}} ->
            {false, RD, Ctx#context{user=Owner,
                                    user_vclock=OwnerVClock}};
        {error, _} ->
            riak_moss_s3_response:api_error(bucket_owner_unavailable, RD, Ctx)
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
            {Media, _Params} = mochiweb_util:parse_header(CType),
            {[{Media, accept_body}], RD, Ctx}
    end.

-spec to_xml(term(), term()) ->
                    {iolist(), term(), term()}.
to_xml(RD, Ctx=#context{user=User,
                        bucket=Bucket,
                        requested_perm='READ'}) ->
    case [B || B <- riak_moss_utils:get_buckets(User),
               B?MOSS_BUCKET.name =:= Bucket] of
        [] ->
            riak_moss_s3_response:api_error(no_such_bucket, RD, Ctx);
        [BucketRecord] ->
            Prefix = list_to_binary(wrq:get_qs_value("prefix", "", RD)),
            case riak_moss_utils:get_keys_and_manifests(Bucket, Prefix) of
                {ok, KeyObjPairs} ->
                    riak_moss_s3_response:list_bucket_response(User,
                                                               BucketRecord,
                                                               KeyObjPairs,
                                                               RD,
                                                               Ctx);
                {error, Reason} ->
                    riak_moss_s3_response:api_error(Reason, RD, Ctx)
            end
    end;
to_xml(RD, Ctx=#context{bucket=Bucket,
                        requested_perm='READ_ACP'}) ->
    case riak_moss_acl:bucket_acl(Bucket) of
        {ok, Acl} ->
            {riak_moss_acl_utils:acl_to_xml(Acl), RD, Ctx};
        {error, Reason} ->
            riak_moss_s3_response:api_error(Reason, RD, Ctx)
    end.

%% @doc Process request body on `PUT' request.
accept_body(RD, Ctx=#context{user=User,
                             user_vclock=VClock,
                             bucket=Bucket,
                             requested_perm='WRITE_ACP'}) ->
    Body = binary_to_list(wrq:req_body(RD)),
    case Body of
        [] ->
            %% Check for `x-amz-acl' header to support
            %% the use of a canned ACL.
            ACL = riak_moss_acl_utils:canned_acl(
                    wrq:get_req_header("x-amz-acl", RD),
                    {User?MOSS_USER.display_name,
                     User?MOSS_USER.canonical_id},
                    undefined);
        _ ->
            ACL = riak_moss_acl_utils:acl_from_xml(Body)
    end,
    case riak_moss_utils:set_bucket_acl(User,
                                        VClock,
                                        Bucket,
                                        ACL) of
        ok ->
            {{halt, 200}, RD, Ctx};
        {error, Reason} ->
            riak_moss_s3_response:api_error(Reason, RD, Ctx)
    end;
accept_body(RD, Ctx=#context{user=User,
                             user_vclock=VClock,
                             bucket=Bucket}) ->
    %% Check for `x-amz-acl' header to support
    %% non-default ACL at bucket creation time.
    ACL = riak_moss_acl_utils:canned_acl(
            wrq:get_req_header("x-amz-acl", RD),
            {User?MOSS_USER.display_name,
             User?MOSS_USER.canonical_id},
            undefined),
    case riak_moss_utils:create_bucket(User,
                                       VClock,
                                       Bucket,
                                       ACL) of
        ok ->
            {{halt, 200}, RD, Ctx};
        ignore ->
            riak_moss_s3_response:api_error(bucket_already_exists, RD, Ctx);
        {error, Reason} ->
            riak_moss_s3_response:api_error(Reason, RD, Ctx)
    end.

%% @doc Callback for deleting a bucket.
-spec delete_resource(term(), term()) -> boolean().
delete_resource(RD, Ctx=#context{user=User,
                                 user_vclock=VClock,
                                 bucket=Bucket}) ->
    case riak_moss_utils:delete_bucket(User,
                                       VClock,
                                       Bucket) of
        ok ->
            {true, RD, Ctx};
        {error, Reason} ->
            riak_moss_s3_response:api_error(Reason, RD, Ctx)
    end.
