%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_wm_bucket).

-export([content_types_provided/2,
         to_xml/2,
         allowed_methods/0,
         content_types_accepted/2,
         accept_body/2,
         delete_resource/2,
         authorize/2]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").

%% @doc Get the list of methods this resource supports.
-spec allowed_methods() -> [atom()].
allowed_methods() ->
    ['HEAD', 'PUT', 'DELETE'].

-spec content_types_provided(#wm_reqdata{}, #context{}) -> {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_provided(RD, Ctx) ->
    {[{"application/xml", to_xml}], RD, Ctx}.

-spec content_types_accepted(#wm_reqdata{}, #context{}) ->
                                    {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_accepted(RD, Ctx) ->
    case wrq:get_req_header("content-type", RD) of
        undefined ->
            {[{"application/octet-stream", accept_body}], RD, Ctx};
        CType ->
            {Media, _Params} = mochiweb_util:parse_header(CType),
            {[{Media, accept_body}], RD, Ctx}
    end.

-spec authorize(#wm_reqdata{}, #context{}) -> {boolean(), #wm_reqdata{}, #context{}}.
authorize(RD, #context{user=User,
                       riakc_pid=RiakPid}=Ctx) ->
    Method = wrq:method(RD),
    RequestedAccess =
        riak_cs_acl_utils:requested_access(Method, false),
    Bucket = list_to_binary(wrq:path_info(bucket, RD)),
    PermCtx = Ctx#context{bucket=Bucket,
                          requested_perm=RequestedAccess},

    case {Method, RequestedAccess} of
        {_, 'WRITE'} when User == undefined ->
            %% unauthed users may neither create nor delete buckets
            riak_cs_wm_utils:deny_access(RD, PermCtx);
        {'PUT', 'WRITE'} ->
            %% authed users are always allowed to attempt bucket creation
            AccessRD = riak_cs_access_logger:set_user(User, RD),
            {false, AccessRD, PermCtx};
        _ ->
            %% only owners are allowed to delete buckets
            case riak_cs_acl_utils:check_grants(User,Bucket,RequestedAccess,RiakPid) of
                true ->
                    %% because users are not allowed to create/destroy
                    %% buckets, we can assume that User is not
                    %% undefined here
                    AccessRD = riak_cs_access_logger:set_user(User, RD),
                    {false, AccessRD, PermCtx};
                {true, _OwnerId} when RequestedAccess == 'WRITE' ->
                    %% grants lied: this is a delete, and only the
                    %% owner is allowed to do that; setting user for
                    %% the request anyway, so the error tally is
                    %% logged for them
                    AccessRD = riak_cs_access_logger:set_user(User, RD),
                    riak_cs_wm_utils:deny_access(AccessRD, PermCtx);
                {true, OwnerId} ->
                    %% this operation is allowed, but we need to get
                    %% the owner's record, and log the access against
                    %% them instead of the actor
                    riak_cs_wm_utils:shift_to_owner(RD, PermCtx, OwnerId, RiakPid);
                false ->
                    case User of
                        undefined ->
                            %% no facility for logging bad access
                            %% against unknown actors
                            AccessRD = RD,
                            riak_cs_wm_utils:deny_access(AccessRD, PermCtx);
                        _ ->
                            %% log bad requests against the actors
                            %% that make them
                            AccessRD = riak_cs_access_logger:set_user(User, RD),
                            %% Check if the bucket actually exists so we can
                            %% make the correct decision to return a 404 or 403
                            case riak_cs_utils:check_bucket_exists(Bucket, RiakPid) of
                                {ok, _} ->
                                    riak_cs_wm_utils:deny_access(AccessRD, PermCtx);
                                {error, Reason} ->
                                    riak_cs_s3_response:api_error(Reason, RD, Ctx)
                            end
                    end
            end
    end.


-spec to_xml(#wm_reqdata{}, #context{}) ->
                    {binary() | {'halt', term()}, #wm_reqdata{}, #context{}}.
to_xml(RD, Ctx) ->
    handle_read_request(RD, Ctx).

%% @private
handle_read_request(RD, Ctx=#context{user=User,
                                     bucket=Bucket}) ->
    riak_cs_dtrace:dt_bucket_entry(?MODULE, <<"bucket_head">>,
                                      [], [riak_cs_wm_utils:extract_name(User), Bucket]),
    %% override the content-type on HEAD
    HeadRD = wrq:set_resp_header("content-type", "text/html", RD),
    StrBucket = binary_to_list(Bucket),
    case [B || B <- riak_cs_utils:get_buckets(User),
               B?RCS_BUCKET.name =:= StrBucket] of
        [] ->
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_head">>,
                                               [404], [riak_cs_wm_utils:extract_name(User), Bucket]),
            {{halt, 404}, HeadRD, Ctx};
        [_BucketRecord] ->
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_head">>,
                                               [200], [riak_cs_wm_utils:extract_name(User), Bucket]),
            {{halt, 200}, HeadRD, Ctx}

    end.

%% @doc Process request body on `PUT' request.
-spec accept_body(#wm_reqdata{}, #context{}) -> {{halt, integer()}, #wm_reqdata{}, #context{}}.
accept_body(RD, Ctx=#context{user=User,
                             user_object=UserObj,
                             bucket=Bucket,
                             riakc_pid=RiakPid}) ->
    riak_cs_dtrace:dt_bucket_entry(?MODULE, <<"bucket_create">>,
                                      [], [riak_cs_wm_utils:extract_name(User), Bucket]),
    %% Check for `x-amz-acl' header to support
    %% non-default ACL at bucket creation time.
    ACL = riak_cs_acl_utils:canned_acl(
            wrq:get_req_header("x-amz-acl", RD),
            {User?RCS_USER.display_name,
             User?RCS_USER.canonical_id,
             User?RCS_USER.key_id},
            undefined,
            RiakPid),
    case riak_cs_utils:create_bucket(User,
                                       UserObj,
                                       Bucket,
                                       ACL,
                                       RiakPid) of
        ok ->
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_create">>,
                                               [200], [riak_cs_wm_utils:extract_name(User), Bucket]),
            {{halt, 200}, RD, Ctx};
        {error, Reason} ->
            Code = riak_cs_s3_response:status_code(Reason),
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_create">>,
                                              [Code], [riak_cs_wm_utils:extract_name(User), Bucket]),
            riak_cs_s3_response:api_error(Reason, RD, Ctx)
    end.

%% @doc Callback for deleting a bucket.
-spec delete_resource(#wm_reqdata{}, #context{}) ->
                             {boolean() | {'halt', term()}, #wm_reqdata{}, #context{}}.
delete_resource(RD, Ctx=#context{user=User,
                                 user_object=UserObj,
                                 bucket=Bucket,
                                 riakc_pid=RiakPid}) ->
    riak_cs_dtrace:dt_bucket_entry(?MODULE, <<"bucket_delete">>,
                                      [], [riak_cs_wm_utils:extract_name(User), Bucket]),
    case riak_cs_utils:delete_bucket(User,
                                       UserObj,
                                       Bucket,
                                       RiakPid) of
        ok ->
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_delete">>,
                                               [200], [riak_cs_wm_utils:extract_name(User), Bucket]),
            {true, RD, Ctx};
        {error, Reason} ->
            Code = riak_cs_s3_response:status_code(Reason),
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_delete">>,
                                               [Code], [riak_cs_wm_utils:extract_name(User), Bucket]),
            riak_cs_s3_response:api_error(Reason, RD, Ctx)
    end.
