%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_wm_bucket).

-export([content_types_provided/0,
         to_xml/2,
         allowed_methods/0,
         content_types_accepted/2,
         accept_body/2,
         delete_resource/2,
         finish_request/2,
         authorize/2]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").

%% @doc Get the list of methods this resource supports.
-spec allowed_methods() -> [atom()].
allowed_methods() ->
    ['HEAD', 'PUT', 'DELETE'].

-spec content_types_provided() -> [{string(), atom()}].
content_types_provided() ->
    [{"application/xml", to_xml}].

%% @spec content_types_accepted(reqdata(), context()) ->
%%          {[{ContentType::string(), Acceptor::atom()}],
%%           reqdata(), context()}
content_types_accepted(RD, Ctx) ->
    dt_entry(<<"content_types_accepted">>),
    case wrq:get_req_header("content-type", RD) of
        undefined ->
            {[{"application/octet-stream", accept_body}], RD, Ctx};
        CType ->
            {Media, _Params} = mochiweb_util:parse_header(CType),
            {[{Media, accept_body}], RD, Ctx}
    end.

-spec authorize(term(), term()) -> {boolean(), term(), term()}.
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
            case check_grants(PermCtx) of
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
                    shift_to_owner(RD, PermCtx, OwnerId, RiakPid);
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

%% @doc Call the correct (anonymous or auth'd user) {@link
%% riak_cs_acl} function to check permissions for this request.
check_grants(#context{user=undefined,
                      bucket=Bucket,
                      requested_perm=RequestedAccess,
                      riakc_pid=RiakPid}) ->
    riak_cs_acl:anonymous_bucket_access(Bucket, RequestedAccess, RiakPid);
check_grants(#context{user=User,
                      bucket=Bucket,
                      requested_perm=RequestedAccess,
                      riakc_pid=RiakPid}) ->
    riak_cs_acl:bucket_access(Bucket,
                                RequestedAccess,
                                User?RCS_USER.canonical_id,
                                RiakPid).

%% @doc The {@link forbidden/2} decision passed, but the bucket
%% belongs to someone else.  Switch to it if the owner's record can be
%% retrieved.
shift_to_owner(RD, Ctx, OwnerId, RiakPid) when RiakPid /= undefined ->
    case riak_cs_utils:get_user(OwnerId, RiakPid) of
        {ok, {Owner, OwnerObject}} when Owner?RCS_USER.status =:= enabled ->
            AccessRD = riak_cs_access_logger:set_user(Owner, RD),
            {false, AccessRD, Ctx#context{user=Owner,
                                          user_object=OwnerObject}};
        {ok, _} ->
            riak_cs_wm_utils:deny_access(RD, Ctx);
        {error, _} ->
            riak_cs_s3_response:api_error(bucket_owner_unavailable, RD, Ctx)
    end.

-spec to_xml(term(), #context{}) ->
                    {binary() | {'halt', term()}, term(), #context{}}.
to_xml(RD, Ctx) ->
    handle_read_request(RD, Ctx).

%% @private
handle_read_request(RD, Ctx=#context{user=User,
                                             bucket=Bucket}) ->
    %% override the content-type on HEAD
    HeadRD = wrq:set_resp_header("content-type", "text/html", RD),
    StrBucket = binary_to_list(Bucket),
    case [B || B <- riak_cs_utils:get_buckets(User),
               B?RCS_BUCKET.name =:= StrBucket] of
        [] ->
            {{halt, 404}, HeadRD, Ctx};
        [_BucketRecord] ->
            {{halt, 200}, HeadRD, Ctx}
    end.

%% @doc Process request body on `PUT' request.
accept_body(RD, Ctx=#context{user=User,
                             user_object=UserObj,
                             bucket=Bucket,
                             riakc_pid=RiakPid}) ->
    dt_entry(<<"accept_body">>, [], [extract_name(User), Bucket]),
    dt_entry_bucket(<<"create">>, [], [extract_name(User), Bucket]),
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
            dt_return(<<"accept_body">>, [200], [extract_name(User), Bucket]),
            dt_return_bucket(<<"create">>, [200], [extract_name(User), Bucket]),
            {{halt, 200}, RD, Ctx};
        {error, Reason} ->
            Code = riak_cs_s3_response:status_code(Reason),
            dt_return(<<"accept_body">>, [Code], [extract_name(User), Bucket]),
            dt_return_bucket(<<"create">>, [Code], [extract_name(User), Bucket]),
            riak_cs_s3_response:api_error(Reason, RD, Ctx)
    end.

%% @doc Callback for deleting a bucket.
-spec delete_resource(term(), term()) -> {boolean() | {'halt', term()}, term, #context{}}.
delete_resource(RD, Ctx=#context{user=User,
                                 user_object=UserObj,
                                 bucket=Bucket,
                                 riakc_pid=RiakPid}) ->
    dt_entry(<<"delete_resource">>, [], [extract_name(User), Bucket]),
    dt_entry_bucket(<<"delete">>, [], [extract_name(User), Bucket]),
    case riak_cs_utils:delete_bucket(User,
                                       UserObj,
                                       Bucket,
                                       RiakPid) of
        ok ->
            dt_return(<<"delete_resource">>, [200], [extract_name(User), Bucket]),
            dt_return_bucket(<<"delete">>, [200], [extract_name(User), Bucket]),
            {true, RD, Ctx};
        {error, Reason} ->
            Code = riak_cs_s3_response:status_code(Reason),
            dt_return(<<"delete_resource">>, [Code], [extract_name(User), Bucket]),
            dt_return_bucket(<<"delete">>, [Code], [extract_name(User), Bucket]),
            riak_cs_s3_response:api_error(Reason, RD, Ctx)
    end.

finish_request(RD, Ctx=#context{riakc_pid=undefined}) ->
    dt_entry(<<"finish_request">>, [0], []),
    {true, RD, Ctx};
finish_request(RD, Ctx=#context{riakc_pid=RiakPid}) ->
    dt_entry(<<"finish_request">>, [1], []),
    riak_cs_utils:close_riak_connection(RiakPid),
    dt_return(<<"finish_request">>, [1], []),
    {true, RD, Ctx#context{riakc_pid=undefined}}.

extract_name(X) ->
    riak_cs_wm_utils:extract_name(X).

dt_entry(Func) ->
    dt_entry(Func, [], []).

dt_entry(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, Ints, ?MODULE, Func, Strings).

dt_entry_bucket(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_BUCKET_OP, 1, Ints, ?MODULE, Func, Strings).

dt_return(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 2, Ints, ?MODULE, Func, Strings).

dt_return_bucket(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_BUCKET_OP, 2, Ints, ?MODULE, Func, Strings).
