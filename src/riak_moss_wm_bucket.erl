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
         delete_resource/2,
         finish_request/2]).

-include("riak_moss.hrl").
-include_lib("webmachine/include/webmachine.hrl").


init(Config) ->
    dt_entry(<<"init">>),
    %% Check if authentication is disabled and
    %% set that in the context.
    AuthBypass = proplists:get_value(auth_bypass, Config),
    {ok, #context{start_time=now(), auth_bypass=AuthBypass}}.

-spec service_available(term(), term()) -> {true, term(), term()}.
service_available(RD, Ctx) ->
    dt_entry(<<"service_available">>),
    Res = riak_moss_wm_utils:service_available(RD, Ctx),
    dt_return(<<"service_available">>),
    Res.

-spec malformed_request(term(), term()) -> {false, term(), term()}.
malformed_request(RD, Ctx) ->
    dt_entry(<<"malformed_request">>),
    {false, RD, Ctx}.

%% @doc Check to see if the user is
%%      authenticated. Normally with HTTP
%%      we'd use the `authorized` callback,
%%      but this is how S3 does things.
forbidden(RD, Ctx) ->
    dt_entry(<<"forbidden">>),
    case riak_moss_wm_utils:find_and_auth_user(RD, Ctx, fun check_permission/2) of
        {false, _RD2, Ctx2} = FalseRet ->
            dt_return(<<"forbidden">>, [], [extract_name(Ctx2#context.user), <<"false">>]),
            FalseRet;
        {Rsn, _RD2, Ctx2} = Ret ->
            Reason = case Rsn of
                         {halt, Code} -> Code;
                         _            -> -1
                     end,
            dt_return(<<"forbidden">>, [Reason], [extract_name(Ctx2#context.user), <<"true">>]),
            Ret
    end.

check_permission(RD, #context{user=User,
                              riakc_pid=RiakPid}=Ctx) ->
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
            AccessRD = riak_moss_access_logger:set_user(User, RD),
            {false, AccessRD, PermCtx};
        _ ->
            %% only owners are allowed to delete buckets
            case check_grants(PermCtx) of
                true ->
                    %% because users are not allowed to create/destroy
                    %% buckets, we can assume that User is not
                    %% undefined here
                    AccessRD = riak_moss_access_logger:set_user(User, RD),
                    {false, AccessRD, PermCtx};
                {true, _OwnerId} when RequestedAccess == 'WRITE' ->
                    %% grants lied: this is a delete, and only the
                    %% owner is allowed to do that; setting user for
                    %% the request anyway, so the error tally is
                    %% logged for them
                    AccessRD = riak_moss_access_logger:set_user(User, RD),
                    riak_moss_wm_utils:deny_access(AccessRD, PermCtx);
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
                            AccessRD = RD;
                        _ ->
                            %% log bad requests against the actors
                            %% that make them
                            AccessRD =
                                riak_moss_access_logger:set_user(User, RD)
                    end,
                    riak_moss_wm_utils:deny_access(AccessRD, PermCtx)
            end
    end.

%% @doc Call the correct (anonymous or auth'd user) {@link
%% riak_moss_acl} function to check permissions for this request.
check_grants(#context{user=undefined,
                      bucket=Bucket,
                      requested_perm=RequestedAccess,
                      riakc_pid=RiakPid}) ->
    riak_moss_acl:anonymous_bucket_access(Bucket, RequestedAccess, RiakPid);
check_grants(#context{user=User,
                      bucket=Bucket,
                      requested_perm=RequestedAccess,
                      riakc_pid=RiakPid}) ->
    riak_moss_acl:bucket_access(Bucket,
                                RequestedAccess,
                                User?MOSS_USER.canonical_id,
                                RiakPid).

%% @doc The {@link forbidden/2} decision passed, but the bucket
%% belongs to someone else.  Switch to it if the owner's record can be
%% retrieved.
shift_to_owner(RD, Ctx, OwnerId, RiakPid) when RiakPid /= undefined ->
    case riak_moss_utils:get_user(OwnerId, RiakPid) of
        {ok, {Owner, OwnerVClock}} ->
            AccessRD = riak_moss_access_logger:set_user(Owner, RD),
            {false, AccessRD, Ctx#context{user=Owner,
                                          user_vclock=OwnerVClock}};
        {error, _} ->
            riak_moss_s3_response:api_error(bucket_owner_unavailable, RD, Ctx)
    end.

%% @doc Get the list of methods this resource supports.
-spec allowed_methods(term(), term()) -> {[atom()], term(), term()}.
allowed_methods(RD, Ctx) ->
    dt_entry(<<"allowed_methods">>),
    %% TODO: add POST
    %% TODO: make this list conditional on Ctx
    {['HEAD', 'GET', 'PUT', 'DELETE'], RD, Ctx}.

-spec content_types_provided(term(), term()) ->
                                    {[{string(), atom()}], term(), term()}.
content_types_provided(RD, Ctx) ->
    dt_entry(<<"content_types_provided">>),
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
    dt_entry(<<"content_types_accepted">>),
    case wrq:get_req_header("content-type", RD) of
        undefined ->
            {[{"application/octet-stream", accept_body}], RD, Ctx};
        CType ->
            {Media, _Params} = mochiweb_util:parse_header(CType),
            {[{Media, accept_body}], RD, Ctx}
    end.

-spec to_xml(term(), #context{}) ->
                    {binary() | {'halt', term()}, term(), #context{}}.
to_xml(RD, Ctx=#context{start_time=StartTime,
                        user=User,
                        bucket=Bucket,
                        requested_perm='READ',
                        riakc_pid=RiakPid}) ->
    dt_entry(<<"to_xml">>, [], [extract_name(User), Bucket]),
    StrBucket = binary_to_list(Bucket),
    case [B || B <- riak_moss_utils:get_buckets(User),
               B?MOSS_BUCKET.name =:= StrBucket] of
        [] ->
            CodeName = no_such_bucket,
            Res = riak_moss_s3_response:api_error(CodeName, RD, Ctx),
            dt_return(<<"to_xml">>, [riak_moss_s3_response:status_code(CodeName)], [extract_name(User), Bucket]),
            Res;
        [BucketRecord] ->
            Prefix = list_to_binary(wrq:get_qs_value("prefix", "", RD)),
            case riak_moss_utils:get_keys_and_manifests(Bucket, Prefix, RiakPid) of
                {ok, KeyObjPairs} ->
                    X = riak_moss_s3_response:list_bucket_response(User,
                                                                   BucketRecord,
                                                                   KeyObjPairs,
                                                                   RD,
                                                                   Ctx),
                    ok = riak_cs_stats:update_with_start(bucket_list_keys,
                                                         StartTime),
                    dt_return(<<"to_xml">>, [200], [extract_name(User), Bucket]),
                    X;
                {error, Reason} ->
                    Code = riak_moss_s3_response:status_code(Reason),
                    X = riak_moss_s3_response:api_error(Reason, RD, Ctx),
                    dt_return(<<"to_xml">>, [Code], [extract_name(User), Bucket]),
                    X
            end
    end;
to_xml(RD, Ctx=#context{start_time=StartTime,
                        user=User,
                        bucket=Bucket,
                        requested_perm='READ_ACP',
                        riakc_pid=RiakPid}) ->
    dt_entry(<<"to_xml">>, [], [extract_name(User), Bucket]),
    case riak_moss_acl:bucket_acl(Bucket, RiakPid) of
        {ok, Acl} ->
            X = {riak_moss_acl_utils:acl_to_xml(Acl), RD, Ctx},
            ok = riak_cs_stats:update_with_start(bucket_get_acl, StartTime),
            dt_return(<<"to_xml">>, [200], [extract_name(User), Bucket]),
            X;
        {error, Reason} ->
            Code = riak_moss_s3_response:status_code(Reason),
            X = riak_moss_s3_response:api_error(Reason, RD, Ctx),
            dt_return(<<"to_xml">>, [Code], [extract_name(User), Bucket]),
            X
    end.

%% @doc Process request body on `PUT' request.
accept_body(RD, Ctx=#context{user=User,
                             user_vclock=VClock,
                             bucket=Bucket,
                             requested_perm='WRITE_ACP',
                             riakc_pid=RiakPid}) ->
    dt_entry(<<"accept_body">>, [], [extract_name(User), Bucket]),
    Body = binary_to_list(wrq:req_body(RD)),
    case Body of
        [] ->
            %% Check for `x-amz-acl' header to support
            %% the use of a canned ACL.
            ACL = riak_moss_acl_utils:canned_acl(
                    wrq:get_req_header("x-amz-acl", RD),
                    {User?MOSS_USER.display_name,
                     User?MOSS_USER.canonical_id,
                     User?MOSS_USER.key_id},
                    undefined,
                    RiakPid);
        _ ->
            ACL = riak_moss_acl_utils:acl_from_xml(Body,
                                                   User?MOSS_USER.key_id,
                                                   RiakPid)
    end,
    case riak_moss_utils:set_bucket_acl(User,
                                        VClock,
                                        Bucket,
                                        ACL,
                                        RiakPid) of
        ok ->
            dt_return(<<"accept_body">>, [200, size(Body)], [extract_name(User)]),
            {{halt, 200}, RD, Ctx};
        {error, Reason} ->
            Code = riak_moss_s3_response:status_code(Reason),
            dt_return(<<"accept_body">>, [Code], [extract_name(User)]),
            riak_moss_s3_response:api_error(Reason, RD, Ctx)
    end;
accept_body(RD, Ctx=#context{user=User,
                             user_vclock=VClock,
                             bucket=Bucket,
                             riakc_pid=RiakPid}) ->
    dt_entry(<<"accept_body">>, [], [extract_name(User), Bucket]),
    %% Check for `x-amz-acl' header to support
    %% non-default ACL at bucket creation time.
    ACL = riak_moss_acl_utils:canned_acl(
            wrq:get_req_header("x-amz-acl", RD),
            {User?MOSS_USER.display_name,
             User?MOSS_USER.canonical_id,
             User?MOSS_USER.key_id},
            undefined,
            RiakPid),
    case riak_moss_utils:create_bucket(User,
                                       VClock,
                                       Bucket,
                                       ACL,
                                       RiakPid) of
        ok ->
            dt_return(<<"accept_body">>, [200], [extract_name(User), Bucket]),
            {{halt, 200}, RD, Ctx};
        {error, Reason} ->
            Code = riak_moss_s3_response:status_code(Reason),
            dt_return(<<"accept_body">>, [Code], [extract_name(User)]),
            riak_moss_s3_response:api_error(Reason, RD, Ctx)
    end.

%% @doc Callback for deleting a bucket.
-spec delete_resource(term(), term()) -> {boolean() | {'halt', term()}, term, #context{}}.
delete_resource(RD, Ctx=#context{user=User,
                                 user_vclock=VClock,
                                 bucket=Bucket,
                                 riakc_pid=RiakPid}) ->
    dt_entry(<<"delete_resource">>, [], [extract_name(User), Bucket]),
    case riak_moss_utils:delete_bucket(User,
                                       VClock,
                                       Bucket,
                                       RiakPid) of
        ok ->
            dt_return(<<"delete_resource">>, [200], [extract_name(User), Bucket]),
            {true, RD, Ctx};
        {error, Reason} ->
            Code = riak_moss_s3_response:status_code(Reason),
            dt_return(<<"delete_resource">>, [Code], [extract_name(User), Bucket]),
            riak_moss_s3_response:api_error(Reason, RD, Ctx)
    end.

finish_request(RD, Ctx=#context{riakc_pid=undefined}) ->
    dt_entry(<<"finish_request">>, [0], []),
    {true, RD, Ctx};
finish_request(RD, Ctx=#context{riakc_pid=RiakPid}) ->
    dt_entry(<<"finish_request">>, [1], []),
    riak_moss_utils:close_riak_connection(RiakPid),
    dt_return(<<"finish_request">>, [1], []),
    {true, RD, Ctx#context{riakc_pid=undefined}}.

extract_name(User) when is_list(User) ->
    User;
extract_name(?MOSS_USER{name=Name}) ->
    Name;
extract_name(_) ->
    "-unknown-".

dt_entry(Func) ->
    dt_entry(Func, [], []).

dt_entry(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, Ints, ?MODULE, Func, Strings).

dt_return(Func) ->
    dt_return(Func, [], []).

dt_return(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 2, Ints, ?MODULE, Func, Strings).
