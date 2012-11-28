%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_wm_bucket_acl).

-export([content_types_provided/2,
         to_xml/2,
         allowed_methods/0,
         content_types_accepted/2,
         accept_body/2]).

-export([authorize/2]).

%% TODO: DELETE?

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").


%% @doc Get the list of methods this resource supports.
-spec allowed_methods() -> [atom()].
allowed_methods() ->
    ['GET', 'PUT'].

-spec content_types_provided(#wm_reqdata{}, #context{}) -> [{string(), atom()}].
content_types_provided(RD, Ctx) ->
    {[{"application/xml", to_xml}], RD, Ctx}.

-spec content_types_accepted(#wm_reqdata{}, #context{}) -> 
                                    {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_accepted(RD, Ctx) ->
    dt_entry(<<"content_types_accepted">>),
    case wrq:get_req_header("content-type", RD) of
        undefined ->
            {[{"application/octet-stream", accept_body}], RD, Ctx};
        CType ->
            {Media, _Params} = mochiweb_util:parse_header(CType),
            {[{Media, accept_body}], RD, Ctx}
    end.

-spec authorize(term(),term()) -> {boolean() | {halt, term()}, term(), term()}.
authorize(RD, #context{user=User,
                       riakc_pid=RiakPid}=Ctx) ->
    Method = wrq:method(RD),
    RequestedAccess =
        riak_cs_acl_utils:requested_access(Method, true),
    Bucket = list_to_binary(wrq:path_info(bucket, RD)),
    PermCtx = Ctx#context{bucket=Bucket,
                          requested_perm=RequestedAccess},
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
    end.

-spec to_xml(term(), #context{}) ->
                    {binary() | {'halt', term()}, term(), #context{}}.
to_xml(RD, Ctx=#context{start_time=StartTime,
                        user=User,
                        bucket=Bucket,
                        riakc_pid=RiakPid}) ->
    dt_entry(<<"to_xml">>, [], [extract_name(User), Bucket]),
    dt_entry_bucket(<<"get_acl">>, [], [extract_name(User), Bucket]),
    case riak_cs_acl:bucket_acl(Bucket, RiakPid) of
        {ok, Acl} ->
            X = {riak_cs_acl_utils:acl_to_xml(Acl), RD, Ctx},
            ok = riak_cs_stats:update_with_start(bucket_get_acl, StartTime),
            dt_return(<<"to_xml">>, [200], [extract_name(User), Bucket]),
            dt_return_bucket(<<"get_acl">>, [200], [extract_name(User), Bucket]),
            X;
        {error, Reason} ->
            Code = riak_cs_s3_response:status_code(Reason),
            X = riak_cs_s3_response:api_error(Reason, RD, Ctx),
            dt_return(<<"to_xml">>, [Code], [extract_name(User), Bucket]),
            dt_return_bucket(<<"get_acl">>, [Code], [extract_name(User), Bucket]),
            X
    end.

%% @doc Process request body on `PUT' request.
accept_body(RD, Ctx=#context{user=User,
                             user_object=UserObj,
                             bucket=Bucket,
                             riakc_pid=RiakPid}) ->
    dt_entry(<<"accept_body">>, [], [extract_name(User), Bucket]),
    dt_entry_bucket(<<"put_acl">>, [], [extract_name(User), Bucket]),
    Body = binary_to_list(wrq:req_body(RD)),
    case Body of
        [] ->
            %% Check for `x-amz-acl' header to support
            %% the use of a canned ACL.
            ACL = riak_cs_acl_utils:canned_acl(
                    wrq:get_req_header("x-amz-acl", RD),
                    {User?RCS_USER.display_name,
                     User?RCS_USER.canonical_id,
                     User?RCS_USER.key_id},
                    undefined,
                    RiakPid);
        _ ->
            ACL = riak_cs_acl_utils:acl_from_xml(Body,
                                                   User?RCS_USER.key_id,
                                                   RiakPid)
    end,
    case riak_cs_utils:set_bucket_acl(User,
                                        UserObj,
                                        Bucket,
                                        ACL,
                                        RiakPid) of
        ok ->
            dt_return(<<"accept_body">>, [200], [extract_name(User), Bucket]),
            dt_return_bucket(<<"put_acl">>, [200], [extract_name(User), Bucket]),
            {{halt, 200}, RD, Ctx};
        {error, Reason} ->
            Code = riak_cs_s3_response:status_code(Reason),
            dt_return(<<"accept_body">>, [Code], [extract_name(User), Bucket]),
            dt_return_bucket(<<"put_acl">>, [Code], [extract_name(User), Bucket]),
            riak_cs_s3_response:api_error(Reason, RD, Ctx)
    end.

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
