%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_wm_bucket_versioning).

-export([content_types_provided/2,
         to_xml/2,
         allowed_methods/0]).

-export([authorize/2]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").

%% @doc Get the list of methods this resource supports.
-spec allowed_methods() -> [atom()].
allowed_methods() ->
    %% TODO: PUT?
    ['GET'].

-spec content_types_provided(#wm_reqdata{}, #context{}) -> [{string(), atom()}].
content_types_provided(RD, Ctx) ->
    {[{"application/xml", to_xml}], RD, Ctx}.

-spec authorize(#wm_reqdata{}, #context{}) -> 
                       {boolean() | {halt, term()}, #wm_reqdata{}, #context{}}.
authorize(RD, #context{user=User,
                              riakc_pid=RiakPid}=Ctx) ->
    Method = wrq:method(RD),
    RequestedAccess =
        riak_cs_acl_utils:requested_access(Method, true),
    Bucket = list_to_binary(wrq:path_info(bucket, RD)),
    PermCtx = Ctx#context{bucket=Bucket,
                          requested_perm=RequestedAccess},
    case riak_cs_acl_utils:check_grants(User,Bucket,RequestedAccess,RiakPid) of 
        true ->
            %% performing on behalf of bucket owner
            AccessRD = riak_cs_access_logger:set_user(User, RD),
            {false, AccessRD, PermCtx};
        {true, OwnerId} ->
            %% performing on behalf of (possibly anon.) actor other than the owner of 
            %% the bucket. need to fetch owner record and log access against it
            riak_cs_wm_utils:shift_to_owner(RD, PermCtx, OwnerId, RiakPid);
        false when User =:= undefined ->
            AccessRD = RD,
            riak_cs_wm_utils:deny_access(AccessRD, PermCtx);
        false ->
            AccessRD = riak_cs_access_logger:set_user(User, RD),
            %% Check if the bucket actually exists so we can
            %% make the correct decision to return a 404 or 403
            case riak_cs_utils:check_bucket_exists(Bucket, RiakPid) of
                {ok, _} ->
                    riak_cs_wm_utils:deny_access(AccessRD, PermCtx);
                {error, Reason} ->
                    riak_cs_s3_response:api_error(Reason, RD, Ctx)
            end
    end.

-spec to_xml(#wm_reqdata{}, #context{}) -> 
                    {binary() | {halt, term()}, #wm_reqdata{}, #context{}}.
to_xml(RD, Ctx=#context{user=User,bucket=Bucket}) ->
    StrBucket = binary_to_list(Bucket),
    case [B || B <- riak_cs_utils:get_buckets(User),
               B?RCS_BUCKET.name =:= StrBucket] of
        [] ->
            riak_cs_s3_response:api_error(no_such_bucket, RD, Ctx);
        [_BucketRecord] ->
            {<<"<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>">>,
             RD, Ctx}
    end.


