%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_wm_bucket_location).

% TODO: add PUT
-export([content_types_provided/0,
         to_xml/2,
         allowed_methods/0,
         finish_request/2 %% TODO remove me
        ]).

-export([authorize/2]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").

%% @doc Get the list of methods this resource supports.
-spec allowed_methods() -> [atom()].
allowed_methods() ->
    ['GET'].

-spec content_types_provided() -> [{string(), atom()}].
content_types_provided() ->
    [{"application/xml", to_xml}].

-spec authorize(term(),term()) -> {boolean() | {halt, term()}, term(), term()}.
authorize(RD, #context{user=User,
                       riakc_pid=RiakPid}=Ctx) ->
    RequestedAccess = riak_cs_acl_utils:requested_access('GET', true),
    Bucket = list_to_binary(wrq:path_info(bucket, RD)),
    PermCtx = Ctx#context{bucket=Bucket,
                          requested_perm=RequestedAccess},
    case riak_cs_acl_utils:check_grants(User,Bucket,RequestedAccess,RiakPid) of 
        true ->
            AccessRD = riak_cs_access_logger:set_user(User, RD),
            {false, AccessRD, PermCtx};
        {true, OwnerId} ->
            shift_to_owner(RD, PermCtx, OwnerId, RiakPid);
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
    {<<"<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>">>,
     RD, Ctx}.

finish_request(RD, Ctx=#context{riakc_pid=undefined}) ->
    dt_entry(<<"finish_request">>, [0], []),
    {true, RD, Ctx};
finish_request(RD, Ctx=#context{riakc_pid=RiakPid}) ->
    dt_entry(<<"finish_request">>, [1], []),
    riak_cs_utils:close_riak_connection(RiakPid),
    dt_return(<<"finish_request">>, [1], []),
    {true, RD, Ctx#context{riakc_pid=undefined}}.

dt_entry(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, Ints, ?MODULE, Func, Strings).

dt_return(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 2, Ints, ?MODULE, Func, Strings).

