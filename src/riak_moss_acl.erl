%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module of functions that provide a way to determine if a
%% user may access a particular system resource.

-module(riak_moss_acl).

-include("riak_moss.hrl").
-include_lib("riakc/include/riakc_obj.hrl").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-endif.

%% Public API
-export([bucket_access/3
        ]).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Determine is a user has the requested access to a bucket.
-spec bucket_access(binary(), atom(), string()) -> boolean().
bucket_access(_Bucket, undefined, _CanonicalId) ->
    false;
bucket_access(Bucket, RequestedAccess, CanonicalId) ->
    %% Fetch the bucket's ACL
    case bucket_acl(Bucket) of
        {ok, Acl} ->
            has_permission(Acl, RequestedAccess, CanonicalId);
        {error, Reason} ->
            %% @TODO Think about bubbling this error up and providing
            %% feedback to requester.
            lager:error("Bucket access check failed due to error. Reason: ~p", [Reason]),
            false
    end.

%% ===================================================================
%% Interal functions
%% ===================================================================

%% @doc Find the ACL in a list of metadata values and
%% convert it to an erlang term representation. Return
%% `undefined' if an ACL is not found.
-spec acl_from_meta([{string(), term()}]) -> undefined | acl_v1().
acl_from_meta([]) ->
    {error, acl_undefined};
acl_from_meta([{?MD_ACL, Acl} | _]) ->
    {ok, binary_to_term(list_to_binary(Acl))};
acl_from_meta([_ | RestMD]) ->
    acl_from_meta(RestMD).

%% @doc Get the ACL for a bucket
-spec bucket_acl(binary()) -> acl_v1().
bucket_acl(Bucket) ->
    case riak_moss_utils:get_object(?BUCKETS_BUCKET, Bucket) of
        {ok, Obj} ->
            %% For buckets there will not be siblings.
            MD = riakc_obj:get_metadata(Obj),
            MetaVals = dict:fetch(?MD_USERMETA, MD),
            acl_from_meta(MetaVals);
        {error, _}=Error ->
            Error
    end.

%% @doc Determine if a user has the requested permission
%% granted in an ACL.
-spec has_permission(acl_v1(), atom(), string()) -> boolean().
has_permission(Acl, RequestedAccess, CanonicalId) ->
    Grants = Acl?ACL.grants,
    case user_grant(Grants, CanonicalId) of
        undefined ->
            false;
        {_, Perms} ->
            check_permission(RequestedAccess, Perms)
    end.

%% @doc Check if a list of ACL permissions contains a specific permission
%% or the `FULL_CONTROL' permission.
-spec check_permission(acl_perm(), acl_perms()) -> boolean().
check_permission(_, []) ->
    false;
check_permission(Permission, [Permission | _]) ->
    true;
check_permission(_, ['FULL_CONTROL' | _]) ->
    true;
check_permission(_Permission, [_ | RestPerms]) ->
    check_permission(_Permission, RestPerms).

%% @doc Iterate through a list of ACL grants and determine
%% if there is an entry for the specified user's id.
-spec user_grant([acl_grant()], string()) -> undefined | acl_grant().
user_grant([], _) ->
    undefined;
user_grant([HeadGrant={{_, CanonicalId}, _} | _], CanonicalId) ->
    HeadGrant;
user_grant([_ | RestGrants], _CanonicalId) ->
    user_grant(RestGrants, _CanonicalId).
