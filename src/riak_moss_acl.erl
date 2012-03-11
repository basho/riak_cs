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
-export([anonymous_bucket_access/2,
         anonymous_object_access/3,
         bucket_access/3,
         bucket_acl/1,
         object_access/4]).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Determine if anonymous access is set for the bucket.
-spec anonymous_bucket_access(binary(), atom()) -> {true, string()} | false.
anonymous_bucket_access(_Bucket, undefined) ->
    false;
anonymous_bucket_access(Bucket, RequestedAccess) ->
    %% Fetch the bucket's ACL
    case bucket_acl(Bucket) of
        {ok, Acl} ->
            case RequestedAccess of
                'WRITE' ->
                    %% Only owners may delete buckets
                    false;
                _ ->
                    case has_permission(Acl, RequestedAccess) of
                        true ->
                            {true, owner_id(Acl)};
                        false ->
                            false
                    end
            end;
        {error, Reason} ->
            %% @TODO Think about bubbling this error up and providing
            %% feedback to requester.
            lager:error("Anonymous bucket access check failed due to error. Reason: ~p", [Reason]),
            false
    end.

%% @doc Determine if anonymous access is set for the object.
%% @TODO Enhance when doing object ACLs
-spec anonymous_object_access(binary(), acl(), atom()) -> {true, string()} |
                                                             false.
anonymous_object_access(_Bucket, _ObjAcl, undefined) ->
    false;
anonymous_object_access(Bucket, ObjAcl, RequestedAccess) ->
    case bucket_acl(Bucket) of
        {ok, BucketAcl} ->
            case RequestedAccess of
                'WRITE' ->
                    %% `WRITE' is the only pertinent bucket-level
                    %% permission when checking object access.
                    has_permission(BucketAcl, RequestedAccess);
                _ ->
                    false
            end;
        {error, Reason} ->
            %% @TODO Think about bubbling this error up and providing
            %% feedback to requester.
            lager:error("Anonymous object access check failed due to error. Reason: ~p", [Reason]),
            false
    end.

%% @doc Determine if a user has the requested access to a bucket.
-spec bucket_access(binary(), atom(), string()) -> boolean() |
                                                   {true, string()}.
bucket_access(_Bucket, undefined, _CanonicalId) ->
    false;
bucket_access(Bucket, RequestedAccess, CanonicalId) ->
    %% Fetch the bucket's ACL
    case bucket_acl(Bucket) of
        {ok, Acl} ->
            IsOwner = is_owner(Acl, CanonicalId),
            HasPerm = has_permission(Acl, RequestedAccess, CanonicalId),
            case HasPerm of
                true when IsOwner == true ->
                    true;
                true ->
                    {true, owner_id(Acl)};
                _ ->
                    false
            end;
        {error, notfound} ->
            %% This indicates the bucket does not exist so
            %% allow the request to proceed.
            true;
        {error, Reason} ->
            %% @TODO Think about bubbling this error up and providing
            %% feedback to requester.
            lager:error("Bucket access check failed due to error. Reason: ~p", [Reason]),
            false
    end.

%% @doc Get the ACL for a bucket
-spec bucket_acl(binary()) -> acl().
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

%% @doc Determine if a user has the requested access to an object
%% @TODO Enhance when doing object-level ACL work. This is a bit
%% patchy until object ACLs are done. The bucket owner gets full
%% control, but bucket-level ACLs only matter for writes otherwise.
-spec object_access(binary(), acl(), atom(), string()) -> boolean() |
                                                             {true, string()}.
object_access(_Bucket, _ObjAcl, undefined, _CanonicalId) ->
    false;
object_access(_Bucket, _ObjAcl, _RequestedAccess, undefined) ->
    %% User record not provided, check for anonymous access
    anonymous_object_access(_Bucket, _ObjAcl, _RequestedAccess);
object_access(Bucket, ObjAcl, RequestedAccess, CanonicalId) ->
    %% Fetch the bucket's ACL
    case bucket_acl(Bucket) of
        {ok, BucketAcl} ->
            IsOwner = is_owner(BucketAcl, CanonicalId),
            HasPerm = has_permission(BucketAcl, RequestedAccess, CanonicalId),
            case HasPerm of
                true when IsOwner == true ->
                    true;
                true when RequestedAccess == 'WRITE' ->
                    {true, owner_id(BucketAcl)};
                _ ->
                    false
            end;
        {error, Reason} ->
            %% @TODO Think about bubbling this error up and providing
            %% feedback to requester.
            lager:error("Object access check failed due to error. Reason: ~p", [Reason]),
            false
    end.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @doc Find the ACL in a list of metadata values and
%% convert it to an erlang term representation. Return
%% `undefined' if an ACL is not found.
-spec acl_from_meta([{string(), term()}]) -> undefined | acl().
acl_from_meta([]) ->
    {error, acl_undefined};
acl_from_meta([{?MD_ACL, Acl} | _]) ->
    {ok, binary_to_term(list_to_binary(Acl))};
acl_from_meta([_ | RestMD]) ->
    acl_from_meta(RestMD).

%% @doc Get the canonical id of the owner of an entity.
-spec owner_id(acl()) -> string().
owner_id(Acl) ->
    {_, OwnerId} = Acl?ACL.owner,
    OwnerId.

%% @doc Iterate through a list of ACL grants and return
%% any group grants.
-spec group_grants([acl_grant()], [acl_grant()]) -> [acl_grant()].
group_grants([], GroupGrants) ->
    GroupGrants;
group_grants([HeadGrant={Grantee, _} | RestGrants],
             GroupGrants) when is_atom(Grantee) ->
    group_grants(RestGrants, [HeadGrant | GroupGrants]);
group_grants([_ | RestGrants], _GroupGrants) ->
    group_grants(RestGrants, _GroupGrants).

%% @doc Determine if the ACL grants group access
%% for the requestsed permission type.
-spec has_group_permission(acl(), atom()) -> boolean().
has_group_permission([], _RequestedAccess) ->
    false;
has_group_permission([{_, Perms} | RestGrants], RequestedAccess) ->
    case check_permission(RequestedAccess, Perms) of
        true ->
            true;
        false ->
            has_group_permission(RestGrants, RequestedAccess)
    end.

%% @doc Determine if the ACL grants anonymous access
%% for the requestsed permission type.
-spec has_permission(acl(), atom()) -> boolean().
has_permission(Acl, RequestedAccess) ->
    GroupGrants = group_grants(Acl?ACL.grants, []),
    case [Perms || {Grantee, Perms} <- GroupGrants,
                   Grantee =:= 'AllUsers'] of
        [] ->
            false;
        [Perms | _] ->
            check_permission(RequestedAccess, Perms)
    end.

%% @doc Determine if a user has the requested permission
%% granted in an ACL.
-spec has_permission(acl(), atom(), string()) -> boolean().
has_permission(Acl, RequestedAccess, CanonicalId) ->
    Grants = Acl?ACL.grants,
    case user_grant(Grants, CanonicalId) of
        undefined ->
            GroupGrants = group_grants(Acl?ACL.grants, []),
            has_group_permission(GroupGrants, RequestedAccess);
        {_, Perms} ->
            check_permission(RequestedAccess, Perms)
    end.

%% @doc Determine if a user is the owner of a system entity.
-spec is_owner(acl(), string()) -> boolean().
is_owner(Acl, CanonicalId) ->
    {_, OwnerId} = Acl?ACL.owner,
    CanonicalId == OwnerId.

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
%% if there is an entry for the specified user's id. Ignore
%% any group grants.
-spec user_grant([acl_grant()], string()) -> undefined | acl_grant().
user_grant([], _) ->
    undefined;
user_grant([{Grantee, _} | RestGrants], _CanonicalId) when is_atom(Grantee) ->
    user_grant(RestGrants, _CanonicalId);
user_grant([HeadGrant={{_, CanonicalId}, _} | _], CanonicalId) ->
    HeadGrant;
user_grant([_ | RestGrants], _CanonicalId) ->
    user_grant(RestGrants, _CanonicalId).
