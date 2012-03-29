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
-export([anonymous_bucket_access/3,
         anonymous_object_access/4,
         bucket_access/4,
         bucket_acl/2,
         object_access/5]).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Determine if anonymous access is set for the bucket.
-spec anonymous_bucket_access(binary(), atom(), pid()) -> {true, string()} | false.
anonymous_bucket_access(_Bucket, undefined, _) ->
    false;
anonymous_bucket_access(Bucket, RequestedAccess, RiakPid) ->
    %% Fetch the bucket's ACL
    case bucket_acl(Bucket, RiakPid) of
        {ok, Acl} ->
            case RequestedAccess of
                'WRITE' ->
                    %% Only owners may delete buckets
                    false;
                _ ->
                    case has_permission(acl_grants(Acl), RequestedAccess) of
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
-spec anonymous_object_access(binary(), acl(), atom(), pid()) ->
                                     {true, string()} |
                                     false.
anonymous_object_access(_Bucket, _ObjAcl, undefined, _) ->
    false;
anonymous_object_access(Bucket, _ObjAcl, 'WRITE', RiakPid) ->
    case bucket_acl(Bucket, RiakPid) of
        {ok, BucketAcl} ->
            %% `WRITE' is the only pertinent bucket-level
            %% permission when checking object access.
            has_permission(acl_grants(BucketAcl), 'WRITE');
        {error, Reason} ->
            %% @TODO Think about bubbling this error up and providing
            %% feedback to requester.
            lager:error("Anonymous object access check failed due to error. Reason: ~p", [Reason]),
            false
    end;
anonymous_object_access(_Bucket, ObjAcl, RequestedAccess, _) ->
    HasObjPerm = has_permission(acl_grants(ObjAcl), RequestedAccess),
    case HasObjPerm of
        true ->
            {true, owner_id(ObjAcl)};
        _ ->
            false
    end.

%% @doc Determine if a user has the requested access to a bucket.
-spec bucket_access(binary(), atom(), string(), pid()) -> boolean() |
                                                   {true, string()}.
bucket_access(_Bucket, undefined, _CanonicalId, _) ->
    false;
bucket_access(Bucket, RequestedAccess, CanonicalId, RiakPid) ->
    %% Fetch the bucket's ACL
    case bucket_acl(Bucket, RiakPid) of
        {ok, Acl} ->
            IsOwner = is_owner(Acl, CanonicalId),
            HasPerm = has_permission(acl_grants(Acl),
                                     RequestedAccess,
                                     CanonicalId),
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
-spec bucket_acl(binary(), pid()) -> acl().
bucket_acl(Bucket, RiakPid) ->
    case riak_moss_utils:get_object(?BUCKETS_BUCKET, Bucket, RiakPid) of
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
-spec object_access(binary(), acl(), atom(), string(), pid()) -> boolean() |
                                                             {true, string()}.
object_access(_Bucket, _ObjAcl, undefined, _CanonicalId, _) ->
    false;
object_access(_Bucket, _ObjAcl, _RequestedAccess, undefined, _RiakPid) ->
    %% User record not provided, check for anonymous access
    anonymous_object_access(_Bucket, _ObjAcl, _RequestedAccess, _RiakPid);
object_access(Bucket, _ObjAcl, 'WRITE', CanonicalId, RiakPid) ->
    %% Fetch the bucket's ACL
    case bucket_acl(Bucket, RiakPid) of
        {ok, BucketAcl} ->
            HasBucketPerm = has_permission(acl_grants(BucketAcl),
                                           'WRITE',
                                           CanonicalId),
            case HasBucketPerm of
                true ->
                    {true, owner_id(BucketAcl)};
                _ ->
                    false
            end;
        {error, Reason} ->
            %% @TODO Think about bubbling this error up and providing
            %% feedback to requester.
            lager:error("Object access check failed due to error. Reason: ~p", [Reason]),
            false
    end;
object_access(_Bucket, ObjAcl, RequestedAccess, CanonicalId, _RiakPid) ->
    lager:debug("ObjAcl: ~p~nCanonicalId: ~p", [ObjAcl, CanonicalId]),
    IsObjOwner = is_owner(ObjAcl, CanonicalId),
    HasObjPerm = has_permission(acl_grants(ObjAcl),
                                RequestedAccess,
                                CanonicalId),
    lager:debug("IsObjOwner: ~p", [IsObjOwner]),
    lager:debug("HasObjPerm: ~p", [HasObjPerm]),
    case HasObjPerm of
        true when IsObjOwner == true ->
            true;
        true ->
            {true, owner_id(ObjAcl)};
        _ ->
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

%% @doc Get the grants from an ACL
-spec acl_grants(acl()) -> [acl_grant()].
acl_grants(?ACL{grants=Grants}) ->
    Grants;
acl_grants(#acl_v1{grants=Grants}) ->
    Grants.

%% @doc Get the canonical id of the owner of an entity.
-spec owner_id(acl()) -> string().
owner_id(?ACL{owner=Owner}) ->
    {_, _, OwnerId} = Owner,
    OwnerId;
owner_id(#acl_v1{owner=OwnerData}) ->
    {Name, CanonicalId} = OwnerData,
    case riak_moss_utils:get_user_by_index(?ID_INDEX,
                                           list_to_binary(CanonicalId)) of
        {ok, {Owner, _}} ->
            Owner?MOSS_USER.key_id;
        {error, _} ->
            lager:warning("Failed to retrieve key_id for user ~p with canonical_id ~p", [Name, CanonicalId]),
            []
    end.

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
-spec has_permission([acl_grant()], atom()) -> boolean().
has_permission(Grants, RequestedAccess) ->
    GroupGrants = group_grants(Grants, []),
    case [Perms || {Grantee, Perms} <- GroupGrants,
                   Grantee =:= 'AllUsers'] of
        [] ->
            false;
        [Perms | _] ->
            check_permission(RequestedAccess, Perms)
    end.

%% @doc Determine if a user has the requested permission
%% granted in an ACL.
-spec has_permission([acl_grant()], atom(), string()) -> boolean().
has_permission(Grants, RequestedAccess, CanonicalId) ->
    GroupGrants = group_grants(Grants, []),
    case user_grant(Grants, CanonicalId) of
        undefined ->
            has_group_permission(GroupGrants, RequestedAccess);
        {_, Perms} ->
            check_permission(RequestedAccess, Perms) orelse
                has_group_permission(GroupGrants, RequestedAccess)
    end.

%% @doc Determine if a user is the owner of a system entity.
-spec is_owner(acl(), string()) -> boolean().
is_owner(?ACL{owner={_, CanonicalId, _}}, CanonicalId) ->
    true;
is_owner(?ACL{}, _) ->
    false;
is_owner(#acl_v1{owner={_, CanonicalId}}, CanonicalId) ->
    true;
is_owner(#acl_v1{}, _) ->
    false.

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
