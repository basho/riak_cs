%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% ---------------------------------------------------------------------

%% @doc Module of functions that provide a way to determine if a
%% user may access a particular system resource.

-module(riak_cs_acl).

-include("riak_cs.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").

-ifdef(TEST).

-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-endif.

%% Public API
-export([anonymous_bucket_access/3,
         anonymous_bucket_access/4,
         anonymous_object_access/4,
         anonymous_object_access/5,
         bucket_access/4,
         bucket_access/5,
         bucket_acl/2,
         bucket_acl_from_contents/2,
         object_access/5,
         object_access/6,
         owner_id/2
        ]).

-define(ACL_UNDEF, {error, acl_undefined}).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Determine if anonymous access is set for the bucket.
-spec anonymous_bucket_access(binary(), atom(), pid()) -> {true, string()} | false.
anonymous_bucket_access(Bucket, RequestedAccess, RiakPid) ->
    anonymous_bucket_access(Bucket, RequestedAccess, RiakPid, undefined).

-spec anonymous_bucket_access(binary(), atom(), pid(), acl()|undefined)
                             -> {true, string()} | false.
anonymous_bucket_access(_Bucket, undefined, _, _) ->
    false;
anonymous_bucket_access(Bucket, RequestedAccess, RiakPid, undefined) ->
    %% Fetch the bucket's ACL
    case bucket_acl(Bucket, RiakPid) of
        {ok, BucketAcl} -> anonymous_bucket_access(Bucket, RequestedAccess, RiakPid, BucketAcl);
        {error, Reason} ->
            %% @TODO Think about bubbling this error up and providing
            %% feedback to requester.
            _ = lager:error("Anonymous bucket access check failed due to error. Reason: ~p", [Reason]),
            false
    end;
anonymous_bucket_access(_Bucket, RequestedAccess, RiakPid, BucketAcl) ->
    case RequestedAccess of
        'WRITE' ->
            %% Only owners may delete buckets
            false;
        _ ->
            case has_permission(acl_grants(BucketAcl), RequestedAccess) of
                true ->
                    {true, owner_id(BucketAcl, RiakPid)};
                false ->
                    false
            end
    end.

%% @doc Determine if anonymous access is set for the object.
%% @TODO Enhance when doing object ACLs
-spec anonymous_object_access(binary(), acl(), atom(), pid()) ->
                                     {true, string()} |
                                     false.
anonymous_object_access(Bucket, ObjAcl, RequestedAccess, RiakPid) ->
    anonymous_object_access(Bucket, ObjAcl, RequestedAccess, RiakPid, undefined).


-spec anonymous_object_access(binary(), acl(), atom(), pid(), acl()|undefined) ->
                                     {true, string()} | false.
anonymous_object_access(_Bucket, _ObjAcl, undefined, _, _) ->
    false;
anonymous_object_access(Bucket, ObjAcl, 'WRITE', RiakPid, undefined) ->
    case bucket_acl(Bucket, RiakPid) of
        {ok, BucketAcl} ->
            %% `WRITE' is the only pertinent bucket-level
            %% permission when checking object access.
            anonymous_object_access(Bucket, ObjAcl, 'WRITE', RiakPid, BucketAcl);
        {error, Reason} ->
            %% @TODO Think about bubbling this error up and providing
            %% feedback to requester.
            _ = lager:error("Anonymous object access check failed due to error. Reason: ~p", [Reason]),
            false
    end;
anonymous_object_access(_Bucket, _ObjAcl, 'WRITE', RiakPid, BucketAcl) ->
    HasObjPerm = has_permission(acl_grants(BucketAcl), 'WRITE'),
    case HasObjPerm of
        true ->
            {true, owner_id(BucketAcl, RiakPid)};
        _ ->
            false
    end;
anonymous_object_access(_Bucket, ObjAcl, RequestedAccess, RiakPid, _) ->
    HasObjPerm = has_permission(acl_grants(ObjAcl), RequestedAccess),
    case HasObjPerm of
        true ->
            {true, owner_id(ObjAcl, RiakPid)};
        _ ->
            false
    end.

%% @doc Determine if a user has the requested access to a bucket.
-spec bucket_access(binary(), atom(), string(), pid()) ->
                           boolean() | {true, string()}.
bucket_access(Bucket, RequestedAccess, CanonicalId, RiakPid) ->
    bucket_access(Bucket, RequestedAccess, CanonicalId, RiakPid, undefined).

-spec bucket_access(binary(), atom(), string(), pid(), acl()|undefined ) ->
                           boolean() | {true, string()}.
bucket_access(_Bucket, undefined, _CanonicalId, _, _) ->
    false;
bucket_access(Bucket, RequestedAccess, CanonicalId, RiakPid, undefined) ->
    %% Fetch the bucket's ACL
    case bucket_acl(Bucket, RiakPid) of
        {ok, Acl} ->
            bucket_access(Bucket, RequestedAccess, CanonicalId, RiakPid, Acl);
        {error, notfound} ->
            %% This indicates the bucket does not exist so
            %% allow the request to proceed.
            true;
        {error, Reason} ->
            %% @TODO Think about bubbling this error up and providing
            %% feedback to requester.
            _ = lager:error("Bucket access check failed due to error. Reason: ~p", [Reason]),
            false
    end;
bucket_access(_, RequestedAccess, CanonicalId, RiakPid, Acl) ->
    IsOwner = is_owner(Acl, CanonicalId),
    HasPerm = has_permission(acl_grants(Acl),
                             RequestedAccess,
                             CanonicalId),
    case HasPerm of
        true when IsOwner == true ->
            true;
        true ->
            {true, owner_id(Acl, RiakPid)};
        false when IsOwner == true
                   andalso RequestedAccess /= 'READ' ->
            true;
        _ ->
            false
    end.


%% @doc Get the ACL for a bucket
-type acl_from_meta_result() :: {'ok', acl()} | {'error', 'acl_undefined'}.
-type bucket_acl_result() :: acl_from_meta_result() | {'error', 'multiple_bucket_owners'}.
-type bucket_acl_riak_error() :: {error, 'notfound' | term()}.
-spec bucket_acl(binary(), pid()) -> bucket_acl_result() | bucket_acl_riak_error().
bucket_acl(Bucket, RiakPid) ->
    case riak_cs_utils:check_bucket_exists(Bucket, RiakPid) of
        {ok, Obj} ->
            %% For buckets there should not be siblings, but in rare
            %% cases it may happen so check for them and attempt to
            %% resolve if possible.
            Contents = riakc_obj:get_contents(Obj),
            bucket_acl_from_contents(Bucket, Contents);
        {error, Reason} ->
            _ = lager:debug("Failed to fetch ACL. Bucket ~p "
                            " does not exist. Reason: ~p",
                            [Bucket, Reason]),
            {error, notfound}
    end.

%% @doc Attempt to resolve an ACL for the bucket based on the contents.
%% We attempt resolution, but intentionally do not write back a resolved
%% value. Instead the fact that the bucket has siblings is logged, but the
%% condition should be rare so we avoid updating the value at this time.
-spec bucket_acl_from_contents(binary(), riakc_obj:contents()) ->
                                      bucket_acl_result().
bucket_acl_from_contents(_, [{MD, _}]) ->
    MetaVals = dict:fetch(?MD_USERMETA, MD),
    acl_from_meta(MetaVals);
bucket_acl_from_contents(Bucket, Contents) ->
    {Metas, Vals} = lists:unzip(Contents),
    UniqueVals = lists:usort(Vals),
    UserMetas = [dict:fetch(?MD_USERMETA, MD) || MD <- Metas],
    riak_cs_utils:maybe_log_bucket_owner_error(Bucket, UniqueVals),
    resolve_bucket_metadata(UserMetas, UniqueVals).

-spec resolve_bucket_metadata(list(riakc_obj:metadata()),
                               list(riakc_obj:value())) -> bucket_acl_result().
resolve_bucket_metadata(Metas, [_Val]) ->
    Acls = [acl_from_meta(M) || M <- Metas],
    resolve_bucket_acls(Acls);
resolve_bucket_metadata(_Metas, _) ->
    {error, multiple_bucket_owners}.

-spec resolve_bucket_acls(list(acl_from_meta_result())) -> acl_from_meta_result().
resolve_bucket_acls([Acl]) ->
    Acl;
resolve_bucket_acls(Acls) ->
    lists:foldl(fun newer_acl/2, ?ACL_UNDEF, Acls).

-spec newer_acl(acl_from_meta_result(), acl_from_meta_result()) ->
                       acl_from_meta_result().
newer_acl(Acl1, ?ACL_UNDEF) ->
    Acl1;
newer_acl({ok, Acl1}, {ok, Acl2})
  when Acl1?ACL.creation_time >= Acl2?ACL.creation_time ->
    {ok, Acl1};
newer_acl(_, Acl2) ->
    Acl2.

%% @doc Determine if a user has the requested access to an object
%% @TODO Enhance when doing object-level ACL work. This is a bit
%% patchy until object ACLs are done. The bucket owner gets full
%% control, but bucket-level ACLs only matter for writes otherwise.
-spec object_access(binary(), acl(), atom(), string(), pid())
                   -> boolean() | {true, string()}.
object_access(Bucket, ObjAcl, RequestedAccess, CanonicalId, RiakPid) ->
    object_access(Bucket, ObjAcl, RequestedAccess, CanonicalId, RiakPid, undefined).


-spec object_access(binary(), acl(), atom(), string(), pid(), undefined|acl())
                   -> boolean() | {true, string()}.
object_access(_Bucket, _ObjAcl, undefined, _CanonicalId, _, _) ->
    false;
object_access(_Bucket, _ObjAcl, _RequestedAccess, undefined, _RiakPid, _) ->
    %% User record not provided, check for anonymous access
    anonymous_object_access(_Bucket, _ObjAcl, _RequestedAccess, _RiakPid);
object_access(Bucket, ObjAcl, 'WRITE', CanonicalId, RiakPid, undefined) ->
    case bucket_acl(Bucket, RiakPid) of
        {ok, BucketAcl} ->
            object_access(Bucket, ObjAcl, 'WRITE', CanonicalId, RiakPid, BucketAcl);
        {error, Reason} ->
            %% @TODO Think about bubbling this error up and providing
            %% feedback to requester.
            _ = lager:error("Object access check failed due to error. Reason: ~p", [Reason]),
            false
    end;
object_access(_Bucket, _ObjAcl, 'WRITE', CanonicalId, RiakPid, BucketAcl) ->
    %% Fetch the bucket's ACL
    IsBucketOwner = is_owner(BucketAcl, CanonicalId),
    HasBucketPerm = has_permission(acl_grants(BucketAcl),
                                   'WRITE',
                                   CanonicalId),
    case HasBucketPerm of
        true when IsBucketOwner == true ->
            true;
        true ->
            {true, owner_id(BucketAcl, RiakPid)};
        _ ->
            false
    end;
object_access(_Bucket, ObjAcl, RequestedAccess, CanonicalId, RiakPid, _) ->
    _ = lager:debug("ObjAcl: ~p~nCanonicalId: ~p", [ObjAcl, CanonicalId]),
    IsObjOwner = is_owner(ObjAcl, CanonicalId),
    HasObjPerm = has_permission(acl_grants(ObjAcl),
                                RequestedAccess,
                                CanonicalId),
    _ = lager:debug("IsObjOwner: ~p", [IsObjOwner]),
    _ = lager:debug("HasObjPerm: ~p", [HasObjPerm]),
    if
        (RequestedAccess == 'READ_ACP' orelse
         RequestedAccess == 'WRITE_ACP') andalso
        IsObjOwner == true ->
            %% The owner of an object may always read and modify the
            %% ACL of an object
            true;
        IsObjOwner == true andalso
        HasObjPerm == true ->
            true;
        HasObjPerm == true ->
            {true, owner_id(ObjAcl, RiakPid)};
        true ->
            false
    end.

%% @doc Get the canonical id of the owner of an entity.
-spec owner_id(acl(), pid()) -> string().
owner_id(?ACL{owner=Owner}, _) ->
    {_, _, OwnerId} = Owner,
    OwnerId;
owner_id(#acl_v1{owner=OwnerData}, RiakPid) ->
    {Name, CanonicalId} = OwnerData,
    case riak_cs_utils:get_user_by_index(?ID_INDEX,
                                           list_to_binary(CanonicalId),
                                           RiakPid) of
        {ok, {Owner, _}} ->
            Owner?RCS_USER.key_id;
        {error, _} ->
            _ = lager:warning("Failed to retrieve key_id for user ~p with canonical_id ~p", [Name, CanonicalId]),
            []
    end.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @doc Find the ACL in a list of metadata values and
%% convert it to an erlang term representation. Return
%% `undefined' if an ACL is not found.
-spec acl_from_meta([{string(), term()}]) -> acl_from_meta_result().
acl_from_meta([]) ->
    ?ACL_UNDEF;
acl_from_meta([{?MD_ACL, Acl} | _]) ->
    {ok, binary_to_term(Acl)};
acl_from_meta([_ | RestMD]) ->
    acl_from_meta(RestMD).

%% @doc Get the grants from an ACL
-spec acl_grants(acl()) -> [acl_grant()].
acl_grants(?ACL{grants=Grants}) ->
    Grants;
acl_grants(#acl_v1{grants=Grants}) ->
    Grants.

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
-spec has_group_permission([{group_grant() | {term(), term()}, term()}], atom()) -> boolean().
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
