%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc

-module(riak_cs_mp_utils).

-include("riak_cs.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% export Public API
-export([
         calc_multipart_2i_dict/3,
         cancel_multipart_upload/2,
         initiate_multipart_upload/4,
         list_multipart_uploads/2,
         new_manifest/4,
         write_new_manifest/1
        ]).

%%%===================================================================
%%% API
%%%===================================================================

calc_multipart_2i_dict(Ms, Bucket, _Key) when is_list(Ms) ->
    %% According to API Version 2006-03-01, page 139-140, bucket
    %% owners have some privileges for multipart uploads performed by
    %% other users, i.e, see those MP uploads via list multipart uploads,
    %% and cancel multipart upload.  We use two different 2I index entries
    %% to allow 2I to do the work of segregating multipart upload requests
    %% of bucket owner vs. non-bucket owner via two different 2I entries,
    %% one that includes the object owner and one that does not.
    L_2i = [
            case proplists:get_value(multipart, M?MANIFEST.props) of
                undefined ->
                    [];
                MP when is_record(MP, ?MULTIPART_MANIFEST_RECNAME) ->
                    %% OwnerVal = term_to_binary({Key, MP?MULTIPART_MANIFEST.upload_id}),
                    [{make_2i_key(Bucket, MP?MULTIPART_MANIFEST.owner), <<"1">>},
                     {make_2i_key(Bucket), <<"1">>}]
            end || M <- Ms,
                   M?MANIFEST.state == writing],
    {?MD_INDEX, lists:usort(lists:flatten(L_2i))}.

cancel_multipart_upload(x, y) ->
    foo.

%% riak_cs_mp_utils:write_new_manifest(riak_cs_mp_utils:new_manifest(<<"test">>, <<"mp0">>, <<"text/plain">>, {"foobar", "18983ba0e16e18a2b103ca16b84fad93d12a2fbed1c88048931fb91b0b844ad3", "J2IP6WGUQ_FNGIAN9AFI"})).
initiate_multipart_upload(Bucket, Key, ContentType, {_,_,_} = Owner) ->
    write_new_manifest(new_manifest(Bucket, Key, ContentType, Owner)).

list_multipart_uploads(Bucket, Owner) ->
    case riak_cs_utils:riak_connection() of
        {ok, RiakcPid} ->
            try
                Key2i = make_2i_key(Bucket, Owner),
                HashBucket = riak_cs_utils:to_bucket_name(objects, Bucket),
                case riakc_pb_socket:get_index(RiakcPid, HashBucket, Key2i,
                                               <<"1">>) of
                    {ok, Names} ->
                        list_multipart_uploads2(Bucket, RiakcPid, Names);
                    Else2 ->
                        Else2
                end
            after
                riak_cs_utils:close_riak_connection(RiakcPid)
            end;
        Else ->
            Else
    end.

%% @doc
-spec new_manifest(binary(), binary(), string(), acl_owner()) -> multipart_manifest().
new_manifest(Bucket, Key, ContentType, {_, _, _} = Owner) ->
    UUID = druuid:v4(),
    M = riak_cs_lfs_utils:new_manifest(Bucket,
                                       Key,
                                       UUID,
                                       0,
                                       ContentType,
                                       %% we won't know the md5 of a multipart
                                       undefined,
                                       [],
                                       riak_cs_lfs_utils:block_size(),
                                       %% ACL: needs Riak client pid, so we wait
                                       no_acl_yet),
    MpM = ?MULTIPART_MANIFEST{upload_id = UUID,
                              owner = Owner},
    M?MANIFEST{props = [{multipart, MpM}|M?MANIFEST.props]}.

write_new_manifest(M) ->
    %% TODO: ACL, cluster_id
    MpM = proplists:get_value(multipart, M?MANIFEST.props),
    Owner = MpM?MULTIPART_MANIFEST.owner,
    case riak_cs_utils:riak_connection() of
        {ok, RiakcPid} ->
            try
                Acl = riak_cs_acl_utils:canned_acl(undefined, Owner, unused, unused),
                ClusterId = riak_cs_utils:get_cluster_id(RiakcPid),
                M2 = M?MANIFEST{acl = Acl,
                                cluster_id = ClusterId,
                                write_start_time=os:timestamp()},
                {Bucket, Key} = M?MANIFEST.bkey,
                {ok, ManiPid} = riak_cs_manifest_fsm:start_link(Bucket, Key, RiakcPid),
                try
                    ok = riak_cs_manifest_fsm:add_new_manifest(ManiPid, M2)
                after
                    ok = riak_cs_manifest_fsm:stop(ManiPid)
                end
            after
                riak_cs_utils:close_riak_connection(RiakcPid)
            end;
        Else ->
            Else
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

make_2i_key(Bucket) ->
    make_2i_key2(Bucket, "").

make_2i_key(Bucket, {_, _, OwnerStr}) ->
    make_2i_key2(Bucket, OwnerStr);
make_2i_key(Bucket, OwnerStr) when is_list(OwnerStr) ->
    make_2i_key2(Bucket, OwnerStr).

make_2i_key2(Bucket, OwnerStr) ->
    iolist_to_binary(["rcs@", OwnerStr, "@", Bucket, "_bin"]).

list_multipart_uploads2(Bucket, RiakcPid, Names) ->
    lists:foldl(fun fold_get_multipart_id/2, {RiakcPid, Bucket, []}, Names).

fold_get_multipart_id(Name, {RiakcPid, Bucket, Acc}) ->
    case riak_cs_utils:get_manifests(RiakcPid, Bucket, Name) of
        {ok, _Obj, Manifests} ->
            [?MULTIPART_DESCR{
                key = element(2, M?MANIFEST.bkey),
                upload_id = UUID,
                owner_key_id = element(3, MpM?MULTIPART_MANIFEST.owner),
                owner_display = element(1, MpM?MULTIPART_MANIFEST.owner),
                initiated = M?MANIFEST.created} ||
                {UUID, M} <- Manifests,
                M?MANIFEST.state == writing,
                MpM <- case proplists:get_value(multipart, M?MANIFEST.props) of
                           undefined -> [];
                           X         -> [X]
                       end]
                ++ Acc;
        _Else ->
            Acc
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-endif.
