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
-compile(export_all).                           % SLF DEBUGGING ONLY!
-export([
         abort_multipart_upload/4,
         calc_multipart_2i_dict/3,
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

abort_multipart_upload(Bucket, Key, UploadId, {_,_,CallerKeyId}) ->
    %% TODO: ACL check of Bucket
    case riak_cs_utils:riak_connection() of
        {ok, RiakcPid} ->
            try
                case riak_cs_utils:get_manifests(RiakcPid, Bucket, Key) of
                    {ok, Obj, Manifests} ->
                        B_OwnerP = is_caller_bucket_owner(RiakcPid,
                                                          Bucket, CallerKeyId),
                        case find_manifest_with_uploadid(UploadId, Manifests) of
                            false ->
                                {error, todo_no_such_uploadid};
                            M ->
                                MpM = proplists:get_value(
                                        multipart, M?MANIFEST.props),
                                {_, _, MpMOwner} = MpM?MULTIPART_MANIFEST.owner,
                                case B_OwnerP orelse CallerKeyId == MpMOwner of
                                    true ->
                                        case M?MANIFEST.state of
                                            writing ->
                                                case riak_cs_gc:gc_specific_manifests(
                                                       [M?MANIFEST.uuid], Obj, RiakcPid) of
                                                    {ok, _NewObj} ->
                                                        ok;
                                                    Else3 ->
                                                        Else3
                                                end;
                                            _ ->
                                                {error, todo_no_such_uploadid2}
                                        end;
                                    false ->
                                        {error, todo_bad_caller}
                                end
                        end;
                    Else2 ->
                        Else2
                end
            catch error:{badmatch, {m_icbo, _}} ->
                    {error, todo_bad_caller}
            after
                riak_cs_utils:close_riak_connection(RiakcPid)
            end;
        Else ->
            Else
    end.

%% riak_cs_mp_utils:write_new_manifest(riak_cs_mp_utils:new_manifest(<<"test">>, <<"mp0">>, <<"text/plain">>, {"foobar", "18983ba0e16e18a2b103ca16b84fad93d12a2fbed1c88048931fb91b0b844ad3", "J2IP6WGUQ_FNGIAN9AFI"})).
initiate_multipart_upload(Bucket, Key, ContentType, {_,_,_} = Owner) ->
    write_new_manifest(new_manifest(Bucket, Key, ContentType, Owner)).

list_multipart_uploads(Bucket, {_Display, _Canon, CallerKeyId} = Caller) ->
    %% TODO: ACL check of Bucket
    case riak_cs_utils:riak_connection() of
        {ok, RiakcPid} ->
            try
                BucketOwnerP = is_caller_bucket_owner(RiakcPid,
                                                      Bucket, CallerKeyId),
                Key2i = case BucketOwnerP of
                            true ->
                                make_2i_key(Bucket); % caller = bucket owner
                            false ->
                                make_2i_key(Bucket, Caller)
                        end,
                HashBucket = riak_cs_utils:to_bucket_name(objects, Bucket),
                case riakc_pb_socket:get_index(RiakcPid, HashBucket, Key2i,
                                               <<"1">>) of
                    {ok, Names} ->
                        MyCaller = case BucketOwnerP of
                                       true -> owner;
                                       _    -> CallerKeyId
                                   end,
                        {ok, list_multipart_uploads2(Bucket, RiakcPid,
                                                     Names, MyCaller)};
                    Else2 ->
                        Else2
                end
            catch error:{badmatch, {m_icbo, _}} ->
                    {error, todo_bad_caller}
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
                    ok = riak_cs_manifest_fsm:add_new_manifest(ManiPid, M2),
                    {ok, M2?MANIFEST.uuid}
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

list_multipart_uploads2(Bucket, RiakcPid, Names, CallerKeyId) ->
    lists:foldl(fun fold_get_multipart_id/2, {RiakcPid, Bucket, CallerKeyId, []}, Names).

fold_get_multipart_id(Name, {RiakcPid, Bucket, CallerKeyId, Acc}) ->
    case riak_cs_utils:get_manifests(RiakcPid, Bucket, Name) of
        {ok, _Obj, Manifests} ->
            [?MULTIPART_DESCR{
                key = element(2, M?MANIFEST.bkey),
                upload_id = UUID,
                owner_key_id = element(3, MpM?MULTIPART_MANIFEST.owner),
                owner_display = element(1, MpM?MULTIPART_MANIFEST.owner),
                initiated = M?MANIFEST.created} ||
                {UUID, M} <- Manifests,
                CallerKeyId == owner orelse
                    iolist_to_binary(CallerKeyId) ==
                    iolist_to_binary(element(3, (M?MANIFEST.acl)?ACL.owner)),
                M?MANIFEST.state == writing,
                MpM <- case proplists:get_value(multipart, M?MANIFEST.props) of
                           undefined -> [];
                           X         -> [X]
                       end]
                ++ Acc;
        _Else ->
            Acc
    end.

%% @doc Will cause error:{badmatch, {m_ibco, _}} if CallerKeyId does not exist

is_caller_bucket_owner(RiakcPid, Bucket, CallerKeyId) ->
    {m_icbo, {ok, {C, _}}} = {m_icbo, riak_cs_utils:get_user(CallerKeyId,
                                                             RiakcPid)},
    Buckets = [iolist_to_binary(B?RCS_BUCKET.name) ||
                  B <- riak_cs_utils:get_buckets(C)],
    lists:member(Bucket, Buckets).

find_manifest_with_uploadid(UploadId, Manifests) ->
    case lists:keyfind(UploadId, 1, Manifests) of
        false ->
            false;
        {UploadId, M} ->
            M
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
%%%%%%%%%%%%%%%%%%%%%%%%-ifdef(TEST). % SLF debugging: put me back!
-ifndef(TESTfoo).

test_0() ->
    test_cleanup_users(),
    test_cleanup_data(),
    test_create_users(),

    ID1 = test_initiate(test_user1()),
    _ID2 = test_initiate(test_user2()),
    {ok, X1} = test_list_uploadids(test_user1()),
    2 = length(X1),
    {ok, X2} = test_list_uploadids(test_user2()),
    1 = length(X2),
    {error, todo_bad_caller} = test_list_uploadids(test_userNONE()),

    {error, todo_bad_caller} = test_abort(ID1, test_user2()),
    {error,todo_no_such_uploadid} = test_abort(<<"no such upload_id">>, test_user2()),
    ok = test_abort(ID1, test_user1()),
    {error, todo_no_such_uploadid2} = test_abort(ID1, test_user1()),

    ok.

test_initiate(User) ->
    {ok, ID1} = initiate_multipart_upload(
                  test_bucket1(), test_key1(), "text/plain", User),
    ID1.

test_abort(UploadId, User) ->
    abort_multipart_upload(test_bucket1(), test_key1(), UploadId, User).

test_list_uploadids(User) ->
    list_multipart_uploads(test_bucket1(), User).

test_create_users() ->
    %% info for test_user1()
    %% NOTE: This user has a "test" bucket in its buckets list,
    %%       therefore test_user1() is the owner of the "test" bucket.
    ok = test_put(<<"moss.users">>, <<"J2IP6WGUQ_FNGIAN9AFI">>, <<131,104,9,100,0,11,114,99,115,95,117,115,101,114,95,118,50,107,0,7,102,111,111,32,98,97,114,107,0,6,102,111,111,98,97,114,107,0,18,102,111,111,98,97,114,64,101,120,97,109,112,108,101,46,99,111,109,107,0,20,74,50,73,80,54,87,71,85,81,95,70,78,71,73,65,78,57,65,70,73,107,0,40,109,98,66,45,49,86,65,67,78,115,114,78,48,121,76,65,85,83,112,67,70,109,88,78,78,66,112,65,67,51,88,48,108,80,109,73,78,65,61,61,107,0,64,49,56,57,56,51,98,97,48,101,49,54,101,49,56,97,50,98,49,48,51,99,97,49,54,98,56,52,102,97,100,57,51,100,49,50,97,50,102,98,101,100,49,99,56,56,48,52,56,57,51,49,102,98,57,49,98,48,98,56,52,52,97,100,51,108,0,0,0,1,104,6,100,0,14,109,111,115,115,95,98,117,99,107,101,116,95,118,49,107,0,4,116,101,115,116,100,0,7,99,114,101,97,116,101,100,107,0,24,50,48,49,50,45,49,50,45,48,56,84,48,48,58,51,53,58,49,57,46,48,48,48,90,104,3,98,0,0,5,74,98,0,14,36,199,98,0,6,176,191,100,0,9,117,110,100,101,102,105,110,101,100,106,100,0,7,101,110,97,98,108,101,100>>),
    %% info for test_user2()
    ok = test_put(<<"moss.users">>, <<"LAHU4GBJIRQD55BJNET7">>, <<131,104,9,100,0,11,114,99,115,95,117,115,101,114,95,118,50,107,0,8,102,111,111,32,98,97,114,50,107,0,7,102,111,111,98,97,114,50,107,0,19,102,111,111,98,97,114,50,64,101,120,97,109,112,108,101,46,99,111,109,107,0,20,76,65,72,85,52,71,66,74,73,82,81,68,53,53,66,74,78,69,84,55,107,0,40,121,104,73,48,56,73,122,50,71,112,55,72,100,103,85,70,50,101,103,85,49,83,99,82,53,97,72,50,49,85,116,87,110,87,110,99,69,103,61,61,107,0,64,51,50,57,99,51,51,50,98,57,101,102,102,52,57,56,57,57,99,50,99,54,101,53,49,56,53,100,101,55,102,100,57,55,99,100,99,54,100,54,52,54,99,53,53,100,51,101,56,52,101,102,49,57,48,48,54,99,55,52,54,99,51,54,56,106,100,0,7,101,110,97,98,108,101,100>>),
    ok.

test_bucket1() ->
    <<"test">>.

test_key1() ->
    <<"mp0">>.

test_cleanup_data() ->
    _ = test_delete(test_hash_objects_bucket(test_bucket1()), test_key1()),
    ok.

test_cleanup_users() ->
    _ = test_delete(<<"moss.users">>, list_to_binary(element(3, test_user1()))),
    _ = test_delete(<<"moss.users">>, list_to_binary(element(3, test_user2()))),
    ok.

test_hash_objects_bucket(Bucket) ->
    riak_cs_utils:to_bucket_name(objects, Bucket).

test_delete(Bucket, Key) ->
    {ok, RiakcPid} = riak_cs_utils:riak_connection(),
    Res = riakc_pb_socket:delete(RiakcPid, Bucket, Key),
    riak_cs_utils:close_riak_connection(RiakcPid),
    Res.

test_put(Bucket, Key, Value) ->
    {ok, RiakcPid} = riak_cs_utils:riak_connection(),
    Res = riakc_pb_socket:put(RiakcPid, riakc_obj:new(Bucket, Key, Value)),
    riak_cs_utils:close_riak_connection(RiakcPid),
    Res.

test_user1() ->
    {"foobar", "18983ba0e16e18a2b103ca16b84fad93d12a2fbed1c88048931fb91b0b844ad3", "J2IP6WGUQ_FNGIAN9AFI"}.

test_user1_secret() ->
    "mbB-1VACNsrN0yLAUSpCFmXNNBpAC3X0lPmINA==".

test_user2() ->
    {"foobar2", "329c332b9eff49899c2c6e5185de7fd97cdc6d646c55d3e84ef19006c746c368", "LAHU4GBJIRQD55BJNET7"}.

test_user2_secret() ->
    "yhI08Iz2Gp7HdgUF2egU1ScR5aH21UtWnWncEg==".

test_userNONE() ->
    {"bar", "bar", "bar"}.

-endif.
