-module(exprec).

-include("riak_cs.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-dialyzer([ {nowarn_function, fromlist_access_v1/1}
          , {nowarn_function, fromlist_context/1}
          , {nowarn_function, fromlist_key_context/1}
          , {nowarn_function, fromlist_lfs_manifest_v2/1}
          , {nowarn_function, fromlist_lfs_manifest_v3/1}
          , {nowarn_function, fromlist_moss_bucket/1}
          , {nowarn_function, fromlist_moss_bucket_v1/1}
          , {nowarn_function, fromlist_moss_user/1}
          , {nowarn_function, fromlist_moss_user_v1/1}
          , {nowarn_function, fromlist_multipart_descr_v1/1}
          , {nowarn_function, fromlist_multipart_manifest_v1/1}
          , {nowarn_function, fromlist_part_descr_v1/1}
          , {nowarn_function, fromlist_part_manifest_v1/1}
          , {nowarn_function, fromlist_rcs_user_v2/1}
          , {nowarn_function, frommap_access_v1/1}
          , {nowarn_function, frommap_context/1}
          , {nowarn_function, frommap_key_context/1}
          , {nowarn_function, frommap_lfs_manifest_v2/1}
          , {nowarn_function, frommap_lfs_manifest_v3/1}
          , {nowarn_function, frommap_moss_bucket/1}
          , {nowarn_function, frommap_moss_bucket_v1/1}
          , {nowarn_function, frommap_moss_user/1}
          , {nowarn_function, frommap_moss_user_v1/1}
          , {nowarn_function, frommap_multipart_descr_v1/1}
          , {nowarn_function, frommap_multipart_manifest_v1/1}
          , {nowarn_function, frommap_part_descr_v1/1}
          , {nowarn_function, frommap_part_manifest_v1/1}
          , {nowarn_function, frommap_rcs_user_v2/1}
          , {nowarn_function, new_access_v1/1}
          , {nowarn_function, new_context/1}
          , {nowarn_function, new_key_context/1}
          , {nowarn_function, new_lfs_manifest_v2/1}
          , {nowarn_function, new_lfs_manifest_v3/1}
          , {nowarn_function, new_moss_bucket/1}
          , {nowarn_function, new_moss_bucket_v1/1}
          , {nowarn_function, new_moss_user/1}
          , {nowarn_function, new_moss_user_v1/1}
          , {nowarn_function, new_multipart_descr_v1/1}
          , {nowarn_function, new_multipart_manifest_v1/1}
          , {nowarn_function, new_part_descr_v1/1}
          , {nowarn_function, new_part_manifest_v1/1}
          , {nowarn_function, new_rcs_user_v2/1}
          , {nowarn_function, new_access_v1/0}
          , {nowarn_function, new_context/0}
          , {nowarn_function, new_key_context/0}
          , {nowarn_function, new_lfs_manifest_v2/0}
          , {nowarn_function, new_lfs_manifest_v3/0}
          , {nowarn_function, new_moss_bucket/0}
          , {nowarn_function, new_moss_bucket_v1/0}
          , {nowarn_function, new_moss_user/0}
          , {nowarn_function, new_moss_user_v1/0}
          , {nowarn_function, new_multipart_descr_v1/0}
          , {nowarn_function, new_multipart_manifest_v1/0}
          , {nowarn_function, new_part_descr_v1/0}
          , {nowarn_function, new_part_manifest_v1/0}
          , {nowarn_function, new_rcs_user_v2/0}
          ]).

-define(ALL_RECORDS,
        [ moss_user
        , moss_user_v1
        , rcs_user_v2
        , moss_bucket
        , moss_bucket_v1
        , context
        , key_context
        , acl_v1
        , acl_v2
        , lfs_manifest_v2
        , lfs_manifest_v3
        , part_manifest_v1
        , multipart_manifest_v1
        , multipart_descr_v1
        , part_descr_v1
        , access_v1
        ]
       ).

-export_records(?ALL_RECORDS).

-exprecs_prefix(["", operation, ""]).
-exprecs_fname([prefix, "_", record]).
-exprecs_vfname([fname, "__", version]).
-compile({parse_transform, exprecs}).
