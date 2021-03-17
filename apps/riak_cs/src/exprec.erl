-module(exprec).

-include("riak_cs.hrl").

-compile(export_all).

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
