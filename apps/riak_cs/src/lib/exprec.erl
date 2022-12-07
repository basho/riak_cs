%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
%%               2021, 2022 TI Tokyo    All Rights Reserved.
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

-module(exprec).

-include("riak_cs.hrl").
-include("stanchion.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-dialyzer([ {nowarn_function, fromlist_access_v1/1}
          , {nowarn_function, fromlist_rcs_context/1}
          , {nowarn_function, fromlist_key_context/1}
          , {nowarn_function, fromlist_stanchion_context/1}
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
          , {nowarn_function, frommap_rcs_context/1}
          , {nowarn_function, frommap_key_context/1}
          , {nowarn_function, frommap_stanchion_context/1}
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
          , {nowarn_function, new_rcs_context/1}
          , {nowarn_function, new_key_context/1}
          , {nowarn_function, new_stanchion_context/1}
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
          , {nowarn_function, new_rcs_context/0}
          , {nowarn_function, new_key_context/0}
          , {nowarn_function, new_stanchion_context/0}
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
        , rcs_context
        , key_context
        , stanchion_context
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
