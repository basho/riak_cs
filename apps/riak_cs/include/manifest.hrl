%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

-ifndef(RCS_COMMON_MANIFEST_HRL).
-define(RCS_COMMON_MANIFEST_HRL, included).

-include("acl.hrl").


-define(MANIFEST, #lfs_manifest_v4).
-define(MULTIPART_MANIFEST, #multipart_manifest_v1).
-define(MULTIPART_MANIFEST_RECNAME, multipart_manifest_v1).
-define(PART_MANIFEST, #part_manifest_v2).
-define(PART_MANIFEST_RECNAME, part_manifest_v2).
-define(MULTIPART_DESCR, #multipart_descr_v1).
-define(PART_DESCR, #part_descr_v1).

-define(LFS_DEFAULT_OBJECT_VERSION, <<"null">>).


-type lfs_manifest_state() :: writing | active |
                              pending_delete | scheduled_delete | deleted.


-type cluster_id() :: undefined | binary(). %% flattened string as binary
-type cs_uuid() :: binary().
-type bag_id() :: undefined | binary().

-record(lfs_manifest_v2, {
        version = 2 :: 2,
        block_size :: integer(),
        bkey :: {binary(), binary()},
        metadata :: orddict:orddict(),
        created :: string(),
        uuid :: cs_uuid(),
        content_length :: non_neg_integer(),
        content_type :: binary(),
        content_md5 :: term(),
        state = undefined :: undefined | lfs_manifest_state(),
        write_start_time :: term(), %% immutable
        last_block_written_time :: term(),
        write_blocks_remaining :: ordsets:ordset(integer()),
        delete_marked_time :: term(),
        last_block_deleted_time :: term(),
        delete_blocks_remaining :: ordsets:ordset(integer()),
        acl :: acl(),
        props = [] :: proplists:proplist(),
        cluster_id :: cluster_id()
    }).

-record(lfs_manifest_v3, {
        version = 3 :: 3,

        block_size :: undefined | integer(),

        bkey :: {binary(), binary()},

        metadata :: orddict:orddict(),

        created :: string(),
        uuid :: cs_uuid(),

        content_length :: non_neg_integer(),
        content_type :: binary(),
        content_md5 :: term(),

        state :: undefined | lfs_manifest_state(),

        write_start_time :: term(), %% immutable
        last_block_written_time :: term(),
        write_blocks_remaining :: undefined | ordsets:ordset(integer()),
        delete_marked_time :: term(),
        last_block_deleted_time :: term(),
        delete_blocks_remaining :: undefined | ordsets:ordset(integer()),
        scheduled_delete_time :: term(),  %% new in v3

        acl = no_acl_yet :: acl() | no_acl_yet,

        props = [] :: undefined | proplists:proplist(),

        cluster_id :: cluster_id()
    }).

-record(lfs_manifest_v4, {
        %% "global" properties
        %% -----------------------------------------------------------------

        %% this isn't as important anymore
        %% since we're naming the record
        %% to include the version number,
        %% but I figured it's worth keeping
        %% in case we change serialization
        %% formats in the future.
        version = 4 :: 4,

        %% the block_size setting when this manifest
        %% was written. Needed if the user
        %% ever changes the block size after writing
        %% data
        block_size :: undefined | integer(),

        %% identifying properties
        %% -----------------------------------------------------------------
        bkey :: {binary(), binary()},
        %% added in v4:
        %% there's always a primary version, which is head of a
        %% double-linked list of all versions
        vsn = ?LFS_DEFAULT_OBJECT_VERSION :: binary(),

        %% user metadata that would normally
        %% be placed on the riak_object. We avoid
        %% putting it on the riak_object so that
        %% we can use that metadata ourselves
        metadata :: orddict:orddict(),

        %% the date the manifest was created.
        %% not sure if we need both this and
        %% write_start_time. My thought was that
        %% write_start_time would have millisecond
        %% resolution, but I suppose there's no
        %% reason we can't change created
        %% to have millisecond as well.
        created :: string(),
        uuid :: cs_uuid(),

        %% content properties
        %% -----------------------------------------------------------------
        content_length :: non_neg_integer(),
        content_type :: binary(),
        content_md5 :: term(),

        %% state properties
        %% -----------------------------------------------------------------
        state :: undefined | lfs_manifest_state(),

        %% writing/active state
        %% -----------------------------------------------------------------
        write_start_time :: term(), %% immutable

        %% used for two purposes
        %% 1. to mark when a file has finished uploading
        %% 2. to decide if a write crashed before completing
        %% and needs to be garbage collected
        last_block_written_time :: term(),

        %% a shrink-only (during resolution)
        %% set to denote which blocks still
        %% need to be written. We use a shrinking
        %% (rather than growing) set to that the
        %% set is empty after the write has completed,
        %% which should be most of the lifespan on disk
        write_blocks_remaining :: undefined | ordsets:ordset(integer()),

        %% pending_delete/deleted state
        %% -----------------------------------------------------------------
        %% set to the current time
        %% when a manifest is marked as deleted
        %% and enters the pending_delete state
        delete_marked_time :: term(),

        %% the timestamp serves a similar
        %% purpose to last_block_written_time,
        %% in that it's used for figuring out
        %% when delete processes have died
        %% and garbage collection needs to
        %% pick up where they left off.
        last_block_deleted_time :: term(),

        %% a shrink-only (during resolution)
        %% set to denote which blocks
        %% still need to be deleted.
        %% See write_blocks_remaining for
        %% an explanation of why we chose
        %% a shrinking set
        delete_blocks_remaining :: undefined | ordsets:ordset(integer()),

        %% the time the manifest was put
        %% into the scheduled_delete
        %% state
        scheduled_delete_time :: term(),

        %% The ACL for the version of the object represented
        %% by this manifest.
        acl = no_acl_yet :: acl() | no_acl_yet,

        %% There are a couple of cases where we want to add record
        %% member'ish data without adding new members to the record,
        %% e.g.
        %%    1. Data for which the common value is 'undefined' or not
        %%       used/set for this particular manifest
        %%    2. Cases where we do want to change the structure of the
        %%       record but don't want to go through the full code
        %%       refactoring and backward-compatibility tap dance
        %%       until sometime later.
        %% 'undefined' is for backward compatibility with v3 manifests
        %% written with Riak CS 1.2.2 or earlier.
        props = [] :: proplists:proplist(),

        %% cluster_id: A couple of uses, both short- and longer-term
        %%  possibilities:
        %%
        %%  1. We don't have a good story in early 2012 for how to
        %%     build a stable 2,000 node Riak cluster.  If MOSS can
        %%     talk to multiple Riak clusters, then each individual
        %%     cluster can be a size that we're comfortable
        %%     supporting.
        %%
        %%  2. We may soon have Riak EE's replication have full
        %%     plumbing to make it feasible to forward arbitrary
        %%     traffic between clusters.  Then if a slave cluster is
        %%     missing a data block, and read-repair cannot
        %%     automagically fix the 'not_found' problem, then perhaps
        %%     forwarding a get request to the source Riak cluster can
        %%     fetch us the missing data.
        cluster_id :: cluster_id()
    }).
-type lfs_manifest() :: #lfs_manifest_v4{}.

-type cs_uuid_and_manifest() :: {cs_uuid(), lfs_manifest()}.
-type wrapped_manifest() :: orddict:orddict(cs_uuid(), lfs_manifest()).

-record(part_manifest_v1, {
    bucket :: binary(),
    key :: binary(),
    start_time :: erlang:timestamp(),
    part_number :: integer(),
    part_id :: binary(),
    content_length :: integer(),
    content_md5 :: undefined | binary(),
    block_size :: integer()
}).

-record(part_manifest_v2, {
    bucket :: binary(),
    key :: binary(),
    %% new in v2
    vsn = ?LFS_DEFAULT_OBJECT_VERSION :: binary(),

    %% used to judge races between concurrent uploads
    %% of the same part_number
    start_time :: erlang:timestamp(),

    %% one-of 1-10000, inclusive
    part_number :: integer(),

    %% a UUID to prevent conflicts with concurrent
    %% uploads of the same {upload_id, part_number}.
    part_id :: binary(),

    %% each individual part upload always has a content-length
    %% content_md5 is used for the part ETag, alas.
    content_length :: integer(),
    content_md5 :: undefined | binary(),

    %% block size just like in `lfs_manifest_v2'. Concievably,
    %% parts for the same upload id could have different block_sizes.
    block_size :: integer()
}).
-type part_manifest() :: #part_manifest_v2{}.

-record(multipart_manifest_v1, {
    upload_id :: binary(),
    owner :: acl_owner(),

    %% since we don't have any point of strong
    %% consistency (other than stanchion), we
    %% can get concurrent `complete' and `abort'
    %% requests. There are still some details to
    %% work out, but what we observe here will
    %% affect whether we accept future `complete'
    %% or `abort' requests.

    %% Stores references to all of the parts uploaded
    %% with this `upload_id' so far. A part
    %% can be uploaded more than once with the same
    %% part number.  type = #part_manifest_vX
    parts = ordsets:new() :: ordsets:ordset(?PART_MANIFEST{}),
    %% List of UUIDs for parts that are done uploading.
    %% The part number is redundant, so we only store
    %% {UUID::binary(), PartETag::binary()} here.
    done_parts = ordsets:new() :: ordsets:ordset({binary(), binary()}),
    %% type = #part_manifest_vX
    cleanup_parts = ordsets:new() :: ordsets:ordset(?PART_MANIFEST{}),

    %% a place to stuff future information
    %% without having to change
    %% the record format
    props = [] :: proplists:proplist()
}).
-type multipart_manifest() :: #multipart_manifest_v1{}.

%% Basis of list multipart uploads output
-record(multipart_descr_v1, {
    %% Object key for the multipart upload
    key :: binary(),

    %% UUID of the multipart upload
    upload_id :: binary(),

    %% User that initiated the upload
    owner_display :: string(),
    owner_key_id :: string(),

    %% storage class: no real options here
    storage_class = standard,

    %% Time that the upload was initiated
    initiated :: string() %% conflict of func vs. type: riak_cs_wm_utils:iso_8601_datetime()
}).
-type multipart_descr() :: #multipart_descr_v1{}.

%% Basis of multipart list parts output
-record(part_descr_v1, {
    part_number :: integer(),
    last_modified :: string(),  % TODO ??
    etag :: binary(),
    size :: integer()
}).

-type part_descr() :: #part_descr_v1{}.


-endif.
