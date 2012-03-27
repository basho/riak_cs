-record(moss_user, {
          name :: string(),
          key_id :: string(),
          key_secret :: string(),
          buckets = []}).

-record(moss_user_v1, {
          name :: string(),
          display_name :: string(),
          email :: string(),
          key_id :: string(),
          key_secret :: string(),
          canonical_id :: string(),
          buckets=[] :: [moss_bucket()]}).
-type moss_user() :: #moss_user_v1{}.

-record(moss_bucket, {
          name :: string(),
          creation_date :: term(),
          acl :: acl()}).

-record(moss_bucket_v1, {
          name :: string(),
          last_action :: created | deleted,
          creation_date :: string(),
          modification_time :: erlang:timestamp(),
          acl :: acl()}).
-type moss_bucket() :: #moss_bucket_v1{}.
-type bucket_operation() :: create | delete | update_acl.
-type bucket_action() :: created | deleted.

-record(context, {auth_bypass :: atom(),
                  user :: moss_user(),
                  user_vclock :: term(),
                  bucket :: binary(),
                  requested_perm :: acl_perm()
                 }).

-record(key_context, {context :: #context{},
                      manifest :: lfs_manifest(),
                      get_fsm_pid :: pid(),
                      putctype :: string(),
                      bucket :: binary(),
                      key :: list(),
                      owner :: string(),
                      size :: non_neg_integer()}).

-type acl_perm() :: 'READ' | 'WRITE' | 'READ_ACP' | 'WRITE_ACP' | 'FULL_CONTROL'.
-type acl_perms() :: [acl_perm()].
-type group_grant() :: 'AllUsers' | 'AuthUsers'.
-type acl_grantee() :: {string(), string()} | group_grant().
-type acl_grant() :: {acl_grantee(), acl_perms()}.
-type acl_owner() :: {string(), string()} | {string(), string(), string()}.
-record(acl_v1, {owner={"", ""} :: acl_owner(),
                 grants=[] :: [acl_grant()],
                 creation_time=now() :: erlang:timestamp()}).
-record(acl_v2, {owner={"", "", ""} :: acl_owner(),
                 grants=[] :: [acl_grant()],
                 creation_time=now() :: erlang:timestamp()}).
-type acl() :: #acl_v1{} | #acl_v2{}.

-type cluster_id() :: undefined | term().  % Type still in flux.

-record(lfs_manifest_v2, {
        %% "global" properties
        %% -----------------------------------------------------------------

        %% this isn't as important anymore
        %% since we're naming the record
        %% to include the version number,
        %% but I figured it's worth keeping
        %% in case we change serialization
        %% formats in the future.
        version=2 :: integer(),

        %% the block_size setting when this manifest
        %% was written. Needed if the user
        %% ever changes the block size after writing
        %% data
        block_size :: integer(),

        %% identifying properties
        %% -----------------------------------------------------------------
        bkey :: {binary(), binary()},

        %% user metadata that would normally
        %% be placed on the riak_object. We avoid
        %% putting it on the riak_object so that
        %% we can use that metadata ourselves
        metadata :: orddict:new(),

        %% the date the manifest was created.
        %% not sure if we need both this and
        %% write_start_time. My thought was that
        %% write_start_time would have millisecond
        %% resolution, but I suppose there's no
        %% reason we can't change created
        %% to have millisecond as well.
        created=riak_moss_wm_utils:iso_8601_datetime(),
        uuid :: binary(),

        %% content properties
        %% -----------------------------------------------------------------
        content_length :: integer(),
        content_type :: binary(),
        content_md5 :: term(),

        %% state properties
        %% -----------------------------------------------------------------
        state=undefined :: undefined | writing | active | pending_delete | deleted,

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
        write_blocks_remaining :: ordsets:new(),

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
        delete_blocks_remaining :: ordsets:new(),

        %% The ACL for the version of the object represented
        %% by this manifest.
        acl :: acl(),

        %% There are a couple of cases where we want to add record
        %% member'ish data without adding new members to the record,
        %% e.g.
        %%    1. Data for which the common value is 'undefined' or not
        %%       used/set for this particular manifest
        %%    2. Cases where we do want to change the structure of the
        %%       record but don't want to go through the full code
        %%       refactoring and backward-compatibility tap dance
        %%       until sometime later.
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
-type lfs_manifest() :: #lfs_manifest_v2{}.

-define(ACL, #acl_v2).
-define(MOSS_BUCKET, #moss_bucket_v1).
-define(MOSS_USER, #moss_user_v1).
-define(USER_BUCKET, <<"moss.users">>).
-define(ACCESS_BUCKET, <<"moss.access">>).
-define(STORAGE_BUCKET, <<"moss.storage">>).
-define(BUCKETS_BUCKET, <<"moss.buckets">>).
-define(FREE_BUCKET_MARKER, <<"0">>).
-define(DEFAULT_MAX_CONTENT_LENGTH, 5368709120). %% 5 GB
-define(DEFAULT_LFS_BLOCK_SIZE, 1048576).%% 1 MB
-define(XML_PROLOG, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>").
-define(DEFAULT_STANCHION_IP, "127.0.0.1").
-define(DEFAULT_STANCHION_PORT, 8085).
-define(DEFAULT_STANCHION_SSL, true).
-define(MD_ACL, "X-Moss-Acl").
-define(EMAIL_INDEX, <<"email_bin">>).
-define(ID_INDEX, <<"c_id_bin">>).
-define(AUTH_USERS_GROUP, "http://acs.amazonaws.com/groups/global/AuthenticatedUsers").
-define(ALL_USERS_GROUP, "http://acs.amazonaws.com/groups/global/AllUsers").
-define(LOG_DELIVERY_GROUP, "http://acs.amazonaws.com/groups/s3/LogDelivery").
-define(DEFAULT_FETCH_CONCURRENCY, 1).
-define(DEFAULT_PUT_CONCURRENCY, 1).
%% A number to multiplied with the block size
%% to determine the PUT buffer size.
%% ex. 2 would mean BlockSize * 2
-define(DEFAULT_PUT_BUFFER_FACTOR, 1).
