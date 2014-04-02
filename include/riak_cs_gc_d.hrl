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

-include("riak_cs.hrl").

-record(gc_d_state, {
          interval :: 'infinity' | non_neg_integer(),
          last :: undefined | non_neg_integer(), % the last time a deletion was scheduled
          next :: undefined | non_neg_integer(), % the next scheduled gc time
          riak :: undefined | pid(), % Riak connection pid
          batch_start :: undefined | non_neg_integer(), % start of the current gc interval
          batch_caller :: undefined | pid(), % caller of manual_batch
          batch_count=0 :: non_neg_integer(),
          batch_skips=0 :: non_neg_integer(), % Count of filesets skipped in this batch
          batch=[] :: undefined | [binary()], % `undefined' only for testing
          manif_count=0 :: non_neg_integer(),
          block_count=0 :: non_neg_integer(),
          pause_state :: undefined | atom(), % state of the fsm when a delete batch was paused
          interval_remaining :: undefined | non_neg_integer(), % used when moving from paused -> idle
          timer_ref :: reference(),
          initial_delay :: non_neg_integer(),
          leeway :: non_neg_integer(),
          worker_pids=[] :: [pid()],
          max_workers :: non_neg_integer(),
          active_workers=0 :: non_neg_integer(),
          continuation :: undefined | binary(), % Used for paginated 2I querying of GC bucket
          testing=false :: boolean()
         }).

-record(gc_worker_state, {
          riak_pid :: undefined | pid(), % Riak connection pid
          current_files :: [lfs_manifest()],
          current_fileset :: twop_set:twop_set(),
          current_riak_object :: riakc_obj:riakc_obj(),
          batch_count=0 :: non_neg_integer(), % Count of filesets collected successfully
          batch_skips=0 :: non_neg_integer(), % Count of filesets skipped in this batch
          batch=[] :: undefined | [binary()], % `undefined' only for testing
          manif_count=0 :: non_neg_integer(),
          block_count=0 :: non_neg_integer(),
          delete_fsm_pid :: pid()
         }).


%% Number of seconds to keep manifests in the `scheduled_delete' state
%% before beginning to delete the file blocks and before the file
%% manifest may be pruned.
-define(DEFAULT_LEEWAY_SECONDS, 86400). %% 24-hours
-define(DEFAULT_GC_INTERVAL, 900). %% 15 minutes
-define(DEFAULT_GC_RETRY_INTERVAL, 21600). %% 6 hours
-define(DEFAULT_GC_KEY_SUFFIX_MAX, 256).
-define(DEFAULT_GC_BATCH_SIZE, 1000).
-define(DEFAULT_GC_WORKERS, 5).
-define(EPOCH_START, <<"0">>).
