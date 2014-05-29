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
-include_lib("riakc/include/riakc.hrl").

%% 'keys()` is defined in `riakc.hrl'.
%% The name is general so declare local type for readability.
-type index_result_keys() :: keys().

-record(gc_d_state, {
          interval :: 'infinity' | non_neg_integer(),
          %% the last time a deletion was scheduled
          last :: undefined | non_neg_integer(),
          %% the next scheduled gc time
          next :: undefined | non_neg_integer(),
          %% start of the current gc interval
          batch_start :: undefined | non_neg_integer(),
          %% caller of manual_batch
          %% Currently only used in `riak_cs_gc_single_run_eqc`.
          batch_caller :: undefined | pid(),
          batch_count=0 :: non_neg_integer(),
          %% Count of filesets skipped in this batch
          batch_skips=0 :: non_neg_integer(),
          batch=[] :: undefined | [index_result_keys()], % `undefined' only for testing
          manif_count=0 :: non_neg_integer(),
          block_count=0 :: non_neg_integer(),
          %% state of the fsm when a delete batch was paused
          pause_state :: undefined | atom(),
          %% used when moving from paused -> idle
          interval_remaining :: undefined | non_neg_integer(),
          timer_ref :: reference(),
          initial_delay :: non_neg_integer(),
          leeway :: non_neg_integer(),
          worker_pids=[] :: [pid()],
          max_workers :: non_neg_integer(),
          active_workers=0 :: non_neg_integer(),
          %% Used for paginated 2I querying of GC bucket
          key_list_state :: undefined | gc_key_list_state(),
          %% Options to use when start workers
          bag_id :: binary(),
          testing=false :: boolean()
         }).

-record(gc_worker_state, {
          %% Riak connection pid
          riak_client :: undefined | riak_client(),
          current_files :: [lfs_manifest()],
          current_fileset :: twop_set:twop_set(),
          current_riak_object :: riakc_obj:riakc_obj(),
          %% Count of filesets collected successfully
          batch_count=0 :: non_neg_integer(),
          %% Count of filesets skipped in this batch
          batch_skips=0 :: non_neg_integer(),
          batch=[] :: undefined | [binary()], % `undefined' only for testing
          manif_count=0 :: non_neg_integer(),
          block_count=0 :: non_neg_integer(),
          delete_fsm_pid :: pid()
         }).

-record(gc_key_list_state, {
          remaining_bags :: [{bag_id(), string(), non_neg_integer()}],
          %% Riak connection pid
          current_riak_client :: undefined | riak_client(),
          current_bag_id :: bag_id(),
          %% start of the current gc interval
          batch_start :: undefined | non_neg_integer(),
          leeway :: non_neg_integer(),
          %% Used for paginated 2I querying of GC bucket
          continuation :: continuation()
         }).

-record(gc_key_list_result, {
          bag_id :: bag_id(),
          batch :: [index_result_keys()]
         }).

-type gc_key_list_state() :: #gc_key_list_state{}.
-type gc_key_list_result() :: #gc_key_list_result{}.

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
