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

-record(state, {
          interval :: 'infinity' | non_neg_integer(),
          last :: undefined | calendar:datetime(), % the last time a deletion was scheduled
          next :: undefined | calendar:datetime(), % the next scheduled gc time
          riak :: undefined | pid(), % Riak connection pid
          current_files :: [lfs_manifest()],
          current_fileset :: twop_set:twop_set(),
          current_riak_object :: riakc_obj:riakc_obj(),
          batch_start :: undefined | non_neg_integer(), % start of the current gc interval
          batch_count=0 :: non_neg_integer(),
          batch_skips=0 :: non_neg_integer(), % Count of filesets skipped in this batch
          batch=[] :: undefined | [binary()], % `undefined' only for testing
          manif_count=0 :: non_neg_integer(),
          block_count=0 :: non_neg_integer(),
          pause_state :: undefined | atom(), % state of the fsm when a delete batch was paused
          interval_remaining :: undefined | non_neg_integer(), % used when moving from paused -> idle
          timer_ref :: reference(),
          delete_fsm_pid :: pid()
         }).
