-record(state, {
          interval :: infinity | non_neg_integer(),
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
          pause_state :: atom(), % state of the fsm when a delete batch was paused
          interval_remaining :: undefined | non_neg_integer(), % used when moving from paused -> idle
          timer_ref :: reference(),
          delete_fsm_pid :: pid()
         }).
