%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.

%% Riak's bucket and key to store weight information
-define(WEIGHT_BUCKET, <<"riak-cs-multibag">>).
-define(WEIGHT_KEY,    <<"weight">>).

-record(weight_info, {
          bag_id :: bag_id(),
          weight :: non_neg_integer(),
          opts = [] :: proplists:proplist()   %% Not used
          }).

-define(MD_BAG, <<"X-Rcs-Bag">>).

-type bag_id() :: undefined | binary().
