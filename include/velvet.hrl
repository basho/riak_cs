%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-define(DEFAULT_TIMEOUT, 60000).

-record(velvet, {ip :: string(),
              port :: pos_integer(),
              ssl :: boolean()}).
-type velvet() :: #velvet{}.
