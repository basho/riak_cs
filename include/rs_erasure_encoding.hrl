%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-define(ALG_FAKE_V0,        'alg_fake0').
-define(ALG_CAUCHY_GOOD_V0, 'alg_cauchy_good0').

-type rs_ec_algorithm() :: ?ALG_FAKE_V0 | ?ALG_CAUCHY_GOOD_V0.
