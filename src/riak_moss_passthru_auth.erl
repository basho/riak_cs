%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_passthru_auth).

-behavior(riak_moss_auth).

-include("riak_moss.hrl").

-export([authenticate/3]).

-spec authenticate(term(), string(), undefined) -> ok.
authenticate(_RD, _KeyData, _) ->
    ok.
