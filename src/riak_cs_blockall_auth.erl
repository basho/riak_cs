%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_blockall_auth).

-behavior(riak_cs_auth).

-include("riak_cs.hrl").

-export([authenticate/3]).

-spec authenticate(term(), string(), undefined) -> {error, atom()}.
authenticate(_RD, Reason, undefined) ->
    {error, Reason}.
