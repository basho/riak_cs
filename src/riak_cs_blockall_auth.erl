%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_blockall_auth).

-behavior(riak_cs_auth).

-include("riak_cs.hrl").

-export([identify/2,authenticate/4]).

-spec identify(term(), term()) -> {undefined, block_all}.
identify(_RD,_Ctx) ->
    {undefined, block_all}.

-spec authenticate(rcs_user(), term(), term(), term()) -> ok | {error, term()}.
authenticate(_User, AuthData, _RD, _Ctx) ->
    {error, AuthData}.
