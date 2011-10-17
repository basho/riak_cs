%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_blockall_auth).

-behavior(riak_moss_auth).

-include("riak_moss.hrl").

-export([authenticate/1]).

-spec authenticate(term()) -> {ok, #moss_user{}}
                        | {ok, unknown}
                        | {error, atom()}.
authenticate(_RD) ->
    %% TODO:
    %% is there a better
    %% atom we could return?
    {error, invalid_authentication}.

