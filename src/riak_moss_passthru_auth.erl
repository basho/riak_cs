%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_passthru_auth).

-behavior(riak_moss_auth).

-include("riak_moss.hrl").

-export([authenticate/2]).

-spec authenticate(term(), [string()]) -> {ok, #rs3_user{}}
                                              | {ok, unknown}
                                              | {error, atom()}.
authenticate(_RD, []) ->
    %% TODO:
    %% Even though we're
    %% going to authorize
    %% no matter what,
    %% maybe it still makes sense
    %% to pass the user on
    {ok, unknown};
authenticate(_RD, KeyID) when is_list(KeyID) ->
    case riak_moss_riakc:get_user(KeyID) of
        {ok, User} ->
            {ok, User};
        _ ->
            {error, invalid_authentication}
    end.
