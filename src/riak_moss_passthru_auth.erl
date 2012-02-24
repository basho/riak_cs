%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_passthru_auth).

-behavior(riak_moss_auth).

-include("riak_moss.hrl").

-export([authenticate/3]).

-spec authenticate(term(), string(), undefined) -> ok | {error, atom()}.
authenticate(_RD, [], _) ->
    %% TODO:
    %% Even though we're
    %% going to authorize
    %% no matter what,
    %% maybe it still makes sense
    %% to pass the user on
    {ok, unknown};
authenticate(_RD, KeyId, _) ->
    %% @TODO Also handle riak connection error
    case riak_moss_utils:get_user(KeyId) of
        {ok, User} ->
            {ok, User};
        _ ->
            {error, invalid_authentication}
    end.
