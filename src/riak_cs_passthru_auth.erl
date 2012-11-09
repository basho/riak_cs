%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_passthru_auth).

-behavior(riak_cs_auth).

-include("riak_cs.hrl").

-export([identify/2, authenticate/4]).

-spec identify(term(),term()) -> {string() | undefined, undefined}.
identify(RD,_Ctx) ->
    case wrq:get_req_header("authorization", RD) of
        undefined -> {[], undefined};
        Key -> {Key, undefined}
    end.
            

-spec authenticate(rcs_user(), undefined, term(), term()) -> ok.
authenticate(_User, _AuthData, _RD, _Ctx) ->
    ok.
