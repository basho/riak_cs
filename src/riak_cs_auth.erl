%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_auth).

-export([behaviour_info/1]).

-compile(export_all).

-spec behaviour_info(atom()) -> 'undefined' | [{atom(), arity()}].
behaviour_info(callbacks) ->
    [{identify, 2}, {authenticate, 4}];
behaviour_info(_Other) ->
    undefined.
