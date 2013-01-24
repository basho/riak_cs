%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_policy).

-export([behaviour_info/1]).

-compile(export_all).

-spec behaviour_info(atom()) -> 'undefined' | [{atom(), arity()}].
behaviour_info(callbacks) ->
    [{eval, 2}, {check_policy, 2}, {reqdata_to_access, 3},
     {policy_from_json, 1}, {policy_to_json_term, 1}];
behaviour_info(_Other) ->
    undefined.
