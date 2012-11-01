%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc

-module(riak_cs_list_objects).

-include("riak_cs.hrl").
-include("list_objects.hrl").

%% API
-export([new_req/1,
         new_req/3]).


%%%===================================================================
%%% API
%%%===================================================================

new_req(Name) ->
    new_req(Name, 1000, []).

new_req(Name, MaxKeys, Options) ->
    process_options(#list_objects_request_v1{name=Name,
                                             max_keys=MaxKeys},
                    Options).

%% @private
process_options(Request, Options) ->
    lists:foldl(fun process_options_helper/2,
                Request,
                Options).

process_options_helper({prefix, Val}, Req) ->
    Req#list_objects_request_v1{prefix=Val};
process_options_helper({delimiter, Val}, Req) ->
    Req#list_objects_request_v1{delimiter=Val};
process_options_helper({marker, Val}, Req) ->
    Req#list_objects_request_v1{marker=Val}.

%% ====================================================================
%% Internal functions
%% ====================================================================
