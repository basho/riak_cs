%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_request).

-compile(export_all).

-record(moss_request, {
          operation :: put|get|delete|list,
          type :: service|bucket|key|user,
          target :: binary()|{binary(),binary()},
          headers,
          data :: binary()
         }).

new() ->
    #moss_request{}.

new(Op, Type, Target, Headers, Data) ->
    #moss_request{operation=Op,
                  type=Type,
                  target=Target,
                  headers=Headers,
                  data=Data}.

operation(#moss_request{operation=Op}) -> Op.
type(#moss_request{type=Type}) -> Type.
target(#moss_request{target=Target}) -> Target.
headers(#moss_request{headers=Headers}) -> Headers.
data(#moss_request{data=Data}) -> Data.
