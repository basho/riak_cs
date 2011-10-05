%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(riak_moss_request).

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





