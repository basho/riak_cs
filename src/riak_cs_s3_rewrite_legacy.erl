%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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
%% ---------------------------------------------------------------------

-module(riak_cs_s3_rewrite_legacy).

-export([rewrite/5, original_resource/1]).

%% @doc Function to rewrite headers prior to processing by webmachine.
-spec rewrite(atom(), atom(), {integer(), integer()}, gb_tree(), string()) ->
                     {gb_tree(), string()}.
rewrite(Method, Scheme, Vsn, Headers, Url) ->
    %% Unquote the path to accomodate some naughty client libs (looking
    %% at you Fog)
    {Path, QueryString, Fragment} = mochiweb_util:urlsplit_path(Url),
    UpdatedUrl = mochiweb_util:urlunsplit_path({mochiweb_util:unquote(Path), QueryString, Fragment}),
    riak_cs_s3_rewrite:rewrite(Method, Scheme, Vsn, Headers, UpdatedUrl).


-spec original_resource(term()) -> undefined | {string(), [{term(),term()}]}.
original_resource(RD) ->
    riak_cs_s3_rewrite:original_resource(RD).
