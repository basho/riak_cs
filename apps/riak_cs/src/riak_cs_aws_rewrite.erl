%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2023 TI Tokyo    All Rights Reserved.
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

-module(riak_cs_aws_rewrite).
-behaviour(riak_cs_rewrite).

-export([rewrite/5,
         original_resource/1,
         raw_url/1
        ]).

-include("riak_cs.hrl").
-include("riak_cs_web.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("kernel/include/logger.hrl").


-spec rewrite(atom(), atom(), {integer(), integer()}, mochiweb_headers(), string()) ->
          {mochiweb_headers(), string()}.
rewrite(Method, Scheme, Vsn, Headers, Url) ->
    Host = mochiweb_headers:get_value("host", Headers),
    case service_from_host(Host) of
        no_rewrite ->
            logger:debug("not rewriting direct request for us: ~s", [Host]),
            riak_cs_rewrite:rewrite(Method, Scheme, Vsn, Headers, Url);
        {unsupported, A} ->
            logger:warning("Service ~s is not supported", [A]),
            {Headers, Url};
        Mod ->
            logger:debug("rewriting url for service ~s", [Mod]),
            Mod:rewrite(Method, Scheme, Vsn, Headers, Url)
    end.

service_from_host(Host) ->
    {AttempRewrite, Third, Fourth} =
        case lists:reverse(
               string:split(string:to_lower(Host), ".", all)) of
            ["com", "amazonaws", A] ->
                {true, A, ""};
            ["com", "amazonaws", A, B|_] ->
                {true, A, B};
            _ ->
                {false, "", ""}
        end,
    case AttempRewrite of
        true ->
            case aws_service_submodule(Third) of
                {unsupported, _} ->
                    %% third item is a region, then fourth must be it
                    aws_service_submodule(Fourth);
                Service ->
                    Service
            end;
        false ->
            no_rewrite
    end.

aws_service_submodule("s3") -> riak_cs_aws_s3_rewrite;
aws_service_submodule("iam") -> riak_cs_aws_iam_rewrite;
aws_service_submodule(A) ->  {unsupported, A}.


-spec original_resource(#wm_reqdata{}) -> undefined | {string(), [{term(),term()}]}.
original_resource(RD) ->
    riak_cs_rewrite:original_resource(RD).

-spec raw_url(#wm_reqdata{}) -> undefined | {string(), [{term(), term()}]}.
raw_url(RD) ->
    riak_cs_rewrite:raw_url(RD).
