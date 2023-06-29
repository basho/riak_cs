%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%               2021, 2022 TI Tokyo    All Rights Reserved.
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

-module(stanchion_response).

-export([api_error/3,
         list_buckets_response/3]).

-include("riak_cs.hrl").
-include("stanchion.hrl").
-include_lib("kernel/include/logger.hrl").

-spec api_error(error_reason(), #wm_reqdata{}, #stanchion_context{}) ->
          {{halt, 400..599}, string(), #stanchion_context{}}.
api_error(Error, RD, Ctx) ->
    StatusCode = riak_cs_aws_response:status_code(Error),
    ErrorDesc = jsx:encode(#{error_tag => base64:encode(term_to_binary(Error)),
                             resource => list_to_binary(wrq:path(RD))}),
    {{halt, StatusCode}, wrq:set_resp_body(ErrorDesc, RD), Ctx}.


list_buckets_response(BucketData, RD, Ctx) ->
    BucketsDoc = [{'Bucket',
                   [{'Name', [binary_to_list(Bucket)]},
                    {'Owner', [binary_to_list(Owner)]}]}
                  || {Bucket, Owner} <- BucketData],
    Contents = [{'Buckets', BucketsDoc}],
    XmlDoc = [{'ListBucketsResult',  Contents}],
    riak_cs_aws_response:respond(200, riak_cs_xml:to_xml(XmlDoc), RD, Ctx).

