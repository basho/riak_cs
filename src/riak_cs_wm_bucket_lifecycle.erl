%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_cs_wm_bucket_lifecycle).

-export([content_types_provided/2,
         to_xml/2,
         allowed_methods/0,
         content_types_accepted/2,
         accept_body/2,
         delete_resource/2]).

-export([authorize/2]).

%% TODO: DELETE?

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").


%% @doc Get the list of methods this resource supports.
-spec allowed_methods() -> [atom()].
allowed_methods() ->
    ['GET', 'PUT', 'DELETE'].

-spec content_types_provided(#wm_reqdata{}, #context{}) -> 
                                    {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_provided(RD, Ctx) ->
    {[{"application/xml", to_xml}], RD, Ctx}.

-spec content_types_accepted(#wm_reqdata{}, #context{}) ->
                                    {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_accepted(RD, Ctx) ->
    case wrq:get_req_header("content-type", RD) of
        undefined ->
            {[{"application/xml", accept_body}], RD, Ctx};
        "application/xml" ->
            {[{"application/xml", accept_body}], RD, Ctx};
        _ ->
            {false, RD, Ctx}
    end.

-spec authorize(#wm_reqdata{}, #context{}) -> {boolean() | {halt, non_neg_integer()}, #wm_reqdata{}, #context{}}.
authorize(RD, Ctx) ->
    riak_cs_wm_utils:bucket_access_authorize_helper(bucket_policy, true, RD, Ctx).


-spec to_xml(#wm_reqdata{}, #context{}) ->
                    {binary() | {'halt', non_neg_integer()}, #wm_reqdata{}, #context{}}.
to_xml(RD, Ctx=#context{start_time=_StartTime,
                         user=User,
                         bucket=Bucket,
                         riak_client=RcPid}) ->

    case riak_cs_s3_lifecycle:fetch_bucket_lifecycle(Bucket, RcPid) of
        {ok, LifecycleXML} ->
            {LifecycleXML, RD, Ctx};
        {error, lifecycle_undefined} ->
            % S3 error: 404 (NoSuchBucketLifecycle): The bucket lifecycle does not exist
            riak_cs_s3_response:api_error(no_such_bucket_lifecycle, RD, Ctx);
        {error, Reason} ->
            riak_cs_s3_response:api_error(Reason, RD, Ctx)
    end.

%% @doc Process request body on `PUT' request.
-spec accept_body(#wm_reqdata{}, #context{}) -> {{halt, non_neg_integer()}, #wm_reqdata{}, #context{}}.
accept_body(RD, Ctx=#context{user=User,
                             user_object=UserObj,
                             bucket=Bucket,
                             riak_client=RcPid}) ->

    XMLString = wrq:req_body(RD),
    case riak_cs_s3_lifecycle:from_xml(XMLString) of
        {ok, Lifecycle} ->
            case riak_cs_bucket:set_bucket_lifecycle(User, UserObj, Bucket, Lifecycle, RcPid) of
                ok ->
                    {{halt, 200}, RD, Ctx};
                {error, Reason} ->
                    riak_cs_s3_response:api_error(Reason, RD, Ctx)
            end;
        {error, Reason} -> 
            riak_cs_s3_response:api_error(Reason, RD, Ctx)
    end.

%% @doc Callback for deleting lifecycle.
-spec delete_resource(#wm_reqdata{}, #context{}) -> {true, #wm_reqdata{}, #context{}} |
                                                    {{halt, 200}, #wm_reqdata{}, #context{}}.
delete_resource(RD, Ctx=#context{user=User,
                                 user_object=UserObj,
                                 bucket=Bucket,
                                 riak_client=RcPid}) ->

    case riak_cs_bucket:delete_bucket_lifecycle(User, UserObj, Bucket, RcPid) of
        ok ->
            {{halt, 200}, RD, Ctx};
        {error, Reason} ->
            riak_cs_s3_response:api_error(Reason, RD, Ctx)
    end.
