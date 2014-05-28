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

-module(riak_cs_wm_bucket_policy).

-export([content_types_provided/2,
         to_json/2,
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
    {[{"application/json", to_json}], RD, Ctx}.

-spec content_types_accepted(#wm_reqdata{}, #context{}) ->
                                    {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_accepted(RD, Ctx) ->
    case wrq:get_req_header("content-type", RD) of
        undefined ->
            {[{"application/json", accept_body}], RD, Ctx};
        "application/json" ->
            {[{"application/json", accept_body}], RD, Ctx};
        _ ->
            {false, RD, Ctx}
    end.

-spec authorize(#wm_reqdata{}, #context{}) -> {boolean() | {halt, non_neg_integer()}, #wm_reqdata{}, #context{}}.
authorize(RD, Ctx) ->
    riak_cs_wm_utils:bucket_access_authorize_helper(bucket_policy, true, RD, Ctx).


-spec to_json(#wm_reqdata{}, #context{}) ->
                    {binary() | {'halt', non_neg_integer()}, #wm_reqdata{}, #context{}}.
to_json(RD, Ctx=#context{start_time=_StartTime,
                         user=User,
                         bucket=Bucket,
                         riak_client=RcPid}) ->
    riak_cs_dtrace:dt_bucket_entry(?MODULE, <<"bucket_get_policy">>,
                                      [], [riak_cs_wm_utils:extract_name(User), Bucket]),

    case riak_cs_s3_policy:fetch_bucket_policy(Bucket, RcPid) of
        {ok, PolicyJson} ->
            {PolicyJson, RD, Ctx};
        {error, policy_undefined} ->
            % S3 error: 404 (NoSuchBucketPolicy): The bucket policy does not exist
            riak_cs_s3_response:api_error(no_such_bucket_policy, RD, Ctx);
        {error, Reason} ->
            Code = riak_cs_s3_response:status_code(Reason),
            X = riak_cs_s3_response:api_error(Reason, RD, Ctx),
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_get_policy">>,
                                               [Code], [riak_cs_wm_utils:extract_name(User), Bucket]),
            X
    end.

%% @doc Process request body on `PUT' request.
-spec accept_body(#wm_reqdata{}, #context{}) -> {{halt, non_neg_integer()}, #wm_reqdata{}, #context{}}.
accept_body(RD, Ctx=#context{user=User,
                             user_object=UserObj,
                             bucket=Bucket,
                             policy_module=PolicyMod,
                             riak_client=RcPid}) ->
    riak_cs_dtrace:dt_bucket_entry(?MODULE, <<"bucket_put_policy">>,
                                   [], [riak_cs_wm_utils:extract_name(User), Bucket]),

    PolicyJson = wrq:req_body(RD),
    case PolicyMod:policy_from_json(PolicyJson) of
        {ok, Policy} ->
            Access = PolicyMod:reqdata_to_access(RD, bucket_policy, User#rcs_user_v2.canonical_id),
            case PolicyMod:check_policy(Access, Policy) of
                ok ->
                    case riak_cs_bucket:set_bucket_policy(User, UserObj, Bucket, PolicyJson, RcPid) of
                        ok ->
                            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_put_policy">>,
                                                            [200], [riak_cs_wm_utils:extract_name(User), Bucket]),
                            {{halt, 200}, RD, Ctx};
                        {error, Reason} ->
                            Code = riak_cs_s3_response:status_code(Reason),
                            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_put_policy">>,
                                                            [Code], [riak_cs_wm_utils:extract_name(User), Bucket]),
                            riak_cs_s3_response:api_error(Reason, RD, Ctx)
                    end;
                {error, Reason} -> %% good JSON, but bad as IAM policy
                    riak_cs_s3_response:api_error(Reason, RD, Ctx)
            end;
        {error, Reason} -> %% Broken as JSON
            riak_cs_s3_response:api_error(Reason, RD, Ctx)
    end.

%% @doc Callback for deleting policy.
-spec delete_resource(#wm_reqdata{}, #context{}) -> {true, #wm_reqdata{}, #context{}} |
                                                    {{halt, 200}, #wm_reqdata{}, #context{}}.
delete_resource(RD, Ctx=#context{user=User,
                                 user_object=UserObj,
                                 bucket=Bucket,
                                 riak_client=RcPid}) ->
    riak_cs_dtrace:dt_object_entry(?MODULE, <<"bucket_policy_delete">>,
                                   [], [RD, Ctx, RcPid]),

    case riak_cs_bucket:delete_bucket_policy(User, UserObj, Bucket, RcPid) of
        ok ->
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_put_policy">>,
                                            [200], [riak_cs_wm_utils:extract_name(User), Bucket]),
            {{halt, 200}, RD, Ctx};
        {error, Reason} ->
            Code = riak_cs_s3_response:status_code(Reason),
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_put_policy">>,
                                            [Code], [riak_cs_wm_utils:extract_name(User), Bucket]),
            riak_cs_s3_response:api_error(Reason, RD, Ctx)
    end.
