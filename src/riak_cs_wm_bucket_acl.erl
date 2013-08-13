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

-module(riak_cs_wm_bucket_acl).

-export([content_types_provided/2,
         to_xml/2,
         allowed_methods/0,
         content_types_accepted/2,
         accept_body/2]).

-export([authorize/2]).

%% TODO: DELETE?

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").


%% @doc Get the list of methods this resource supports.
-spec allowed_methods() -> [atom()].
allowed_methods() ->
    ['GET', 'PUT'].

-spec content_types_provided(#wm_reqdata{}, #context{}) -> {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_provided(RD, Ctx) ->
    {[{"application/xml", to_xml}], RD, Ctx}.

-spec content_types_accepted(#wm_reqdata{}, #context{}) ->
                                    {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_accepted(RD, Ctx) ->
    case wrq:get_req_header("content-type", RD) of
        undefined ->
            {[{"application/octet-stream", accept_body}], RD, Ctx};
        CType ->
            {Media, _Params} = mochiweb_util:parse_header(CType),
            {[{Media, accept_body}], RD, Ctx}
    end.

-spec authorize(#wm_reqdata{}, #context{}) -> {boolean() | {halt, non_neg_integer()}, #wm_reqdata{}, #context{}}.
authorize(RD, Ctx) ->
    riak_cs_wm_utils:bucket_access_authorize_helper(bucket_acl, true, RD, Ctx).


-spec to_xml(#wm_reqdata{}, #context{}) ->
                    {binary() | {'halt', non_neg_integer()}, #wm_reqdata{}, #context{}}.
to_xml(RD, Ctx=#context{start_time=StartTime,
                        user=User,
                        bucket=Bucket,
                        riakc_pid=RiakPid}) ->
    riak_cs_dtrace:dt_bucket_entry(?MODULE, <<"bucket_get_acl">>,
                                      [], [riak_cs_wm_utils:extract_name(User), Bucket]),
    case riak_cs_acl:bucket_acl(Bucket, RiakPid) of
        {ok, Acl} ->
            X = {riak_cs_xml:to_xml(Acl), RD, Ctx},
            ok = riak_cs_stats:update_with_start(bucket_get_acl, StartTime),
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_get_acl">>,
                                               [200], [riak_cs_wm_utils:extract_name(User), Bucket]),
            X;
        {error, Reason} ->
            Code = riak_cs_s3_response:status_code(Reason),
            X = riak_cs_s3_response:api_error(Reason, RD, Ctx),
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_get_acl">>,
                                               [Code], [riak_cs_wm_utils:extract_name(User), Bucket]),
            X
    end.

%% @doc Process request body on `PUT' request.
-spec accept_body(#wm_reqdata{}, #context{}) -> {{halt, non_neg_integer()}, #wm_reqdata{}, #context{}}.
accept_body(RD, Ctx=#context{user=User,
                             user_object=UserObj,
                             bucket=Bucket,
                             riakc_pid=RiakPid}) ->
    riak_cs_dtrace:dt_bucket_entry(?MODULE, <<"bucket_put_acl">>,
                                      [], [riak_cs_wm_utils:extract_name(User), Bucket]),
    Body = binary_to_list(wrq:req_body(RD)),
    AclRes =
        case Body of
            [] ->
                %% Check for `x-amz-acl' header to support
                %% the use of a canned ACL.
                CannedAcl = riak_cs_acl_utils:canned_acl(
                              wrq:get_req_header("x-amz-acl", RD),
                              {User?RCS_USER.display_name,
                               User?RCS_USER.canonical_id,
                               User?RCS_USER.key_id},
                              riak_cs_wm_utils:bucket_owner(Bucket, RiakPid)),
                {ok, CannedAcl};
            _ ->
                riak_cs_acl_utils:validate_acl(
                  riak_cs_acl_utils:acl_from_xml(Body,
                                                 User?RCS_USER.key_id,
                                                 RiakPid),
                  User?RCS_USER.canonical_id)
        end,
    case AclRes of
        {ok, ACL} ->
            case riak_cs_utils:set_bucket_acl(User,
                                              UserObj,
                                              Bucket,
                                              ACL,
                                              RiakPid) of
                ok ->
                    riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_put_acl">>,
                                                    [200], [riak_cs_wm_utils:extract_name(User), Bucket]),
                    {{halt, 200}, RD, Ctx};
                {error, Reason} ->
                    Code = riak_cs_s3_response:status_code(Reason),
                    riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_put_acl">>,
                                                    [Code], [riak_cs_wm_utils:extract_name(User), Bucket]),
                    riak_cs_s3_response:api_error(Reason, RD, Ctx)
            end;
        {error, Reason2} ->
            Code = riak_cs_s3_response:status_code(Reason2),
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_put_acl">>,
                                            [Code], [riak_cs_wm_utils:extract_name(User), Bucket]),
            riak_cs_s3_response:api_error(Reason2, RD, Ctx)
    end.
