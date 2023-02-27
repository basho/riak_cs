%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
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

-module(riak_cs_wm_bucket_acl).

-export([stats_prefix/0,
         content_types_provided/2,
         to_xml/2,
         allowed_methods/0,
         malformed_request/2,
         content_types_accepted/2,
         accept_body/2
        ]).

-ignore_xref([stats_prefix/0,
              content_types_provided/2,
              to_xml/2,
              allowed_methods/0,
              malformed_request/2,
              content_types_accepted/2,
              accept_body/2
             ]).

-export([authorize/2]).

%% TODO: DELETE?

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").


-spec stats_prefix() -> bucket_acl.
stats_prefix() -> bucket_acl.

%% @doc Get the list of methods this resource supports.
-spec allowed_methods() -> [atom()].
allowed_methods() ->
    ['GET', 'PUT'].

-spec malformed_request(#wm_reqdata{}, #rcs_context{}) -> {false, #wm_reqdata{}, #rcs_context{}}.
malformed_request(RD, Ctx) ->
    case riak_cs_wm_utils:has_acl_header_and_body(RD) of
        true ->
            riak_cs_s3_response:api_error(unexpected_content,
                                          RD, Ctx);
        false ->
            {false, RD, Ctx}
    end.

-spec content_types_provided(#wm_reqdata{}, #rcs_context{}) -> {[{string(), atom()}], #wm_reqdata{}, #rcs_context{}}.
content_types_provided(RD, Ctx) ->
    {[{"application/xml", to_xml}], RD, Ctx}.

-spec content_types_accepted(#wm_reqdata{}, #rcs_context{}) ->
                                    {[{string(), atom()}], #wm_reqdata{}, #rcs_context{}}.
content_types_accepted(RD, Ctx) ->
    case wrq:get_req_header("content-type", RD) of
        undefined ->
            {[{"application/octet-stream", add_acl_to_context_then_accept}], RD, Ctx};
        CType ->
            {Media, _Params} = mochiweb_util:parse_header(CType),
            {[{Media, add_acl_to_context_then_accept}], RD, Ctx}
    end.

-spec authorize(#wm_reqdata{}, #rcs_context{}) -> {boolean() | {halt, non_neg_integer()}, #wm_reqdata{}, #rcs_context{}}.
authorize(RD, Ctx) ->
    riak_cs_wm_utils:bucket_access_authorize_helper(bucket_acl, true, RD, Ctx).


-spec to_xml(#wm_reqdata{}, #rcs_context{}) ->
                    {binary() | {'halt', non_neg_integer()}, #wm_reqdata{}, #rcs_context{}}.
to_xml(RD, Ctx=#rcs_context{user=User,
                            bucket=Bucket,
                            riak_client=RcPid}) ->
    riak_cs_dtrace:dt_bucket_entry(?MODULE, <<"bucket_get_acl">>,
                                      [], [riak_cs_wm_utils:extract_name(User), Bucket]),
    case riak_cs_acl:fetch_bucket_acl(Bucket, RcPid) of
        {ok, Acl} ->
            X = {riak_cs_xml:to_xml(Acl), RD, Ctx},
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_acl_get">>,
                                               [200], [riak_cs_wm_utils:extract_name(User), Bucket]),
            X;
        {error, Reason} ->
            Code = riak_cs_s3_response:status_code(Reason),
            X = riak_cs_s3_response:api_error(Reason, RD, Ctx),
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_acl">>,
                                               [Code], [riak_cs_wm_utils:extract_name(User), Bucket]),
            X
    end.

%% @doc Process request body on `PUT' request.
-spec accept_body(#wm_reqdata{}, #rcs_context{}) -> {{halt, non_neg_integer()}, #wm_reqdata{}, #rcs_context{}}.
accept_body(RD, Ctx=#rcs_context{user=User,
                                 user_object=UserObj,
                                 acl=AclFromHeadersOrDefault,
                                 bucket=Bucket,
                                 riak_client=RcPid}) ->
    riak_cs_dtrace:dt_bucket_entry(?MODULE, <<"bucket_put_acl">>,
                                      [], [riak_cs_wm_utils:extract_name(User), Bucket]),
    Body = binary_to_list(wrq:req_body(RD)),
    AclRes =
        case Body of
            [] ->
                {ok, AclFromHeadersOrDefault};
            _ ->
                riak_cs_acl_utils:validate_acl(
                  riak_cs_acl_utils:acl_from_xml(Body,
                                                 User?RCS_USER.key_id,
                                                 RcPid),
                  User?RCS_USER.canonical_id)
        end,
    case AclRes of
        {ok, ACL} ->
            case riak_cs_bucket:set_bucket_acl(User,
                                               UserObj,
                                               Bucket,
                                               ACL,
                                               RcPid) of
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
