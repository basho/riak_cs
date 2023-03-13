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

-module(riak_cs_wm_bucket).

-export([accept_body/2,
         allowed_methods/0,
         authorize/2,
         content_types_accepted/2,
         content_types_provided/2,
         delete_resource/2,
         malformed_request/2,
         stats_prefix/0,
         to_xml/2
        ]).

-ignore_xref([accept_body/2,
              allowed_methods/0,
              authorize/2,
              content_types_accepted/2,
              content_types_provided/2,
              delete_resource/2,
              malformed_request/2,
              stats_prefix/0,
              to_xml/2
             ]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("kernel/include/logger.hrl").

-spec stats_prefix() -> bucket.
stats_prefix() -> bucket.

%% @doc Get the list of methods this resource supports.
-spec allowed_methods() -> [atom()].
allowed_methods() ->
    ['HEAD', 'PUT', 'DELETE'].

-spec malformed_request(#wm_reqdata{}, #rcs_s3_context{}) -> {false, #wm_reqdata{}, #rcs_s3_context{}}.
malformed_request(RD, Ctx) ->
    case riak_cs_wm_utils:has_canned_acl_and_header_grant(RD) of
        true ->
            riak_cs_s3_response:api_error(canned_acl_and_header_grant,
                                          RD, Ctx);
        false ->
            {false, RD, Ctx}
    end.

-spec content_types_provided(#wm_reqdata{}, #rcs_s3_context{}) ->
          {[{string(), atom()}], #wm_reqdata{}, #rcs_s3_context{}}.
content_types_provided(RD, Ctx) ->
    {[{"application/xml", to_xml}], RD, Ctx}.

-spec content_types_accepted(#wm_reqdata{}, #rcs_s3_context{}) ->
          {[{string(), atom()}], #wm_reqdata{}, #rcs_s3_context{}}.
content_types_accepted(RD, Ctx) ->
    content_types_accepted(wrq:get_req_header("content-type", RD), RD, Ctx).

-spec content_types_accepted(undefined | string(), #wm_reqdata{}, #rcs_s3_context{}) ->
          {[{string(), atom()}], #wm_reqdata{}, #rcs_s3_context{}}.
content_types_accepted(CT, RD, Ctx) when CT =:= undefined;
                                         CT =:= [] ->
    content_types_accepted("application/octet-stream", RD, Ctx);
content_types_accepted(CT, RD, Ctx) ->
    {Media, _Params} = mochiweb_util:parse_header(CT),
    {[{Media, add_acl_to_context_then_accept}], RD, Ctx}.

-spec authorize(#wm_reqdata{}, #rcs_s3_context{}) -> {boolean(), #wm_reqdata{}, #rcs_s3_context{}}.
authorize(RD, #rcs_s3_context{user=User}=Ctx) ->
    Method = wrq:method(RD),
    RequestedAccess =
        riak_cs_acl_utils:requested_access(Method, false),
    Bucket = list_to_binary(wrq:path_info(bucket, RD)),
    PermCtx = Ctx#rcs_s3_context{bucket = Bucket,
                                 requested_perm = RequestedAccess},

    case {Method, RequestedAccess} of
        {_, 'WRITE'} when User == undefined ->
            %% unauthed users may neither create nor delete buckets
            riak_cs_wm_utils:deny_access(RD, PermCtx);
        {'PUT', 'WRITE'} ->
            %% authed users are always allowed to attempt bucket creation
            AccessRD = riak_cs_access_log_handler:set_user(User, RD),
            {false, AccessRD, PermCtx};
        _ ->
            riak_cs_wm_utils:bucket_access_authorize_helper(bucket, true, RD, Ctx)
    end.


-spec to_xml(#wm_reqdata{}, #rcs_s3_context{}) ->
          {binary() | {'halt', term()}, #wm_reqdata{}, #rcs_s3_context{}}.
to_xml(RD, Ctx) ->
    handle_read_request(RD, Ctx).

%% @private
handle_read_request(RD, Ctx=#rcs_s3_context{user=User,
                                            bucket=Bucket}) ->
    riak_cs_dtrace:dt_bucket_entry(?MODULE, <<"bucket_head">>,
                                      [], [riak_cs_wm_utils:extract_name(User), Bucket]),
    %% override the content-type on HEAD
    HeadRD = wrq:set_resp_header("content-type", "text/html", RD),
    StrBucket = binary_to_list(Bucket),
    case [B || B <- riak_cs_bucket:get_buckets(User),
               B?RCS_BUCKET.name =:= StrBucket] of
        [] ->
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_head">>,
                                               [404], [riak_cs_wm_utils:extract_name(User), Bucket]),
            {{halt, 404}, HeadRD, Ctx};
        [_BucketRecord] ->
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_head">>,
                                               [200], [riak_cs_wm_utils:extract_name(User), Bucket]),
            {{halt, 200}, HeadRD, Ctx}
    end.

%% @doc Process request body on `PUT' request.
-spec accept_body(#wm_reqdata{}, #rcs_s3_context{}) -> {{halt, integer()}, #wm_reqdata{}, #rcs_s3_context{}}.
accept_body(RD, Ctx=#rcs_s3_context{user=User,
                                    acl=ACL,
                                    user_object=UserObj,
                                    bucket=Bucket,
                                    response_module=ResponseMod,
                                    riak_client=RcPid}) ->
    riak_cs_dtrace:dt_bucket_entry(?MODULE, <<"bucket_put">>,
                                   [], [riak_cs_wm_utils:extract_name(User), Bucket]),
    BagId = riak_cs_mb_helper:choose_bag_id(manifest, Bucket),
    case riak_cs_bucket:create_bucket(User,
                                      UserObj,
                                      Bucket,
                                      BagId,
                                      ACL,
                                      RcPid) of
        ok ->
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_put">>,
                                               [200], [riak_cs_wm_utils:extract_name(User), Bucket]),
            {{halt, 200}, RD, Ctx};
        {error, Reason} ->
            Code = ResponseMod:status_code(Reason),
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_put">>,
                                              [Code], [riak_cs_wm_utils:extract_name(User), Bucket]),
            ResponseMod:api_error(Reason, RD, Ctx)
    end.

%% @doc Callback for deleting a bucket.
-spec delete_resource(#wm_reqdata{}, #rcs_s3_context{}) ->
          {boolean() | {'halt', term()}, #wm_reqdata{}, #rcs_s3_context{}}.
delete_resource(RD, Ctx=#rcs_s3_context{user=User,
                                        user_object=UserObj,
                                        response_module=ResponseMod,
                                        bucket=Bucket,
                                        riak_client=RcPid}) ->
    riak_cs_dtrace:dt_bucket_entry(?MODULE, <<"bucket_delete">>,
                                      [], [riak_cs_wm_utils:extract_name(User), Bucket]),
    case riak_cs_bucket:delete_bucket(User,
                                      UserObj,
                                      Bucket,
                                      RcPid) of
        ok ->
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_delete">>,
                                               [200], [riak_cs_wm_utils:extract_name(User), Bucket]),
            {true, RD, Ctx};
        {error, Reason} ->
            Code = ResponseMod:status_code(Reason),
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_delete">>,
                                               [Code], [riak_cs_wm_utils:extract_name(User), Bucket]),
            ResponseMod:api_error(Reason, RD, Ctx)
    end.
