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

-module(riak_cs_wm_bucket).

-export([content_types_provided/2,
         to_xml/2,
         allowed_methods/0,
         content_types_accepted/2,
         accept_body/2,
         delete_resource/2,
         authorize/2]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").

%% @doc Get the list of methods this resource supports.
-spec allowed_methods() -> [atom()].
allowed_methods() ->
    ['HEAD', 'PUT', 'DELETE'].

-spec content_types_provided(#wm_reqdata{}, #context{}) -> {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_provided(RD, Ctx) ->
    {[{"application/xml", to_xml}], RD, Ctx}.

-spec content_types_accepted(#wm_reqdata{}, #context{}) ->
                                    {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_accepted(RD, Ctx) ->
    content_types_accepted(wrq:get_req_header("content-type", RD), RD, Ctx).

-spec content_types_accepted(undefined | string(), #wm_reqdata{}, #context{}) ->
                                    {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_accepted(CT, RD, Ctx) when CT =:= undefined;
                                         CT =:= [] ->
    content_types_accepted("application/octet-stream", RD, Ctx);
content_types_accepted(CT, RD, Ctx) ->
    {Media, _Params} = mochiweb_util:parse_header(CT),
    {[{Media, accept_body}], RD, Ctx}.

-spec authorize(#wm_reqdata{}, #context{}) -> {boolean(), #wm_reqdata{}, #context{}}.
authorize(RD, #context{user=User}=Ctx) ->
    Method = wrq:method(RD),
    RequestedAccess =
        riak_cs_acl_utils:requested_access(Method, false),
    Bucket = list_to_binary(wrq:path_info(bucket, RD)),
    PermCtx = Ctx#context{bucket=Bucket,
                          requested_perm=RequestedAccess},

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


-spec to_xml(#wm_reqdata{}, #context{}) ->
                    {binary() | {'halt', term()}, #wm_reqdata{}, #context{}}.
to_xml(RD, Ctx) ->
    handle_read_request(RD, Ctx).

%% @private
handle_read_request(RD, Ctx=#context{user=User,
                                     bucket=Bucket}) ->
    riak_cs_dtrace:dt_bucket_entry(?MODULE, <<"bucket_head">>,
                                      [], [riak_cs_wm_utils:extract_name(User), Bucket]),
    %% override the content-type on HEAD
    HeadRD = wrq:set_resp_header("content-type", "text/html", RD),
    StrBucket = binary_to_list(Bucket),
    case [B || B <- riak_cs_utils:get_buckets(User),
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
-spec accept_body(#wm_reqdata{}, #context{}) -> {{halt, integer()}, #wm_reqdata{}, #context{}}.
accept_body(RD, Ctx=#context{user=User,
                             user_object=UserObj,
                             bucket=Bucket,
                             response_module=ResponseMod,
                             riakc_pid=RiakPid}) ->
    riak_cs_dtrace:dt_bucket_entry(?MODULE, <<"bucket_create">>,
                                      [], [riak_cs_wm_utils:extract_name(User), Bucket]),
    %% Check for `x-amz-acl' header to support
    %% non-default ACL at bucket creation time.
    ACL = riak_cs_acl_utils:canned_acl(
            wrq:get_req_header("x-amz-acl", RD),
            {User?RCS_USER.display_name,
             User?RCS_USER.canonical_id,
             User?RCS_USER.key_id},
            undefined),
    case riak_cs_utils:create_bucket(User,
                                       UserObj,
                                       Bucket,
                                       ACL,
                                       RiakPid) of
        ok ->
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_create">>,
                                               [200], [riak_cs_wm_utils:extract_name(User), Bucket]),
            {{halt, 200}, RD, Ctx};
        {error, Reason} ->
            Code = ResponseMod:status_code(Reason),
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_create">>,
                                              [Code], [riak_cs_wm_utils:extract_name(User), Bucket]),
            ResponseMod:api_error(Reason, RD, Ctx)
    end.

%% @doc Callback for deleting a bucket.
-spec delete_resource(#wm_reqdata{}, #context{}) ->
                             {boolean() | {'halt', term()}, #wm_reqdata{}, #context{}}.
delete_resource(RD, Ctx=#context{user=User,
                                 user_object=UserObj,
                                 response_module=ResponseMod,
                                 bucket=Bucket,
                                 riakc_pid=RiakPid}) ->
    riak_cs_dtrace:dt_bucket_entry(?MODULE, <<"bucket_delete">>,
                                      [], [riak_cs_wm_utils:extract_name(User), Bucket]),
    case riak_cs_utils:delete_bucket(User,
                                       UserObj,
                                       Bucket,
                                       RiakPid) of
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
