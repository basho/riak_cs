%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
%%               2021-2023 TI Tokyo    All Rights Reserved.
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

-module(riak_cs_wm_object_upload).

-export([init/1,
         stats_prefix/0,
         authorize/2,
         content_types_provided/2,
         allowed_methods/0,
         malformed_request/2,
         content_types_accepted/2,
         post_is_create/2,
         process_post/2,
         valid_entity_length/2
        ]).

-ignore_xref([init/1,
              stats_prefix/0,
              authorize/2,
              content_types_provided/2,
              allowed_methods/0,
              malformed_request/2,
              content_types_accepted/2,
              post_is_create/2,
              process_post/2,
              valid_entity_length/2
             ]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-spec init(#rcs_s3_context{}) -> {ok, #rcs_s3_context{}}.
init(Ctx) ->
    {ok, Ctx#rcs_s3_context{local_context=#key_context{}}}.

-spec stats_prefix() -> multipart.
stats_prefix() -> multipart.

-spec malformed_request(#wm_reqdata{}, #rcs_s3_context{}) -> {false, #wm_reqdata{}, #rcs_s3_context{}}.
malformed_request(RD, #rcs_s3_context{response_module=ResponseMod} = Ctx) ->
    case riak_cs_wm_utils:extract_key(RD, Ctx) of
        {error, Reason} ->
            ResponseMod:api_error(Reason, RD, Ctx);
        {ok, ContextWithKey} ->
            case riak_cs_wm_utils:has_canned_acl_and_header_grant(RD) of
                true ->
                    ResponseMod:api_error(canned_acl_and_header_grant,
                                          RD, ContextWithKey);
                false ->
                    {false, RD, ContextWithKey}
            end
    end.

%% @doc Get the type of access requested and the manifest with the
%% object ACL and compare the permission requested with the permission
%% granted, and allow or deny access. Returns a result suitable for
%% directly returning from the {@link forbidden/2} webmachine export.
-spec authorize(#wm_reqdata{}, #rcs_s3_context{}) ->
          {boolean() | {halt, term()}, #wm_reqdata{}, #rcs_s3_context{}}.
authorize(RD, Ctx0=#rcs_s3_context{local_context=LocalCtx0, riak_client=RcPid}) ->
    Method = wrq:method(RD),
    RequestedAccess =
        riak_cs_acl_utils:requested_access(Method, false),
    LocalCtx = riak_cs_wm_utils:ensure_doc(LocalCtx0, RcPid),
    Ctx = Ctx0#rcs_s3_context{requested_perm=RequestedAccess,local_context=LocalCtx},
    authorize(RD, Ctx, LocalCtx#key_context.bucket_object).

authorize(RD, Ctx, notfound = _BucketObj) ->
    riak_cs_wm_utils:respond_api_error(RD, Ctx, no_such_bucket);
authorize(RD, Ctx, _BucketObj) ->
    riak_cs_wm_utils:object_access_authorize_helper(object_part, false, RD, Ctx).

%% @doc Get the list of methods this resource supports.
-spec allowed_methods() -> [atom()].
allowed_methods() ->
    ['POST'].

post_is_create(RD, Ctx) ->
    {false, RD, Ctx}.

process_post(RD, Ctx) ->
    case riak_cs_wm_utils:maybe_update_context_with_acl_from_headers(RD, Ctx) of
        {ok, ContextWithAcl} ->
            process_post_helper(RD, ContextWithAcl);
        {error, HaltResponse} ->
            HaltResponse
    end.

process_post_helper(RD, Ctx = #rcs_s3_context{riak_client = RcPid,
                                              local_context = #key_context{bucket = Bucket,
                                                                           key = Key,
                                                                           obj_vsn = ObjVsn},
                                              acl = ACL}) ->
    ContentType = try
                      list_to_binary(wrq:get_req_header("Content-Type", RD))
                  catch error:badarg ->
                          %% Per http://docs.amazonwebservices.com/AmazonS3/latest/API/mpUploadInitiate.html
                          <<"binary/octet-stream">>
                  end,
    User = riak_cs_user:to_3tuple(Ctx#rcs_s3_context.user),
    Metadata = riak_cs_wm_utils:extract_user_metadata(RD),
    Opts = [{acl, ACL}, {meta_data, Metadata}],

    case riak_cs_mp_utils:initiate_multipart_upload(
           Bucket, Key, ObjVsn,
           ContentType, User, Opts,
           RcPid) of
        {ok, UploadId} ->
            XmlDoc = {'InitiateMultipartUploadResult',
                       [{'xmlns', "http://s3.amazonaws.com/doc/2006-03-01/"}],
                       [
                        {'Bucket', [Bucket]},
                        {'Key', [Key]},
                        {'UploadId', [base64url:encode(UploadId)]}
                       ]
                     },
            Body = riak_cs_xml:to_xml([XmlDoc]),
            RD2 = wrq:set_resp_body(Body, RD),
            {true, RD2, Ctx};
        {error, Reason} ->
            riak_cs_s3_response:api_error(Reason, RD, Ctx)
    end.

-spec valid_entity_length(#wm_reqdata{}, #rcs_s3_context{}) -> {boolean(), #wm_reqdata{}, #rcs_s3_context{}}.
valid_entity_length(RD, Ctx=#rcs_s3_context{local_context=LocalCtx}) ->
    case wrq:method(RD) of
        'PUT' ->
            case catch(
                   list_to_integer(
                     wrq:get_req_header("Content-Length", RD))) of
                Length when is_integer(Length) ->
                    case Length =< riak_cs_lfs_utils:max_content_len() of
                        false ->
                            riak_cs_s3_response:api_error(
                              entity_too_large, RD, Ctx);
                        true ->
                            UpdLocalCtx = LocalCtx#key_context{size=Length},
                            {true, RD, Ctx#rcs_s3_context{local_context=UpdLocalCtx}}
                    end;
                _ ->
                    {false, RD, Ctx}
            end;
        _ ->
            {true, RD, Ctx}
    end.

-spec content_types_provided(#wm_reqdata{}, #rcs_s3_context{}) -> {[{string(), atom()}], #wm_reqdata{}, #rcs_s3_context{}}.
content_types_provided(RD, Ctx=#rcs_s3_context{}) ->
    Method = wrq:method(RD),
    if Method == 'POST' ->
            {[{?XML_TYPE, unused_callback}], RD, Ctx};
       true ->
            %% this shouldn't ever be called, it's just to
            %% appease webmachine
            {[{"text/plain", unused_callback}], RD, Ctx}
    end.

-spec content_types_accepted(#wm_reqdata{}, #rcs_s3_context{}) -> {[{string(), atom()}], #wm_reqdata{}, #rcs_s3_context{}}.
content_types_accepted(RD, Ctx) ->
    riak_cs_mp_utils:make_content_types_accepted(RD, Ctx).
