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

-module(riak_cs_wm_object_upload).

-export([init/1,
         authorize/2,
         content_types_provided/2,
         allowed_methods/0,
         malformed_request/2,
         content_types_accepted/2,
         post_is_create/2,
         process_post/2,
         multiple_choices/2,
         valid_entity_length/2,
         finish_request/2]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-spec init(#context{}) -> {ok, #context{}}.
init(Ctx) ->
    {ok, Ctx#context{local_context=#key_context{}}}.

-spec malformed_request(#wm_reqdata{}, #context{}) -> {false, #wm_reqdata{}, #context{}}.
malformed_request(RD,Ctx=#context{local_context=LocalCtx0}) ->
    Bucket = list_to_binary(wrq:path_info(bucket, RD)),
    %% need to unquote twice since we re-urlencode the string during rewrite in
    %% order to trick webmachine dispatching
    %% NOTE: Bucket::binary(), *but* Key::string()
    Key = mochiweb_util:unquote(mochiweb_util:unquote(wrq:path_info(object, RD))),
    LocalCtx = LocalCtx0#key_context{bucket=Bucket, key=Key},
    {false, RD, Ctx#context{local_context=LocalCtx}}.

%% @doc Get the type of access requested and the manifest with the
%% object ACL and compare the permission requested with the permission
%% granted, and allow or deny access. Returns a result suitable for
%% directly returning from the {@link forbidden/2} webmachine export.
-spec authorize(#wm_reqdata{}, #context{}) ->
                       {boolean() | {halt, term()}, #wm_reqdata{}, #context{}}.
authorize(RD, Ctx0=#context{local_context=LocalCtx0, riakc_pid=RiakPid}) ->
    Method = wrq:method(RD),
    RequestedAccess =
        riak_cs_acl_utils:requested_access(Method, false),
    LocalCtx = riak_cs_wm_utils:ensure_doc(LocalCtx0, RiakPid),
    Ctx = Ctx0#context{requested_perm=RequestedAccess,local_context=LocalCtx},

    riak_cs_wm_utils:object_access_authorize_helper(object_part, false, RD, Ctx).


%% @doc Get the list of methods this resource supports.
-spec allowed_methods() -> [atom()].
allowed_methods() ->
    ['POST'].

post_is_create(RD, Ctx) ->
    {false, RD, Ctx}.

process_post(RD, Ctx=#context{local_context=LocalCtx,
                              riakc_pid=RiakcPid}) ->
    #key_context{bucket=Bucket, key=Key} = LocalCtx,
    ContentType = try
                      list_to_binary(wrq:get_req_header("Content-Type", RD))
                  catch error:badarg ->
                          %% Per http://docs.amazonwebservices.com/AmazonS3/latest/API/mpUploadInitiate.html
                          <<"binary/octet-stream">>
                  end,
    User = riak_cs_mp_utils:user_rec_to_3tuple(Ctx#context.user),
    ACL = riak_cs_acl_utils:canned_acl(
            wrq:get_req_header("x-amz-acl", RD),
            User,
            riak_cs_wm_utils:bucket_owner(Bucket, RiakcPid)),
    Metadata = riak_cs_wm_utils:extract_user_metadata(RD),
    Opts = [{acl, ACL}, {meta_data, Metadata}],

    case riak_cs_mp_utils:initiate_multipart_upload(Bucket, list_to_binary(Key),
                                                    ContentType, User, Opts,
                                                    RiakcPid) of
        {ok, UploadId} ->
            XmlDoc = {'InitiateMultipartUploadResult',
                       [{'xmlns', "http://s3.amazonaws.com/doc/2006-03-01/"}],
                       [
                        {'Bucket', [binary_to_list(Bucket)]},
                        {'Key', [Key]},
                        {'UploadId', [binary_to_list(base64url:encode(UploadId))]}
                       ]
                     },
            Body = riak_cs_xml:export_xml([XmlDoc]),
            RD2 = wrq:set_resp_body(Body, RD),
            {true, RD2, Ctx};
        {error, Reason} ->
            riak_cs_s3_response:api_error(Reason, RD, Ctx)
    end.

multiple_choices(RD, Ctx) ->
    {false, RD, Ctx}.

-spec valid_entity_length(#wm_reqdata{}, #context{}) -> {boolean(), #wm_reqdata{}, #context{}}.
valid_entity_length(RD, Ctx=#context{local_context=LocalCtx}) ->
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
                            {true, RD, Ctx#context{local_context=UpdLocalCtx}}
                    end;
                _ ->
                    {false, RD, Ctx}
            end;
        _ ->
            {true, RD, Ctx}
    end.

finish_request(RD, Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"finish_request">>, [0], []),
    {true, RD, Ctx}.

-spec content_types_provided(#wm_reqdata{}, #context{}) -> {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_provided(RD, Ctx=#context{}) ->
    Method = wrq:method(RD),
    if Method == 'POST' ->
            {[{?XML_TYPE, unused_callback}], RD, Ctx};
       true ->
            %% this shouldn't ever be called, it's just to
            %% appease webmachine
            {[{"text/plain", unused_callback}], RD, Ctx}
    end.

-spec content_types_accepted(#wm_reqdata{}, #context{}) -> {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_accepted(RD, Ctx) ->
    riak_cs_mp_utils:make_content_types_accepted(RD, Ctx).
