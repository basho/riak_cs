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

-module(riak_cs_wm_object_acl).

-export([init/1,
         stats_prefix/0,
         allowed_methods/0,
         malformed_request/2,
         authorize/2,
         content_types_accepted/2,
         content_types_provided/2,
         accept_body/2,
         produce_body/2
        ]).

-ignore_xref([init/1,
              stats_prefix/0,
              allowed_methods/0,
              malformed_request/2,
              authorize/2,
              content_types_accepted/2,
              content_types_provided/2,
              accept_body/2,
              produce_body/2
             ]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").

init(Ctx) ->
    {ok, Ctx#rcs_s3_context{local_context=#key_context{}}}.

-spec stats_prefix() -> object_acl.
stats_prefix() -> object_acl.

-spec malformed_request(#wm_reqdata{}, #rcs_s3_context{}) -> {false, #wm_reqdata{}, #rcs_s3_context{}}.
malformed_request(RD, #rcs_s3_context{response_module=ResponseMod} = Ctx) ->
    case riak_cs_wm_utils:has_acl_header_and_body(RD) of
        true ->
            ResponseMod:api_error(unexpected_content, RD, Ctx);
        false ->
            case riak_cs_wm_utils:extract_key(RD, Ctx) of
                {error, Reason} ->
                    ResponseMod:api_error(Reason, RD, Ctx);
                {ok, NewCtx} ->
                    {false, RD, NewCtx}
            end
    end.

%% @doc Get the type of access requested and the manifest with the
%% object ACL and compare the permission requested with the permission
%% granted, and allow or deny access. Returns a result suitable for
%% directly returning from the {@link forbidden/2} webmachine export.
authorize(RD, Ctx0=#rcs_s3_context{local_context=LocalCtx0, riak_client=RcPid}) ->
    Method = wrq:method(RD),
    RequestedAccess =
        %% This is really the only difference between authorize/2 in this module and riak_cs_wm_object
        riak_cs_acl_utils:requested_access(Method, true),
    LocalCtx = riak_cs_wm_utils:ensure_doc(LocalCtx0, RcPid),
    Ctx = Ctx0#rcs_s3_context{requested_perm=RequestedAccess,local_context=LocalCtx},
    authorize(RD, Ctx,
              LocalCtx#key_context.bucket_object,
              Method, LocalCtx#key_context.manifest).

authorize(RD, Ctx, notfound = _BucketObj, _Method, _Manifest) ->
    riak_cs_wm_utils:respond_api_error(RD, Ctx, no_such_bucket);
authorize(RD, Ctx, _BucketObj, 'GET', notfound = _Manifest) ->
    riak_cs_wm_utils:respond_api_error(RD, Ctx, no_such_key);
authorize(RD, Ctx, _BucketObj, 'HEAD', notfound = _Manifest) ->
    riak_cs_wm_utils:respond_api_error(RD, Ctx, no_such_key);
authorize(RD, Ctx, _BucketObj, _Method, _Manifest) ->
    riak_cs_wm_utils:object_access_authorize_helper(object_acl, false, RD, Ctx).

%% @doc Get the list of methods this resource supports.
-spec allowed_methods() -> [atom()].
allowed_methods() ->
    ['GET', 'PUT'].


-spec content_types_provided(#wm_reqdata{}, #rcs_s3_context{}) -> {[{string(), atom()}], #wm_reqdata{}, #rcs_s3_context{}}.
content_types_provided(RD, Ctx=#rcs_s3_context{local_context=LocalCtx,
                                               riak_client=RcPid}) ->
    Mfst = LocalCtx#key_context.manifest,
    %% TODO:
    %% As I understand S3, the content types provided
    %% will either come from the value that was
    %% last PUT or, from you adding a
    %% `response-content-type` header in the request.
    Method = wrq:method(RD),
    if Method == 'GET'; Method == 'HEAD' ->
            UpdLocalCtx = riak_cs_wm_utils:ensure_doc(LocalCtx, RcPid),
            ContentType = binary_to_list(Mfst?MANIFEST.content_type),
            case ContentType of
                _ ->
                    UpdCtx = Ctx#rcs_s3_context{local_context=UpdLocalCtx},
                    {[{ContentType, produce_body}], RD, UpdCtx}
            end;
       true ->
            %% TODO this shouldn't ever be called, it's just to
            %% appease webmachine
            {[{"text/plain", produce_body}], RD, Ctx}
    end.

-spec content_types_accepted(term(), term()) -> {[{string(), atom()}], #wm_reqdata{}, #rcs_s3_context{}}.
content_types_accepted(RD, Ctx=#rcs_s3_context{local_context=LocalCtx0}) ->
    case wrq:get_req_header("Content-Type", RD) of
        undefined ->
            DefaultCType = "application/octet-stream",
            LocalCtx = LocalCtx0#key_context{putctype=DefaultCType},
            {[{DefaultCType, add_acl_to_context_then_accept}],
             RD,
             Ctx#rcs_s3_context{local_context=LocalCtx}};
        %% This was shamelessly ripped out of
        %% https://github.com/basho/riak_kv/blob/0d91ca641a309f2962a216daa0cee869c82ffe26/src/riak_kv_wm_object.erl#L492
        CType ->
            {Media, _Params} = mochiweb_util:parse_header(CType),
            case string:tokens(Media, "/") of
                [_Type, _Subtype] ->
                    %% accept whatever the user says
                    LocalCtx = LocalCtx0#key_context{putctype=Media},
                    {[{Media, add_acl_to_context_then_accept}], RD, Ctx#rcs_s3_context{local_context=LocalCtx}};
                _ ->
                    %% TODO:
                    %% Maybe we should have caught
                    %% this in malformed_request?
                    {[],
                     wrq:set_resp_header(
                       "Content-Type",
                       "text/plain",
                       wrq:set_resp_body(
                         ["\"", Media, "\""
                          " is not a valid media type"
                          " for the Content-type header.\n"],
                         RD)),
                     Ctx}
            end
    end.


-spec produce_body(term(), term()) -> {iolist()|binary(), term(), term()}.
produce_body(RD, Ctx=#rcs_s3_context{local_context=LocalCtx,
                                     requested_perm='READ_ACP',
                                     user=User}) ->
    #key_context{get_fsm_pid = GetFsmPid,
                 manifest = ?MANIFEST{bkey = {Bucket, File},
                                      vsn = Vsn,
                                      acl = Acl}} = LocalCtx,
    BFile_str = bfile_str(Bucket, File, Vsn),
    UserName = riak_cs_wm_utils:extract_name(User),
    riak_cs_dtrace:dt_object_entry(?MODULE, <<"object_acl_get">>,
                                   [], [UserName, BFile_str]),
    riak_cs_get_fsm:stop(GetFsmPid),
    {AclXml, DtraceTag} = case Acl of
                              no_acl_yet -> {riak_cs_acl_utils:empty_acl_xml(), -1};
                              _ -> {riak_cs_xml:to_xml(Acl), -2}
                          end,
    riak_cs_dtrace:dt_object_return(?MODULE, <<"object_acl_get">>,
                                    [DtraceTag], [UserName, BFile_str]),
    {AclXml, RD, Ctx}.

-spec accept_body(term(), term()) ->
    {boolean() | {halt, term()}, term(), term()}.
accept_body(RD, Ctx = #rcs_s3_context{local_context = #key_context{get_fsm_pid = GetFsmPid,
                                                                   manifest = Mfst,
                                                                   key = Key,
                                                                   obj_vsn = Vsn,
                                                                   bucket = Bucket},
                                      user = User,
                                      acl = AclFromHeadersOrDefault,
                                      requested_perm = 'WRITE_ACP',
                                      riak_client = RcPid})  when Bucket /= undefined,
                                                                  Key /= undefined,
                                                                  Mfst /= undefined,
                                                                  RcPid /= undefined ->
    BFile_str = bfile_str(Bucket, Key, Vsn),
    UserName = riak_cs_wm_utils:extract_name(User),
    riak_cs_dtrace:dt_object_entry(?MODULE, <<"object_put_acl">>,
                                      [], [UserName, BFile_str]),
    riak_cs_get_fsm:stop(GetFsmPid),
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
        {ok, Acl} ->
            %% Write new ACL to active manifest
            case riak_cs_utils:set_object_acl(Bucket, Key, Vsn, Mfst, Acl, RcPid) of
                ok ->
                    riak_cs_dtrace:dt_object_return(?MODULE, <<"object_acl_put">>,
                                                    [200], [UserName, BFile_str]),
                    {{halt, 200}, RD, Ctx};
                {error, Reason} ->
                    Code = riak_cs_s3_response:status_code(Reason),
                    riak_cs_dtrace:dt_object_return(?MODULE, <<"object_acl_put">>,
                                                    [Code], [UserName, BFile_str]),
                    riak_cs_s3_response:api_error(Reason, RD, Ctx)
            end;
        {error, Reason2} ->
            Code = riak_cs_s3_response:status_code(Reason2),
            riak_cs_dtrace:dt_object_return(?MODULE, <<"object_acl_put">>,
                                            [Code], [UserName, BFile_str]),
            riak_cs_s3_response:api_error(Reason2, RD, Ctx)
    end.

bfile_str(B, K, ?LFS_DEFAULT_OBJECT_VERSION) ->
    [B, $,, K];
bfile_str(B, K, V) ->
    [B, $,, K, $,, V].
