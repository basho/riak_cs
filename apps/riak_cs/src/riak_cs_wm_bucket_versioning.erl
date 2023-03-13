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

-module(riak_cs_wm_bucket_versioning).

-export([stats_prefix/0,
         content_types_provided/2,
         content_types_accepted/2,
         to_xml/2,
         accept_body/2,
         allowed_methods/0
        ]).

-ignore_xref([stats_prefix/0,
              content_types_provided/2,
              content_types_accepted/2,
              to_xml/2,
              accept_body/2,
              allowed_methods/0
             ]).

-export([authorize/2]).

-include("riak_cs.hrl").
-include_lib("xmerl/include/xmerl.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-spec stats_prefix() -> bucket_versioning.
stats_prefix() -> bucket_versioning.

%% @doc Get the list of methods this resource supports.
-spec allowed_methods() -> [atom()].
allowed_methods() ->
    ['GET', 'PUT'].

-spec content_types_provided(#wm_reqdata{}, #rcs_s3_context{}) -> {[{string(), atom()}], #wm_reqdata{}, #rcs_s3_context{}}.
content_types_provided(RD, Ctx) ->
    {[{"application/xml", to_xml}], RD, Ctx}.

-spec content_types_accepted(#wm_reqdata{}, #rcs_s3_context{}) ->
          {[{string(), atom()}], #wm_reqdata{}, #rcs_s3_context{}}.
content_types_accepted(RD, Ctx) ->
    case wrq:get_req_header("content-type", RD) of
        undefined ->
            {[{"application/octet-stream", add_acl_to_context_then_accept}], RD, Ctx};
        CType ->
            {Media, _Params} = mochiweb_util:parse_header(CType),
            {[{Media, add_acl_to_context_then_accept}], RD, Ctx}
    end.

-spec authorize(#wm_reqdata{}, #rcs_s3_context{}) ->
                       {boolean() | {halt, term()}, #wm_reqdata{}, #rcs_s3_context{}}.
authorize(RD, Ctx) ->
    riak_cs_wm_utils:bucket_access_authorize_helper(bucket_version, false, RD, Ctx).


-spec to_xml(#wm_reqdata{}, #rcs_s3_context{}) ->
          {binary() | {halt, term()}, #wm_reqdata{}, #rcs_s3_context{}}.
to_xml(RD, Ctx=#rcs_s3_context{user = User,
                               bucket = Bucket,
                               riak_client = RcPid}) ->
    StrBucket = binary_to_list(Bucket),
    case [B || B <- riak_cs_bucket:get_buckets(User),
               B?RCS_BUCKET.name =:= StrBucket] of
        [] ->
            riak_cs_s3_response:api_error(no_such_bucket, RD, Ctx);
        [_BucketRecord] ->
            {ok, #bucket_versioning{status = Status,
                                    mfa_delete = MFADelete,
                                    use_subversioning = UseSubVersioning,
                                    can_update_versions = CanUpdateVersions,
                                    repl_siblings = ReplSiblings}} =
                riak_cs_bucket:get_bucket_versioning(Bucket, RcPid),
            {iolist_to_binary(["<VersioningConfiguration>",
                               "<Status>", to_string(status, Status), "</Status>",
                               "<MFADelete>", to_string(mfa_delete, MFADelete), "</MFADelete>",
                               "<UseSubVersioning>", to_string(bool, UseSubVersioning), "</UseSubVersioning>",
                               "<CanUpdateVersions>", to_string(bool, CanUpdateVersions), "</CanUpdateVersions>",
                               "<ReplSiblings>", to_string(bool, ReplSiblings), "</ReplSiblings>",
                               "</VersioningConfiguration>"]),
             RD, Ctx}
    end.

to_string(status, enabled) -> "Enabled";
to_string(status, suspended) -> "Suspended";
to_string(mfa_delete, enabled) -> "Enabled";
to_string(mfa_delete, disabled) -> "Disabled";
to_string(bool, true) -> "True";
to_string(bool, false) -> "False".

-spec accept_body(#wm_reqdata{}, #rcs_s3_context{}) -> {{halt, integer()}, #wm_reqdata{}, #rcs_s3_context{}}.
accept_body(RD, Ctx = #rcs_s3_context{user = User,
                                      user_object = UserObj,
                                      bucket = Bucket,
                                      response_module = ResponseMod,
                                      riak_client = RcPid}) ->
    riak_cs_dtrace:dt_bucket_entry(?MODULE, <<"bucket_put_versioning">>,
                                   [], [riak_cs_wm_utils:extract_name(User), Bucket]),
    {ok, OldV} = riak_cs_bucket:get_bucket_versioning(Bucket, RcPid),
    case riak_cs_xml:scan(binary_to_list(wrq:req_body(RD))) of
        {ok, Doc} ->
            {NewV, IsUpdated} =
                update_versioning_struct_from_headers(
                  update_versioning_struct_from_xml(OldV, Doc),
                  RD),
            case IsUpdated of
                true ->
                    riak_cs_bucket:set_bucket_versioning(
                      User, UserObj, Bucket, NewV, RcPid),
                    riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_put_versioning">>,
                                                    [200], [riak_cs_wm_utils:extract_name(User), Bucket]),
                    {{halt, 200}, RD, Ctx};
                false ->
                    riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_put_versioning">>,
                                                    [200], [riak_cs_wm_utils:extract_name(User), Bucket]),
                    {{halt, 200}, RD, Ctx};
                {error, Reason} ->
                    Code = ResponseMod:status_code(Reason),
                    riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_put_versioning">>,
                                                    [Code], [riak_cs_wm_utils:extract_name(User), Bucket]),
                    ResponseMod:api_error(Reason, RD, Ctx)
            end;
        {error, Reason} ->
            Code = ResponseMod:status_code(Reason),
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_put_versioning">>,
                                            [Code], [riak_cs_wm_utils:extract_name(User), Bucket]),
            ResponseMod:api_error(Reason, RD, Ctx)
    end.

update_versioning_struct_from_xml(Old, #xmlElement{name = 'VersioningConfiguration',
                                                   content = Content}) ->
    MaybeNew =
        lists:foldl(
          fun(#xmlElement{name = 'Status', content = C}, Acc) ->
                  Acc#bucket_versioning{status = from_xml_node_content(status, C, Old#bucket_versioning.status)};
             (#xmlElement{name = 'MFADelete', content = C}, Acc) ->
                  Acc#bucket_versioning{mfa_delete = from_xml_node_content(mfa_delete, C, Old#bucket_versioning.mfa_delete)};
             (#xmlElement{name = 'UseSubVersioning', content = C}, Acc) ->
                  Acc#bucket_versioning{use_subversioning = from_xml_node_content(use_subversioning, C, Old#bucket_versioning.use_subversioning)};
             (#xmlElement{name = 'CanUpdateVersions', content = C}, Acc) ->
                  Acc#bucket_versioning{can_update_versions = from_xml_node_content(can_update_versions, C, Old#bucket_versioning.can_update_versions)};
             (#xmlElement{name = 'ReplSiblings', content = C}, Acc) ->
                  Acc#bucket_versioning{repl_siblings = from_xml_node_content(repl_siblings, C, Old#bucket_versioning.repl_siblings)};
             (#xmlElement{}, Acc) ->
                  Acc
          end,
          Old,
          Content),
    case Old == MaybeNew of
        true ->
            {Old, false};
        false ->
            {MaybeNew, true}
    end;
update_versioning_struct_from_xml(Old, _) ->
    {Old, {error, malformed_xml}}.

from_xml_node_content(status, CC, Old) ->
    case lists:search(fun(#xmlText{}) -> true; (_) -> false end, CC) of
        {value, #xmlText{value = "Enabled"}} ->
            enabled;
        {value, #xmlText{value = "Suspended"}} ->
            suspended;
        _ ->
            Old
    end;
from_xml_node_content(mfa_delete, CC, Old) ->
    case lists:search(fun(#xmlText{}) -> true; (_) -> false end, CC) of
        {value, #xmlText{value = "Enabled"}} ->
            enabled;
        {value, #xmlText{value = "Disabled"}} ->
            disabled;
        _ ->
            Old
    end;
from_xml_node_content(_, CC, Old) ->
    case lists:search(fun(#xmlText{}) -> true; (_) -> false end, CC) of
        {value, #xmlText{value = V}} when V == "True";
                                          V == "true" ->
            true;
        {value, #xmlText{value = V}} when V == "False";
                                          V == "false" ->
            false;
        _ ->
            Old
    end.

update_versioning_struct_from_headers({OldV, IsUpdated0}, RD) ->
    MaybeNew =
        lists:foldl(fun({H, F}, Q) -> maybe_set_field(F, to_bool(wrq:get_req_header(H, RD)), Q) end,
                    OldV,
                    [{"x-rcs-versioning-use_subversioning", #bucket_versioning.use_subversioning},
                     {"x-rcs-versioning-can_update_versions", #bucket_versioning.can_update_versions},
                     {"x-rcs-versioning-repl_siblings", #bucket_versioning.repl_siblings}]),
    if MaybeNew == OldV ->
            {OldV, IsUpdated0};
       el/=se ->
            {MaybeNew, true}
    end.
maybe_set_field(_, undefined, OldV) ->
    OldV;
maybe_set_field(F, V, OldV) ->
    if element(F, OldV) == V ->
            OldV;
       el/=se ->
            setelement(F, OldV, V)
    end.

to_bool("True") -> true;
to_bool("true") -> true;
to_bool("1") -> true;
to_bool("False") -> false;
to_bool("false") -> false;
to_bool("0") -> false;
to_bool(_) -> undefined.
