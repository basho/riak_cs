%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
%%               2021 TI Tokyo    All Rights Reserved.
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
         allowed_methods/0]).

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

-spec content_types_provided(#wm_reqdata{}, #context{}) -> {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_provided(RD, Ctx) ->
    {[{"application/xml", to_xml}], RD, Ctx}.

-spec content_types_accepted(#wm_reqdata{}, #context{}) ->
                                    {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_accepted(RD, Ctx) ->
    case wrq:get_req_header("content-type", RD) of
        undefined ->
            {[{"application/octet-stream", add_acl_to_context_then_accept}], RD, Ctx};
        CType ->
            {Media, _Params} = mochiweb_util:parse_header(CType),
            {[{Media, add_acl_to_context_then_accept}], RD, Ctx}
    end.

-spec authorize(#wm_reqdata{}, #context{}) ->
                       {boolean() | {halt, term()}, #wm_reqdata{}, #context{}}.
authorize(RD, Ctx) ->
    riak_cs_wm_utils:bucket_access_authorize_helper(bucket_version, false, RD, Ctx).


-spec to_xml(#wm_reqdata{}, #context{}) ->
                    {binary() | {halt, term()}, #wm_reqdata{}, #context{}}.
to_xml(RD, Ctx=#context{user = User,
                        bucket = Bucket,
                        riak_client = RcPid}) ->
    StrBucket = binary_to_list(Bucket),
    case [B || B <- riak_cs_bucket:get_buckets(User),
               B?RCS_BUCKET.name =:= StrBucket] of
        [] ->
            riak_cs_s3_response:api_error(no_such_bucket, RD, Ctx);
        [_BucketRecord] ->
            {ok, VsnOption} = riak_cs_bucket:get_bucket_versioning(Bucket, RcPid),
            {iolist_to_binary(["<VersioningConfiguration>",
                               "<Status>", vsn_option_to_string(VsnOption), "</Status>"
                               "</VersioningConfiguration>"]),
             RD, Ctx}
    end.

vsn_option_to_string(enabled) -> "Enabled";
vsn_option_to_string(disabled) -> "Disabled".

-spec accept_body(#wm_reqdata{}, #context{}) -> {{halt, integer()}, #wm_reqdata{}, #context{}}.
accept_body(RD, Ctx = #context{user = User,
                               user_object=UserObj,
                               bucket=Bucket,
                               response_module=ResponseMod,
                               riak_client=RcPid}) ->
    riak_cs_dtrace:dt_bucket_entry(?MODULE, <<"bucket_put_versioning">>,
                                   [], [riak_cs_wm_utils:extract_name(User), Bucket]),
    case riak_cs_xml:scan(binary_to_list(wrq:req_body(RD))) of
        {ok, #xmlElement{name = 'VersioningConfiguration',
                         content = CC}} ->
            case lists:search(fun(#xmlElement{name = N}) -> N == 'Status' end, CC) of
                {value, #xmlElement{content = [#xmlText{value = VsnOption}]}}
                  when VsnOption == "Enabled";
                       VsnOption == "Disabled" ->
                    case riak_cs_bucket:set_bucket_versioning(User,
                                                              UserObj,
                                                              Bucket,
                                                              versioning_string_to_internal(VsnOption),
                                                              RcPid) of
                        ok ->
                            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_put_versioning">>,
                                                            [200], [riak_cs_wm_utils:extract_name(User), Bucket]),
                            {{halt, 200}, RD, Ctx};
                        {error, Reason} ->
                            Code = ResponseMod:status_code(Reason),
                            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_put_versioning">>,
                                                            [Code], [riak_cs_wm_utils:extract_name(User), Bucket]),
                            ResponseMod:api_error(Reason, RD, Ctx)
                    end;
                false ->
                    Reason = malformed_xml,
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

versioning_string_to_internal("Enabled") -> enabled;
versioning_string_to_internal("Disabled") -> disabled.
