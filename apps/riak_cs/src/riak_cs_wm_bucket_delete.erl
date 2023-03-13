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

%% @doc WM resouce for Delete Multiple Objects

-module(riak_cs_wm_bucket_delete).

-export([init/1,
         stats_prefix/0,
         allowed_methods/0,
         post_is_create/2,
         process_post/2
        ]).

-ignore_xref([init/1,
              stats_prefix/0,
              allowed_methods/0,
              post_is_create/2,
              process_post/2
             ]).

-export([authorize/2]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("xmerl/include/xmerl.hrl").
-include_lib("kernel/include/logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(RIAKCPOOL, request_pool).

-spec init(#rcs_s3_context{}) -> {ok, #rcs_s3_context{}}.
init(Ctx) ->
    {ok, Ctx#rcs_s3_context{rc_pool=?RIAKCPOOL}}.

-spec stats_prefix() -> multiple_delete.
stats_prefix() -> multiple_delete.

-spec allowed_methods() -> [atom()].
allowed_methods() ->
    %% POST is for Delete Multiple Objects
    ['POST'].

%% TODO: change to authorize/spec/cleanup unneeded cases
%% TODO: requires update for multi-delete
-spec authorize(#wm_reqdata{}, #rcs_s3_context{}) -> {boolean(), #wm_reqdata{}, #rcs_s3_context{}}.
authorize(RD, Ctx) ->
    Bucket = list_to_binary(wrq:path_info(bucket, RD)),
    {false, RD, Ctx#rcs_s3_context{bucket=Bucket}}.

post_is_create(RD, Ctx) ->
    {false, RD, Ctx}.

-spec process_post(#wm_reqdata{}, #rcs_s3_context{}) -> {term(), #wm_reqdata{}, #rcs_s3_context{}}.
process_post(RD, Ctx=#rcs_s3_context{bucket=Bucket,
                                     riak_client=RcPid, user=User}) ->
    UserName = riak_cs_wm_utils:extract_name(User),
    riak_cs_dtrace:dt_bucket_entry(?MODULE, <<"multiple_delete">>, [], [UserName, Bucket]),

    handle_with_bucket_obj(riak_cs_bucket:fetch_bucket_object(Bucket, RcPid), RD, Ctx).

handle_with_bucket_obj({error, notfound}, RD,
                       #rcs_s3_context{response_module=ResponseMod} = Ctx) ->
    ResponseMod:api_error(no_such_bucket, RD, Ctx);

handle_with_bucket_obj({error, _} = Error, RD,
                       #rcs_s3_context{response_module=ResponseMod} = Ctx) ->
    ?LOG_DEBUG("bucket error: ~p", [Error]),
    ResponseMod:api_error(Error, RD, Ctx);

handle_with_bucket_obj({ok, BucketObj},
                       RD, Ctx=#rcs_s3_context{bucket=Bucket,
                                               riak_client=RcPid, user=User,
                                               response_module=ResponseMod,
                                               policy_module=PolicyMod}) ->

    case parse_body(binary_to_list(wrq:req_body(RD))) of
        {error, _} = Error ->
            ResponseMod:api_error(Error, RD, Ctx);
        {ok, Keys} when length(Keys) > 1000 ->
            %% Delete Multiple Objects accepts a request to delete up to 1000 Objects.
            ResponseMod:api_error(malformed_xml, RD, Ctx);
        {ok, Keys} ->
            ?LOG_DEBUG("deleting keys at ~p: ~p", [Bucket, Keys]),

            Policy = riak_cs_wm_utils:translate_bucket_policy(PolicyMod, BucketObj),
            CanonicalId = riak_cs_wm_utils:extract_canonical_id(User),
            Access0 = PolicyMod:reqdata_to_access(RD, object, CanonicalId),

            %% map: keys => delete_results => xmlElements
            Results =
                lists:map(fun({Key, Vsn} = VKey) ->
                                  handle_key(RcPid, Bucket, VKey,
                                             check_permission(
                                               RcPid, Bucket, Key, Vsn,
                                               Access0, CanonicalId, Policy, PolicyMod, BucketObj))
                          end, Keys),

            %% xmlDoc => return body.
            Xml = riak_cs_xml:to_xml([{'DeleteResult', [{'xmlns', ?S3_XMLNS}], Results}]),

            RD2 = wrq:set_resp_body(Xml, RD),
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"multiple_delete">>, [200], []),
            {true, RD2, Ctx}
    end.

-spec check_permission(riak_client(), binary(), binary(), binary(),
                       access(), string(), policy()|undefined, atom(), riakc_obj:riakc_obj()) ->
                              ok | {error, access_denied|notfound|no_active_manifest}.
check_permission(RcPid, Bucket, Key, Vsn,
                 Access0, CanonicalId, Policy, PolicyMod, BucketObj) ->
    case riak_cs_manifest:fetch(RcPid, Bucket, Key, Vsn) of
        {ok, Manifest} ->
            ObjectAcl = riak_cs_manifest:object_acl(Manifest),
            Access = Access0#access_v1{key=Key, method='DELETE', target=object},

            case riak_cs_wm_utils:check_object_authorization(Access, false, ObjectAcl,
                                                             Policy, CanonicalId, PolicyMod,
                                                             RcPid, BucketObj) of
                {ok, _} -> ok;
                {error, _} -> {error, access_denied}
            end;
        E ->
            E
    end.

%% bucket/key => delete => xml indicating each result
-spec handle_key(riak_client(), binary(), {binary(), binary()},
                ok | {error, access_denied|notfound|no_active_manifest}) ->
                        {'Deleted', list(tuple())} | {'Error', list(tuple())}.
handle_key(_RcPid, _Bucket, {Key, Vsn}, {error, notfound}) ->
    %% delete is RESTful, thus this is success
    {'Deleted', [{'Key', [Key]}, {'VersionId', [Vsn]}]};
handle_key(_RcPid, _Bucket, {Key, Vsn}, {error, no_active_manifest}) ->
    {'Deleted', [{'Key', [Key]}, {'VersionId', [Vsn]}]};
handle_key(_RcPid, _Bucket, {Key, Vsn}, {error, Error}) ->
    {'Error',
     [{'Key', [Key]},
      {'VersionId', [Vsn]},
      {'Code', [riak_cs_s3_response:error_code(Error)]},
      {'Message', [riak_cs_s3_response:error_message(Error)]}]};
handle_key(RcPid, Bucket, {Key, Vsn}, ok) ->
    case riak_cs_utils:delete_object(Bucket, Key, Vsn, RcPid) of
        {ok, _UUIDsMarkedforDelete} ->
            {'Deleted', [{'Key', [Key]}, {'VersionId', [Vsn]}]};
        Error ->
            handle_key(RcPid, Bucket, {Key, Vsn}, Error)
    end.

parse_body(Body) ->
    case riak_cs_xml:scan(Body) of
        {ok, #xmlElement{name='Delete'} = ParsedData} ->
            KKVV = [ key_and_version_from_xml_node(Node)
                     || Node <- xmerl_xpath:string("//Delete/Object", ParsedData),
                        is_record(Node, xmlElement) ],
            case lists:member(malformed_xml, KKVV) of
                true ->
                    {error, malformed_xml};
                false ->
                    {ok, KKVV}
            end;
        {ok, _ParsedData} ->
            {error, malformed_xml};
        Error ->
             Error
    end.

key_and_version_from_xml_node(Node) ->
    case {xmerl_xpath:string("//Key/text()", Node),
          xmerl_xpath:string("//VersionId/text()", Node)} of
        {[#xmlText{value = K}], [#xmlText{value = V}]} ->
            {unicode:characters_to_binary(K), unicode:characters_to_binary(V)};
        {[#xmlText{value = K}], _} ->
            {unicode:characters_to_binary(K), ?LFS_DEFAULT_OBJECT_VERSION};
        _ ->
            malformed_xml
    end.

-ifdef(TEST).

parse_body_test() ->
    Body = "<Delete> <Object> <Key>&lt;/Key&gt;</Key> </Object> </Delete>",
    ?assertEqual({ok, [{<<"</Key>">>, ?LFS_DEFAULT_OBJECT_VERSION}]}, parse_body(Body)).

-endif.
