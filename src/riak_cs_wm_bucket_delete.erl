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

%% @doc WM resouce for Delete Multiple Objects

-module(riak_cs_wm_bucket_delete).

-export([init/1,
         allowed_methods/0,
         post_is_create/2,
         process_post/2
        ]).

-export([authorize/2]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-define(RIAKCPOOL, bucket_list_pool).

-spec init(#context{}) -> {ok, #context{}}.
init(Ctx) ->
    {ok, Ctx#context{rc_pool=?RIAKCPOOL}}.

-spec allowed_methods() -> [atom()].
allowed_methods() ->
    %% POST is for Delete Multiple Objects
    ['POST'].

%% TODO: change to authorize/spec/cleanup unneeded cases
%% TODO: requires update for multi-delete
-spec authorize(#wm_reqdata{}, #context{}) -> {boolean(), #wm_reqdata{}, #context{}}.
authorize(RD, Ctx) ->
    Bucket = list_to_binary(wrq:path_info(bucket, RD)),
    {false, RD, Ctx#context{bucket=Bucket}}.

post_is_create(RD, Ctx) ->
    {false, RD, Ctx}.

-spec process_post(#wm_reqdata{}, #context{}) -> {term(), #wm_reqdata{}, #context{}}.
process_post(RD, Ctx=#context{bucket=Bucket,
                              riak_client=RcPid, user=User,
                              response_module=ResponseMod,
                              policy_module=PolicyMod}) ->
    UserName = riak_cs_wm_utils:extract_name(User),
    riak_cs_dtrace:dt_bucket_entry(?MODULE, <<"multiple_delete">>, [], [UserName, Bucket]),

    Keys = parse_body(binary_to_list(wrq:req_body(RD))),
    lager:debug("deleting keys at ~p: ~p", [Bucket, Keys]),

    case riak_cs_bucket:fetch_bucket_object(Bucket, RcPid) of
        {error, _} = Error ->
            ResponseMod:api_error(Error, RD, Ctx);
        {ok, BucketObj} ->

            Policy = riak_cs_wm_utils:translate_bucket_policy(PolicyMod, BucketObj),
            CanonicalId = riak_cs_wm_utils:extract_canonical_id(User),
            Access0 = PolicyMod:reqdata_to_access(RD, object, CanonicalId),

            %% map: keys => delete_results => xmlElements
            Results = [ handle_key(RcPid, Bucket, list_to_binary(Key),
                                   check_permission(
                                     RcPid, Bucket, list_to_binary(Key),
                                     Access0, CanonicalId, Policy, PolicyMod, BucketObj))
                        || Key <- Keys ],

            %% xmlDoc => return body.
            Xml = riak_cs_xml:to_xml([{'DeleteResult', [{'xmlns', ?S3_XMLNS}], Results}]),

            RD2 = wrq:set_resp_body(Xml, RD),
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"multiple_delete">>, [200], []),
            {true, RD2, Ctx}
    end.

-spec check_permission(riak_client(), binary(), binary(),
                       access(), string(), policy()|undefined, atom(), riakc_obj:riakc_obj()) ->
                              ok | {error, access_denied}.
check_permission(RcPid, Bucket, Key,
                 Access0, CanonicalId, Policy, PolicyMod, BucketObj) ->
    {ok, Manifest} = riak_cs_manifest:fetch(RcPid, Bucket, Key),
    ObjectAcl = riak_cs_manifest:object_acl(Manifest),
    Access = Access0#access_v1{key=Key, method='DELETE', target=object},

    case riak_cs_wm_utils:check_object_authorization(Access, false, ObjectAcl,
                                                     Policy, CanonicalId, PolicyMod,
                                                     RcPid, BucketObj) of
        {ok, _} -> ok;
        {error, _} -> {error, access_denied}
    end.

%% bucket/key => delete => xml indicating each result
-spec handle_key(riak_client(), binary(), binary(), term()) -> tuple().
handle_key(_RcPid, _Bucket, Key, {error, Error}) ->
    {'Error',
     [{'Key', [Key]},
      {'Code', [riak_cs_s3_response:error_code(Error)]},
      {'Message', [riak_cs_s3_response:error_message(Error)]}]};
handle_key(RcPid, Bucket, Key, ok) ->

    %% TODO: no authorization and permission check here;
    %% do we even need them, or should we?
    case riak_cs_utils:delete_object(Bucket, Key, RcPid) of
        {ok, _UUIDsMarkedforDelete} ->
            {'Deleted', [{'Key', [Key]}]};
        Error ->
            handle_key(RcPid, Bucket, Key, Error)
    end.

-spec parse_body(string()) -> [string()].
parse_body(Body0) ->
    try
        Body = re:replace(Body0, "&quot;", "", [global, {return, list}]),
        {ok, ParsedData} = riak_cs_xml:scan(Body),
        #xmlElement{name='Delete'} = ParsedData,
        _Keys = [T#xmlText.value ||
                    T <- xmerl_xpath:string("//Delete/Object/Key/text()", ParsedData)]
        %% VersionIds = [riak_cs_utils:hexlist_to_binary(string:strip(T#xmlText.value, both, $")) ||
        %%                  T <- xmerl_xpath:string("//Delete/Object/VersionId/text()", ParsedData)],

    catch _:_ ->
            bad
    end.
