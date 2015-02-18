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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(RIAKCPOOL, request_pool).

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
                              riak_client=RcPid, user=User}) ->
    UserName = riak_cs_wm_utils:extract_name(User),
    riak_cs_dtrace:dt_bucket_entry(?MODULE, <<"multiple_delete">>, [], [UserName, Bucket]),


    handle_with_bucket_obj(riak_cs_bucket:fetch_bucket_object(Bucket, RcPid), RD, Ctx).

handle_with_bucket_obj({error, notfound}, RD,
                       #context{response_module=ResponseMod} = Ctx) ->
    ResponseMod:api_error(no_such_bucket, RD, Ctx);

handle_with_bucket_obj({error, _} = Error, RD,
                       #context{response_module=ResponseMod} = Ctx) ->
    _ = lager:debug("bucket error: ~p", [Error]),
    ResponseMod:api_error(Error, RD, Ctx);

handle_with_bucket_obj({ok, BucketObj},
                       RD, Ctx=#context{bucket=Bucket,
                                        riak_client=RcPid, user=User,
                                        response_module=ResponseMod,
                                        policy_module=PolicyMod}) ->

    case parse_body(binary_to_list(wrq:req_body(RD))) of
        {error, _} = Error ->
            ResponseMod:api_error(Error, RD, Ctx);
        {ok, BinKeys} when length(BinKeys) > 1000 ->
            %% Delete Multiple Objects accepts a request to delete up to 1000 Objects.
            ResponseMod:api_error(malformed_xml, RD, Ctx);
        {ok, BinKeys} ->
            Policy = riak_cs_wm_utils:translate_bucket_policy(PolicyMod, BucketObj),
            CanonicalId = riak_cs_wm_utils:extract_canonical_id(User),
            Access0 = PolicyMod:reqdata_to_access(RD, object, CanonicalId),

            %% map: keys => delete_results => xmlElements
            Results =
                lists:map(fun(BinKey) ->
                                  Result = handle_key(RcPid, Bucket, BinKey,
                                                      check_permission(
                                                        RcPid, Bucket, BinKey,
                                                        Access0, CanonicalId, Policy, PolicyMod, BucketObj)),
                                  lager:debug("attempted to delete /buckets/~s/objects/~s result=~p",
                                              [Bucket, to_logging_key(BinKey), element(1, Result)]),
                                  Result
                          end, BinKeys),

            %% xmlDoc => return body.
            Xml = riak_cs_xml:to_xml([{'DeleteResult', [{'xmlns', ?S3_XMLNS}], Results}]),

            RD2 = wrq:set_resp_body(Xml, RD),
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"multiple_delete">>, [200], []),
            {true, RD2, Ctx}
    end.

-spec check_permission(riak_client(), binary(), binary(),
                       access(), string(), policy()|undefined, atom(), riakc_obj:riakc_obj()) ->
                              ok | {error, access_denied|notfound|no_active_manifest}.
check_permission(RcPid, Bucket, Key,
                 Access0, CanonicalId, Policy, PolicyMod, BucketObj) ->
    case riak_cs_manifest:fetch(RcPid, Bucket, Key) of
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
-spec handle_key(riak_client(), binary(), binary(),
                ok | {error, access_denied|notfound|no_active_manifest}) ->
                        {'Deleted', list(tuple())} | {'Error', list(tuple())}.
handle_key(_RcPid, _Bucket, Key, {error, notfound}) ->
    %% delete is RESTful, thus this is success
    {'Deleted', [{'Key', [Key]}]};
handle_key(_RcPid, _Bucket, Key, {error, no_active_manifest}) ->
    %% delete is RESTful, thus this is success
    {'Deleted', [{'Key', [Key]}]};
handle_key(_RcPid, _Bucket, Key, {error, Error}) ->
    {'Error',
     [{'Key', [Key]},
      {'Code', [riak_cs_s3_response:error_code(Error)]},
      {'Message', [riak_cs_s3_response:error_message(Error)]}]};
handle_key(RcPid, Bucket, Key, ok) ->
    case riak_cs_utils:delete_object(Bucket, Key, RcPid) of
        {ok, _UUIDsMarkedforDelete} ->
            {'Deleted', [{'Key', [Key]}]};
        Error ->
            handle_key(RcPid, Bucket, Key, Error)
    end.

-spec parse_body(string()) -> {ok, [binary()]} | {error, malformed_xml}.
parse_body(Body) ->
    case riak_cs_xml:scan(Body) of
        {ok, #xmlElement{name='Delete'} = ParsedData} ->
            Keys = [ unicode:characters_to_binary(
                       [ T#xmlText.value || T <- xmerl_xpath:string("//Key/text()", Node)]
                      ) || Node <- xmerl_xpath:string("//Delete/Object/node()", ParsedData),
                           is_record(Node, xmlElement) ],
        %% TODO: handle version id
        %% VersionIds = [riak_cs_utils:hexlist_to_binary(string:strip(T#xmlText.value, both, $")) ||
        %%                  T <- xmerl_xpath:string("//Delete/Object/VersionId/text()", ParsedData)],
            {ok, Keys};
        {ok, _ParsedData} ->
            {error, malformed_xml};
        Error ->
             Error
    end.

to_logging_key(BinKey) ->
    mochiweb_util:quote_plus(
      string:join(lists:map(
           fun mochiweb_util:quote_plus/1,
           binary:split(BinKey, <<"/">>, [global])), "/")).

-ifdef(TEST).

parse_body_test() ->
    Body = "<Delete> <Object> <Key>&lt;/Key&gt;</Key> </Object> </Delete>",
    ?assertEqual({ok, [<<"</Key>">>]}, parse_body(Body)).

-endif.
