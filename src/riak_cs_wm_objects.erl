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

-module(riak_cs_wm_objects).

-export([init/1,
         allowed_methods/0,
         post_is_create/2,
         process_post/2,
         api_request/2
        ]).

-export([authorize/2]).

-include("riak_cs.hrl").
-include("riak_cs_api.hrl").
-include("list_objects.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-define(RIAKCPOOL, bucket_list_pool).

-spec init(#context{}) -> {ok, #context{}}.
init(Ctx) ->
    lager:debug("~p", [Ctx]),
    {ok, Ctx#context{rc_pool=?RIAKCPOOL}}.

-spec allowed_methods() -> [atom()].
allowed_methods() ->
    %% POST is for multi-delete
    %% GET is for object listing
    ['GET', 'POST'].

%% TODO: change to authorize/spec/cleanup unneeded cases
%% TODO: requires update for multi-delete
-spec authorize(#wm_reqdata{}, #context{}) -> {boolean(), #wm_reqdata{}, #context{}}.
authorize(RD, Ctx) ->
    case wrq:method(RD) of
        'GET' ->
            riak_cs_wm_utils:bucket_access_authorize_helper(bucket, false, RD, Ctx);
        'POST' ->
            %% TODO: authorization is played per each objects?
            %% or paththrough and check each key at process_post?
            riak_cs_wm_utils:bucket_access_authorize_helper(bucket, true, RD, Ctx)
    end.

post_is_create(RD, Ctx) ->
    {false, RD, Ctx}.

-spec process_post(#wm_reqdata{}, #context{}) -> {term(), #wm_reqdata{}, #context{}}.
process_post(RD, Ctx=#context{bucket=Bucket, riak_client=RcPid, user=User}) ->
    UserName = riak_cs_wm_utils:extract_name(User),
    riak_cs_dtrace:dt_bucket_entry(?MODULE, <<"multiple_delete">>, [], [UserName, Bucket]),

    Keys = parse_body(binary_to_list(wrq:req_body(RD))),
    lager:debug("deleting keys at ~p: ~p", [Bucket, Keys]),

    %% map: keys => delete_results => xmlElements
    Results = [ handle_key(RcPid, Bucket, list_to_binary(Key)) || Key <- Keys ],

    %% xmlDoc => return body.
    Xml = riak_cs_xml:simple_form_to_xml([{'DeleteResult', [{'xmlns', ?S3_XMLNS}], Results}]),

    RD2 = wrq:set_resp_body(Xml, RD),
    riak_cs_dtrace:dt_bucket_return(?MODULE, <<"multiple_delete">>, [200], []),
    {true, RD2, Ctx}.

%% bucket/key => delete => xml indicating each result
-spec handle_key(riak_client(), binary(), binary()) -> tuple().
handle_key(RcPid, Bucket, Key) ->
    %% TODO: no authorization and permission check here;
    %% do we even need them, or should we?
    case riak_cs_utils:delete_object(Bucket, Key, RcPid) of
        {ok, _UUIDsMarkedforDelete} ->
            {'Deleted', [{'Key', [Key]}]};
        {error, Error} ->
            {'Error',
             [{'Key', [Key]},
              {'Code', [riak_cs_s3_response:error_code(Error)]},
              {'Message', [riak_cs_s3_response:error_message(Error)]}]}
    end.

-spec parse_body(string()) -> [string()].
parse_body(Body0) ->
    try
        Body = re:replace(Body0, "&quot;", "", [global, {return, list}]),
        {ok, ParsedData} = riak_cs_xml:scan(Body),
        #xmlElement{name='Delete'} = ParsedData,
        Keys = [T#xmlText.value ||
                   T <- xmerl_xpath:string("//Delete/Object/Key/text()", ParsedData)]
        %% VersionIds = [riak_cs_utils:hexlist_to_binary(string:strip(T#xmlText.value, both, $")) ||
        %%                  T <- xmerl_xpath:string("//Delete/Object/VersionId/text()", ParsedData)],
        
    catch _:_ ->
            bad
    end.


-spec api_request(#wm_reqdata{}, #context{}) -> {ok, ?LORESP{}} | {error, term()}.
api_request(RD, Ctx=#context{bucket=Bucket,
                             riak_client=RcPid,
                             user=User,
                             start_time=StartTime}) ->
    UserName = riak_cs_wm_utils:extract_name(User),
    riak_cs_dtrace:dt_bucket_entry(?MODULE, <<"list_keys">>, [], [UserName, Bucket]),
    Res = riak_cs_api:list_objects(
            [B || B <- riak_cs_bucket:get_buckets(User),
                  B?RCS_BUCKET.name =:= binary_to_list(Bucket)],
            Ctx#context.bucket,
            get_max_keys(RD),
            get_options(RD),
            RcPid),
    ok = riak_cs_stats:update_with_start(bucket_list_keys, StartTime),
    riak_cs_dtrace:dt_bucket_return(?MODULE, <<"list_keys">>, [200], [UserName, Bucket]),
    Res.

-spec get_options(#wm_reqdata{}) -> [{atom(), 'undefined' | binary()}].
get_options(RD) ->
    [get_option(list_to_atom(Opt), wrq:get_qs_value(Opt, RD)) ||
        Opt <- ["delimiter", "marker", "prefix"]].

-spec get_option(atom(), 'undefined' | string()) -> {atom(), 'undefined' | binary()}.
get_option(Option, undefined) ->
    {Option, undefined};
get_option(Option, Value) ->
    {Option, list_to_binary(Value)}.

-spec get_max_keys(#wm_reqdata{}) -> non_neg_integer() | {error, 'invalid_argument'}.
get_max_keys(RD) ->
    case wrq:get_qs_value("max-keys", RD) of
        undefined ->
            ?DEFAULT_LIST_OBJECTS_MAX_KEYS;
        StringKeys ->
            try
                erlang:min(list_to_integer(StringKeys),
                           ?DEFAULT_LIST_OBJECTS_MAX_KEYS)
            catch _:_ ->
                    {error, invalid_argument}
            end
    end.
