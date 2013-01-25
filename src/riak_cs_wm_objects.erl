%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_wm_objects).

-export([init/1,
         allowed_methods/0,
         content_types_provided/2,
         to_xml/2]).

-export([authorize/2]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-define(RIAKCPOOL, bucket_list_pool).

-spec init(#context{}) -> {ok, #context{}}.
init(Ctx) ->
    {ok, Ctx#context{riakc_pool=?RIAKCPOOL}}.

-spec allowed_methods() -> [atom()].
allowed_methods() ->
    %% TODO: POST (multi-delete)
    ['GET'].

-spec content_types_provided(#wm_reqdata{}, #context{}) -> {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_provided(RD,Ctx) ->
    {[{"application/xml", to_xml}], RD, Ctx}.


%% TODO: change to authorize/spec/cleanup unneeded cases
%% TODO: requires update for multi-delete
-spec authorize(#wm_reqdata{}, #context{}) -> {boolean(), #wm_reqdata{}, #context{}}.
authorize(RD, Ctx) ->
    riak_cs_wm_utils:bucket_access_authorize_helper(bucket, false, RD, Ctx).

-spec to_xml(#wm_reqdata{}, #context{}) ->
                    {binary() | {'halt', non_neg_integer()}, #wm_reqdata{}, #context{}}.
to_xml(RD, Ctx=#context{start_time=StartTime,
                        user=User,
                        bucket=Bucket,
                        requested_perm='READ',
                        riakc_pid=RiakPid}) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"to_xml">>, [], [riak_cs_wm_utils:extract_name(User), Bucket]),
    riak_cs_dtrace:dt_bucket_entry(?MODULE, <<"list_keys">>, [], [riak_cs_wm_utils:extract_name(User), Bucket]),
    StrBucket = binary_to_list(Bucket),
    case [B || B <- riak_cs_utils:get_buckets(User),
               B?RCS_BUCKET.name =:= StrBucket] of
        [] ->
            CodeName = no_such_bucket,
            Res = riak_cs_s3_response:api_error(CodeName, RD, Ctx),
            Code = riak_cs_s3_response:status_code(CodeName),
            riak_cs_dtrace:dt_wm_return(?MODULE, <<"to_xml">>, [Code], [riak_cs_wm_utils:extract_name(User), Bucket]),
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"list_keys">>, [Code], [riak_cs_wm_utils:extract_name(User), Bucket]),
            Res;
        [_BucketRecord] ->
            MaxKeys = case wrq:get_qs_value("max-keys", RD) of
                          undefined ->
                              ?DEFAULT_LIST_OBJECTS_MAX_KEYS;
                          StringKeys ->
                              erlang:min(list_to_integer(StringKeys),
                                         ?DEFAULT_LIST_OBJECTS_MAX_KEYS)
                      end,
            Options = get_options(RD),
            ListKeysRequest = riak_cs_list_objects:new_request(Bucket,
                                                               MaxKeys,
                                                               Options),
            BinPid = riak_cs_utils:pid_to_binary(self()),
            CacheKey = << BinPid/binary, <<":">>/binary, Bucket/binary >>,
            UseCache = riak_cs_list_objects_ets_cache:cache_enabled(),
            case riak_cs_list_objects_fsm:start_link(RiakPid, self(),
                                                     ListKeysRequest, CacheKey,
                                                     UseCache) of
                {ok, ListFSMPid} ->
                    {ok, ListObjectsResponse} = riak_cs_list_objects_fsm:get_object_list(ListFSMPid),
                    Response = riak_cs_xml:to_xml(ListObjectsResponse),
                    ok = riak_cs_stats:update_with_start(bucket_list_keys,
                                                         StartTime),
                    riak_cs_dtrace:dt_wm_return(?MODULE, <<"to_xml">>, [200], [riak_cs_wm_utils:extract_name(User), Bucket]),
                    riak_cs_dtrace:dt_bucket_return(?MODULE, <<"list_keys">>, [200], [riak_cs_wm_utils:extract_name(User), Bucket]),
                    riak_cs_s3_response:respond(200, Response, RD, Ctx);
                {error, Reason} ->
                    Code = riak_cs_s3_response:status_code(Reason),
                    Response = riak_cs_s3_response:api_error(Reason, RD, Ctx),
                    riak_cs_dtrace:dt_wm_return(?MODULE, <<"to_xml">>, [Code], [riak_cs_wm_utils:extract_name(User), Bucket]),
                    riak_cs_dtrace:dt_bucket_return(?MODULE, <<"list_keys">>, [Code], [riak_cs_wm_utils:extract_name(User), Bucket]),
                    Response
            end
    end.

-spec get_options(#wm_reqdata{}) -> [{atom(), binary()}].
get_options(RD) ->
    [get_option(list_to_atom(Opt), wrq:get_qs_value(Opt, RD)) ||
        Opt <- ["delimiter", "marker", "prefix"]].

-spec get_option(#wm_reqdata{}, 'undefined' | string()) -> {atom(), binary()}.
get_option(Option, undefined) ->
    {Option, undefined};
get_option(Option, Value) ->
    {Option, list_to_binary(Value)}.
