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
    riak_cs_dtrace:dt_bucket_entry(?MODULE, <<"bucket_list_keys">>, 
                                      [], [riak_cs_wm_utils:extract_name(User), Bucket]),
    StrBucket = binary_to_list(Bucket),
    case [B || B <- riak_cs_utils:get_buckets(User),
               B?RCS_BUCKET.name =:= StrBucket] of
        [] ->
            CodeName = no_such_bucket,
            Res = riak_cs_s3_response:api_error(CodeName, RD, Ctx),
            Code = riak_cs_s3_response:status_code(CodeName),
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_list_keys">>, 
                                               [Code], [riak_cs_wm_utils:extract_name(User), Bucket]),
            Res;
        [BucketRecord] ->
            Prefix = list_to_binary(wrq:get_qs_value("prefix", "", RD)),
            case riak_cs_utils:get_keys_and_manifests(Bucket, Prefix, RiakPid) of
                {ok, KeyObjPairs} ->
                    X = riak_cs_s3_response:list_bucket_response(User,
                                                                   BucketRecord,
                                                                   KeyObjPairs,
                                                                   RD,
                                                                   Ctx),
                    ok = riak_cs_stats:update_with_start(bucket_list_keys,
                                                         StartTime),
                    riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_list_keys">>, 
                                                       [200], [riak_cs_wm_utils:extract_name(User), Bucket]),
                    X;
                {error, Reason} ->
                    Code = riak_cs_s3_response:status_code(Reason),
                    X = riak_cs_s3_response:api_error(Reason, RD, Ctx),
                    riak_cs_dtrace:dt_bucket_return(?MODULE, <<"bucket_list_keys">>, 
                                                       [Code], [riak_cs_wm_utils:extract_name(User), Bucket]),
                    X
            end
    end.
