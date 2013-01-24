%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_wm_bucket_versioning).

-export([content_types_provided/2,
         to_xml/2,
         allowed_methods/0]).

-export([authorize/2]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").

%% @doc Get the list of methods this resource supports.
-spec allowed_methods() -> [atom()].
allowed_methods() ->
    %% TODO: PUT?
    ['GET'].

-spec content_types_provided(#wm_reqdata{}, #context{}) -> {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_provided(RD, Ctx) ->
    {[{"application/xml", to_xml}], RD, Ctx}.

-spec authorize(#wm_reqdata{}, #context{}) -> 
                       {boolean() | {halt, term()}, #wm_reqdata{}, #context{}}.
authorize(RD, Ctx) ->
    riak_cs_wm_utils:bucket_access_authorize_helper(bucket_version, false, RD, Ctx).


-spec to_xml(#wm_reqdata{}, #context{}) -> 
                    {binary() | {halt, term()}, #wm_reqdata{}, #context{}}.
to_xml(RD, Ctx=#context{user=User,bucket=Bucket}) ->
    StrBucket = binary_to_list(Bucket),
    case [B || B <- riak_cs_utils:get_buckets(User),
               B?RCS_BUCKET.name =:= StrBucket] of
        [] ->
            riak_cs_s3_response:api_error(no_such_bucket, RD, Ctx);
        [_BucketRecord] ->
            {<<"<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>">>,
             RD, Ctx}
    end.


