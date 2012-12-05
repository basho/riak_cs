%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_wm_buckets).

-export([allowed_methods/0,
         content_types_provided/2,
         to_xml/2,
         anon_ok/0]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").

%% @doc Get the list of methods this resource supports.
-spec allowed_methods() -> [atom()].
allowed_methods() ->
    ['GET'].

-spec content_types_provided(#wm_reqdata{},#context{}) -> [{string(), atom()}].
content_types_provided(RD,Ctx) ->
    {[{"application/xml", to_xml}], RD, Ctx}.

%% @TODO This spec will need to be updated when we change this to
%% allow streaming bodies.
-spec to_xml(#wm_reqdata{}, #context{}) -> {{'halt', non_neg_integer()}, #wm_reqdata{}, #context{}}.
to_xml(RD, Ctx=#context{start_time=StartTime,
                        user=User}) ->
    riak_cs_dtrace:dt_service_entry(?MODULE, <<"service_get_buckets">>, [], [riak_cs_wm_utils:extract_name(User)]),
    Res = riak_cs_s3_response:list_all_my_buckets_response(User, RD, Ctx),
    ok = riak_cs_stats:update_with_start(service_get_buckets, StartTime),
    riak_cs_dtrace:dt_service_return(?MODULE, <<"service_get_buckets">>, [], [riak_cs_wm_utils:extract_name(User)]),
    Res.

-spec anon_ok() -> boolean().
anon_ok() ->
    false.
