%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_wm_user).

-export([init/1,
         service_available/2,
         process_post/2,
         allowed_methods/2]).

-include("riak_moss.hrl").
-include_lib("webmachine/include/webmachine.hrl").

init(_Config) ->
    {ok, #context{}}.

-spec service_available(term(), term()) -> {true, term(), term()}.
service_available(RD, Ctx) ->
    riak_moss_wm_utils:service_available(RD, Ctx).

-spec allowed_methods(term(), term()) -> {[atom()], term(), term()}.
allowed_methods(RD, Ctx) ->
    {['POST'], RD, Ctx}.


%% @doc Create a user from a POST
%%      and return the user object
%%      as JSON
-spec process_post(term(), term()) -> {true, term(), term}.
process_post(RD, Ctx) ->
    Body = wrq:req_body(RD),
    ParsedBody = mochiweb_util:parse_qs(binary_to_list(Body)),
    %% TODO:
    %% we should halt the request
    %% if this pattern doesn't match
    %% and return 400
    UserName = proplists:get_value("name", ParsedBody),
    {ok, UserRecord} = riak_moss_riakc:create_user(UserName),
    PropListUser = riak_moss_wm_utils:user_record_to_proplist(UserRecord),
    CTypeWritten = wrq:set_resp_header("Content-Type", "application/json", RD),
    WrittenRD = wrq:set_resp_body(list_to_binary(mochijson2:encode(PropListUser)), CTypeWritten),
    {true, WrittenRD, Ctx}.
