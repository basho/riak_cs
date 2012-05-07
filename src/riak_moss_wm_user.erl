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
    dt_entry(<<"init">>),
    {ok, #context{}}.

-spec service_available(term(), term()) -> {true, term(), term()}.
service_available(RD, Ctx) ->
    dt_entry(<<"service_available">>),
    {true, RD, Ctx}.

-spec allowed_methods(term(), term()) -> {[atom()], term(), term()}.
allowed_methods(RD, Ctx) ->
    dt_entry(<<"allowed_methods">>),
    {['POST'], RD, Ctx}.


%% @doc Create a user from a POST
%%      and return the user object
%%      as JSON
-spec process_post(term(), term()) -> {true, term(), term}.
process_post(RD, Ctx) ->
    dt_entry(<<"process_post">>),
    Body = wrq:req_body(RD),
    ParsedBody = mochiweb_util:parse_qs(binary_to_list(Body)),
    UserName = proplists:get_value("name", ParsedBody, ""),
    Email= proplists:get_value("email", ParsedBody, ""),
    case riak_moss_utils:create_user(UserName, Email) of
        {ok, UserRecord} ->
            PropListUser = riak_moss_wm_utils:user_record_to_proplist(UserRecord),
            CTypeWritten = wrq:set_resp_header("Content-Type", "application/json", RD),
            WrittenRD = wrq:set_resp_body(list_to_binary(
                                            mochijson2:encode(PropListUser)),
                                          CTypeWritten),
            dt_return(<<"process_post">>, [200], [UserName]),
            {true, WrittenRD, Ctx};
        {error, Reason} ->
            Code = riak_moss_s3_response:status_code(Reason),
            dt_return(<<"process_post">>, [Code], [UserName]),
            riak_moss_s3_response:api_error(Reason, RD, Ctx)
    end.

dt_entry(Func) ->
    dt_entry(Func, [], []).

dt_entry(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, Ints, ?MODULE, Func, Strings).

dt_return(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 2, Ints, ?MODULE, Func, Strings).
