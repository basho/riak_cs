%% -------------------------------------------------------------------
%%
%% stats_http_resource: publishing RiakCS runtime stats via HTTP
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_wm_stats).

%% webmachine resource exports
-export([
         init/1,
         ping/2,
         encodings_provided/2,
         content_types_provided/2,
         service_available/2,
         forbidden/2,
         finish_request/2]).
-export([
         produce_body/2,
         pretty_print/2
        ]).

-include("riak_moss.hrl").
-include_lib("webmachine/include/wm_reqdata.hrl").

-record(ctx, {
          auth_bypass :: boolean(),
          riakc_pid :: pid(),
          path_tokens :: [string()]
         }).

init(Props) ->
    dt_entry(<<"init">>, [], []),
    AuthBypass = proplists:get_value(auth_bypass, Props, false),
    {ok, #ctx{auth_bypass = AuthBypass}}.

%% @spec encodings_provided(webmachine:wrq(), context()) ->
%%         {[encoding()], webmachine:wrq(), context()}
%% @doc Get the list of encodings this resource provides.
%%      "identity" is provided for all methods, and "gzip" is
%%      provided for GET as well
encodings_provided(RD, Context) ->
    dt_entry(<<"encodings_provided">>, [], []),
    case wrq:method(RD) of
        'GET' ->
            {[{"identity", fun(X) -> X end},
              {"gzip", fun(X) -> zlib:gzip(X) end}], RD, Context};
        _ ->
            {[{"identity", fun(X) -> X end}], RD, Context}
    end.

%% @spec content_types_provided(webmachine:wrq(), context()) ->
%%          {[ctype()], webmachine:wrq(), context()}
%% @doc Get the list of content types this resource provides.
%%      "application/json" and "text/plain" are both provided
%%      for all requests.  "text/plain" is a "pretty-printed"
%%      version of the "application/json" content.
%%
%%      NOTE: s3cmd doesn't appear to honor --add-header when using
%%            "get --add-header=Accept:text/plain s3://RiakCS/stats",
%%            so s3cmd will only be able to get the JSON flavor.

content_types_provided(RD, Context) ->
    dt_entry(<<"content_types_provided">>, [], []),
    {[{"application/json", produce_body},
      {"text/plain", pretty_print}],
     RD, Context}.

ping(RD, Ctx) ->
    dt_entry(<<"pong">>, [], []),
    {pong, RD, Ctx#ctx{path_tokens = path_tokens(RD)}}.

service_available(RD, #ctx{path_tokens = []} = Ctx) ->
    dt_entry(<<"service_available">>, [], []),
    case riak_moss_utils:get_env(riak_moss, riak_cs_stat, false) of
        false ->
            {false, RD, Ctx};
        true ->
            case riak_moss_utils:riak_connection() of
                {ok, Pid} ->
                    {true, RD, Ctx#ctx{riakc_pid = Pid}};
                _ ->
                    {false, RD, Ctx}
            end
    end;
service_available(RD, Ctx) ->
    dt_entry(<<"service_available">>, [], []),
    {false, RD, Ctx}.

produce_body(RD, Ctx) ->
    dt_entry(<<"produce_body">>, [], []),
    Body = mochijson2:encode(get_stats()),
    ETag = riak_moss_utils:binary_to_hexlist(crypto:md5(Body)),
    RD2 = wrq:set_resp_header("ETag", ETag, RD),
    dt_return(<<"produce_body">>, [], []),
    {Body, RD2, Ctx}.

forbidden(RD, #ctx{auth_bypass = AuthBypass, riakc_pid = RiakPid} = Ctx) ->
    dt_entry(<<"forbidden">>, [], []),
    case riak_moss_wm_utils:validate_auth_header(RD, AuthBypass, RiakPid) of
        {ok, User, _UserVclock} ->
            UserKeyId = User?MOSS_USER.key_id,
            case riak_moss_utils:get_admin_creds() of
                {ok, {Admin, _}} when Admin == UserKeyId ->
                    %% admin account is allowed
                    dt_return(<<"forbidden">>, [], [<<"false">>, Admin]),
                    {false, RD, Ctx};
                _ ->
                    Res = riak_moss_wm_utils:deny_access(RD, Ctx),
                    dt_return(<<"forbidden">>, [], [<<"true">>]),
                    Res
            end;
        _ ->
            Res = riak_moss_wm_utils:deny_access(RD, Ctx),
            dt_return(<<"forbidden">>, [], [<<"true">>]),
            Res
    end.

finish_request(RD, #ctx{riakc_pid=undefined}=Ctx) ->
    dt_entry(<<"finish_request">>, [0], []),
    {true, RD, Ctx};
finish_request(RD, #ctx{riakc_pid=RiakPid}=Ctx) ->
    dt_entry(<<"finish_request">>, [1], []),
    riak_moss_utils:close_riak_connection(RiakPid),
    dt_return(<<"finish_request">>, [1], []),
    {true, RD, Ctx#ctx{riakc_pid=undefined}}.

%% @spec pretty_print(webmachine:wrq(), context()) ->
%%          {string(), webmachine:wrq(), context()}
%% @doc Format the respons JSON object is a "pretty-printed" style.
pretty_print(RD1, C1=#ctx{}) ->
    {Json, RD2, C2} = produce_body(RD1, C1),
    Body = riak_moss_utils:json_pp_print(lists:flatten(Json)),
    ETag = riak_moss_utils:binary_to_hexlist(crypto:md5(term_to_binary(Body))),
    RD3 = wrq:set_resp_header("ETag", ETag, RD2),
    {Body, RD3, C2}.

get_stats() ->
    riak_cs_stats:get_stats().

path_tokens(RD) ->
    wrq:path_tokens(RD).

dt_entry(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, Ints, ?MODULE, Func, Strings).

dt_return(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 2, Ints, ?MODULE, Func, Strings).
