%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_foundry_utils).

-export([validate_auth_header/2,
         convert_foundry_accesstoken/1]).

-include_lib("xmerl/include/xmerl.hrl").
-include("riak_cs.hrl").

-record(wm_reqdata, {method, scheme, version, peer, wm_state,
                     disp_path, path, raw_path, path_info, path_tokens,
                     app_root,response_code,max_recv_body, max_recv_hunk,
                     req_cookie, req_qs, req_headers, req_body,
                     resp_redirect, resp_headers, resp_body,
                     host_tokens, port, notes
                    }).


convert_foundry_accesstoken(RD) ->
    AuthHeader = wrq:get_req_header("authorization", RD),
    case AuthHeader of
        undefined ->
            RD;
        "AWS " ++ Key ->
            [AccessToken, _] = string:tokens(Key, ":"),
            case string:len(AccessToken) of
                32 ->
                    %% Length of foundry access_tokens is 32 characters
                    convert_to_additional_headers(RD, AccessToken);
                _ ->
                    RD
            end
    end.

validate_auth_header(RD, RiakPid) ->
    ClientId = wrq:get_req_header("client_id", RD),
    UserId = wrq:get_req_header("user_id", RD),
    AuthId = UserId ++ "###" ++ ClientId,
    FoundryClientId = AuthId ++ "@attfoundry.com",

    case riak_cs_utils:get_user_by_index(?EMAIL_INDEX, list_to_binary(FoundryClientId), RiakPid) of
        {ok, {User, UserVclock}} ->
            {ok, User, UserVclock};
        {error, _} ->
            {ok, NewUser} = riak_cs_utils:create_user(AuthId, FoundryClientId),
            {ok, {User, UserVclock}} = riak_cs_utils:get_user(NewUser?MOSS_USER.key_id, RiakPid),
            {ok, User, UserVclock}
    end.


convert_to_additional_headers(RD, AccessToken) ->
    %% If we run into any issue,
    %% we pass RD to CS standard authentication, which will throw a key error
    try
        {ClientId, UserId, Scope} = resolve(AccessToken),
        case Scope of
            "S3" ->
                RequestHeadersTemp = mochiweb_headers:insert("user_id", UserId, wrq:req_headers(RD)),
                NewRequestHeaders = mochiweb_headers:insert("client_id", ClientId, RequestHeadersTemp),
                RD#wm_reqdata{req_headers=NewRequestHeaders};
            _ ->
                RD
        end
    catch
        Exception:Reason ->
            io:format("~p ~p~n", [Exception, Reason]),
            RD
    end.

resolve(AccessToken) ->
    JSONResponse = auth_request(AccessToken),
    {struct, Response} = mochijson2:decode(JSONResponse),
    Entries = proplists:get_value(<<"entry">>, Response),
    {ClientId, UserId, Scope} = entries_to_items(Entries),
    io:format("Result ~p ~p ~p~n", [ClientId, UserId, Scope]),
    {ClientId, UserId, Scope}.


entries_to_items(Entries) ->
    lists:foldl(fun entry_to_item/2, {client_id, user_id, scope}, Entries).

entry_to_item({struct,[{<<"name">>,<<"consumer_id">>}, {<<"value">>, Value}]}, {ClientId, _UserId, Scope}) ->
    {ClientId, binary:bin_to_list(Value), Scope};
entry_to_item({struct,[{<<"name">>,<<"client_application_id">>}, {<<"value">>, Value}]}, {_ClientId, UserId, Scope}) ->
    {binary:bin_to_list(Value), UserId, Scope};
entry_to_item({struct,[{<<"name">>,<<"scopes">>}, {<<"value">>, _Value}]}, {ClientId, UserId, _Scope}) ->
    {ClientId, UserId, "S3"};
entry_to_item(_, Result) -> Result.

%% Example:
%% curl -v -X GET -H "Authorization: Basic bWFyY2VsQGJhc2hvLmNvbTpaaWd6YWcxMjM=" "http://75.62.61.233:8080/v1/organizations/prod/maps/8od2ssonlbetz0xyhtbsrf9ifwnm00oh"

auth_request(AccessToken) ->
    RequestHeaders = [{"Authorization", "Basic bWFyY2VsQGJhc2hvLmNvbTpaaWd6YWcxMjM="}],
    RequestURI = lists:flatten([
        "http",
        "://",
        "75.62.61.233:8080",
        "/v1/organizations/prod/maps/",
        AccessToken
    ]),
    Response = httpc:request(get, {RequestURI, RequestHeaders}, [], []),
    %%io:format("~p ~n", [Response]),
    case Response of
        {ok, {{_HTTPVer, OKStatus, _StatusLine}, _ResponseHeaders, ResponseBody}}
          when OKStatus >= 200, OKStatus =< 299 ->
            ResponseBody;
        {ok, {{_HTTPVer, Status, _StatusLine}, _ResponseHeaders, _ResponseBody}} ->
            erlang:error({aws_error, {http_error, Status, _StatusLine, _ResponseBody}});
        {error, Error} ->
            erlang:error({aws_error, {socket_error, Error}})
    end.
