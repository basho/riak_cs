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
                    %% foundry access_tokens length is 32 characters
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
	UserId= resolveUserId(AccessToken),
	ClientId= resolveClientId(AccessToken),
	case resolveScope(AccessToken) of
        "S3" ->
            RequestHeadersTemp = mochiweb_headers:insert("user_id", UserId, wrq:req_headers(RD)),
            NewRequestHeaders = mochiweb_headers:insert("client_id", ClientId, RequestHeadersTemp),
            RD#wm_reqdata{req_headers=NewRequestHeaders};
        _ ->
            RD
    end.
	
resolveUserId(AccessToken) ->
	XMLResponse = auth_request("validate", AccessToken),
	{XML, _Rest} = xmerl_scan:string(XMLResponse),
	[ #xmlText{value=UserId} ] = xmerl_xpath:string("/user/uid/text()", XML),
	UserId.

resolveClientId(AccessToken) ->
	XMLResponse = auth_request("application", AccessToken),
	{XML, _Rest} = xmerl_scan:string(XMLResponse),
	[ #xmlText{value=ClientId} ] = xmerl_xpath:string("/application/info/client-id/text()", XML),
	ClientId.

resolveScope(AccessToken) ->
	XMLResponse = auth_request("accesstoken", AccessToken),
	{XML, _Rest} = xmerl_scan:string(XMLResponse),
	[ #xmlText{value=Rank} ] = xmerl_xpath:string("/access-token/scope/text()", XML),
	Rank.


%% Example:
%% curl -v -X GET -H "Authorization: Basic dGhpc2lzU0VDUkVUOlRvdGFsbHlTM2NyM3Q=" "https://auth.tfoundry.com/apigee/validate?token=8b8c07dcaaee58027def7df4ec1ea729"


auth_request(What, AccessToken) ->
    RequestHeaders = [{"Authorization", "Basic dGhpc2lzU0VDUkVUOlRvdGFsbHlTM2NyM3Q="}],
    RequestURI = lists:flatten([
        "https",
        "://",
        "auth.tfoundry.com",
        "/apigee/",
        What,
        "?token=",
		AccessToken
    ]),
    Response = httpc:request(get, {RequestURI, RequestHeaders}, [], []),
    case Response of
        {ok, {{_HTTPVer, OKStatus, _StatusLine}, _ResponseHeaders, ResponseBody}}
          when OKStatus >= 200, OKStatus =< 299 ->
            ResponseBody;
        {ok, {{_HTTPVer, Status, _StatusLine}, _ResponseHeaders, _ResponseBody}} ->
            erlang:error({aws_error, {http_error, Status, _StatusLine, _ResponseBody}});
        {error, Error} ->
            erlang:error({aws_error, {socket_error, Error}})
    end.
