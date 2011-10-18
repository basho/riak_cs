%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_wm_utils).

-export([service_available/2,
         parse_auth_header/2]).

-include("riak_moss.hrl").
-include_lib("webmachine/include/webmachine.hrl").

service_available(RD, Ctx) ->
    %% TODO:
    %% At some point in the future
    %% this needs to check if we have
    %% an alive Riak server. Although
    %% maybe it makes more sense to be
    %% optimistic and wait untl we actually
    %% check the ACL?

    %% For now we just always
    %% return true
    {true, RD, Ctx}.

%% @doc Parse an authentication header string and determine
%%      the appropriate module to use to authenticate the request.
%%      The passthru auth can be used either with a KeyID or
%%      anonymously by leving the header empty.
-spec parse_auth_header(string(), boolean()) -> {ok, atom(), [string()]} | {error, term()}.
parse_auth_header(KeyID, true) when KeyID =/= undefined ->
    {ok, riak_moss_passthru_auth, [KeyID]};
parse_auth_header(_, true) ->
    {ok, riak_moss_passthru_auth, []};
parse_auth_header(undefined, false) ->
    {ok, riak_moss_blockall_auth, [unkown_auth_scheme]};
parse_auth_header("AWS " ++ Key, _) ->
    case string:tokens(Key, ":") of
        [KeyId, KeyData] ->
            {ok, riak_moss_s3_auth, [KeyId, KeyData]};
        Other -> Other
    end;
parse_auth_header(_, _) ->
    {ok, riak_moss_blockall_auth, [unkown_auth_scheme]}.

