%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%               2021 TI Tokyo    All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% ---------------------------------------------------------------------

-module(stanchion_wm_utils).

-export([service_available/2,
         parse_auth_header/2,
         iso_8601_datetime/0,
         json_to_proplist/1]).

-include("stanchion.hrl").
-include_lib("webmachine/include/webmachine.hrl").

service_available(RD, Ctx) ->
    {true, RD, Ctx}.

%% @doc Parse an authentication header string and determine
%%      the appropriate module to use to authenticate the request.
%%      The passthru auth can be used either with a KeyID or
%%      anonymously by leving the header empty.
-spec parse_auth_header(string(), boolean()) -> {ok, atom(), [string()]} | {error, term()}.
parse_auth_header(_, true) ->
    {ok, stanchion_passthru_auth, []};
parse_auth_header("MOSS " ++ Key, _) ->
    case string:tokens(Key, ":") of
        [KeyId, KeyData] ->
            {ok, stanchion_auth, [KeyId, KeyData]};
        _Other ->
            {error, {bad_key, Key}}
    end;
parse_auth_header(_, false) ->
    {ok, stanchion_blockall_auth, ["unkown_auth_scheme"]}.

%% @doc Get an ISO 8601 formatted timestamp representing
%% current time.
-spec iso_8601_datetime() -> [non_neg_integer()].
iso_8601_datetime() ->
    {{Year, Month, Day}, {Hour, Min, Sec}} = erlang:universaltime(),
    io_lib:format("~4.10.0B-~2.10.0B-~2.10.0BT~2.10.0B:~2.10.0B:~2.10.0B.000Z",
                  [Year, Month, Day, Hour, Min, Sec]).

%% @doc Convert a list of mochijson2-decoded JSON objects
%% into a standard propllist.
-type json_term() :: {struct, [{term(), term()}]}.
-type json_terms() :: [{struct, [{term(), term()}]}].
-spec json_to_proplist(json_term() | json_terms()) -> [{term(), term()}].
json_to_proplist({struct, JsonObject}) ->
    JsonObject;
json_to_proplist([{struct, JsonObjects}]) ->
    json_to_proplist(JsonObjects, []).

%% @doc Convert a list of mochijson2-decoded JSON objects
%% into a standard propllist.
-spec json_to_proplist([{'struct',[any(),...]}],[any()]) -> [any()].
json_to_proplist([], Acc) ->
    lists:flatten(Acc);
json_to_proplist([{struct, [ObjContents]} | RestObjs], Acc) ->
    json_to_proplist(RestObjs, [ObjContents | Acc]).
