%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_wm_rewrite).

-export([rewrite/2]).

-include("riak_cs.hrl").
-include("s3_api.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

rewrite(Headers, RawPath) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, [], ?MODULE, <<"rewrite">>, []),
    Host = mochiweb_headers:get_value("host", Headers),
    {Path, QueryString, _} = mochiweb_util:urlsplit_path(RawPath),
    SubResources = get_subresources(QueryString),
    do_rewrite(Headers, Path, bucket_from_host(Host), SubResources).

do_rewrite(_Headers, Path, undefined, _SubResources) ->
    Path;
do_rewrite(_Headers, Path, Bucket, _SubResources) ->
    "/buckets/" ++ Bucket ++ "/" ++ string:strip(Path, left, $/).

bucket_from_host(undefined) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, [], ?MODULE, <<"bucket_from_host">>, []),
    undefined;
bucket_from_host(HostHeader) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, [], ?MODULE, <<"bucket_from_host">>, []),
    HostNoPort = hd(string:tokens(HostHeader, ":")),
    {ok, RootHost} = application:get_env(riak_cs, cs_root_host),
    case HostNoPort of
        RootHost ->
            undefined;
        Host ->
            case string:str(HostNoPort, RootHost) of
                0 ->
                    undefined;
                I ->
                    string:substr(Host, 1, I-2)
            end
    end.

%% @doc Parse the valid subresources from the raw path.
-spec get_subresources(string()) -> [{string(), string()}].
get_subresources(QueryString) ->
    lists:filter(fun valid_subresource/1, mochiweb_util:parse_qs(QueryString)).

%% @doc Determine if a query parameter key is a valid S3 subresource
-spec valid_subresource({string(), string()}) -> boolean().
valid_subresource({Key, _}) ->
    lists:member(Key, ?SUBRESOURCES).


%% ===================================================================
%% Eunit tests
%% ===================================================================

-ifdef(TEST).

rewrite_test() ->
    ?assert(true).

-endif.
