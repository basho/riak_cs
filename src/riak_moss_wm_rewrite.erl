%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(riak_moss_wm_rewrite).
-export([rewrite/2]).

-include("riak_moss.hrl").

bucket_from_host(undefined) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, [], ?MODULE, <<"bucket_from_host">>, []),
    undefined;
bucket_from_host(HostHeader) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, [], ?MODULE, <<"bucket_from_host">>, []),
    HostNoPort = hd(string:tokens(HostHeader, ":")),
    {ok, RootHost} = application:get_env(riak_moss, moss_root_host),
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

rewrite(Headers, Path) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, [], ?MODULE, <<"bucket_from_host">>, []),
    HostHeader = mochiweb_headers:get_value("host", Headers),
    case bucket_from_host(HostHeader) of
        undefined ->
            Path;
        Bucket ->
            "/" ++ Bucket ++ "/" ++ string:strip(Path, left, $/)
    end.    
