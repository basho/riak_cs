%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(riak_moss_wm_rewrite).

-include("riak_moss.hrl").

-export([rewrite/2]).

bucket_from_host(undefined) ->
    undefined;
bucket_from_host(HostHeader) ->
    HostNoPort = hd(string:tokens(HostHeader, ":")),
    {ok, RootHost} = application:get_env(?RIAKCS, riak_cs_root_host),
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
    HostHeader = mochiweb_headers:get_value("host", Headers),
    case bucket_from_host(HostHeader) of
        undefined ->
            Path;
        Bucket ->
            "/" ++ Bucket ++ "/" ++ string:strip(Path, left, $/)
    end.
