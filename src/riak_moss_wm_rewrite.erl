%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(riak_moss_wm_rewrite).
-export([rewrite/1]).

bucket_from_host(undefined) ->
    undefined;
bucket_from_host(HostHeader) ->
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

rewrite(MochiReq) ->
    HostHeader = MochiReq:get_header_value("host"),
    case bucket_from_host(HostHeader) of
        undefined ->
            MochiReq;
        Bucket ->
            Path = "/" ++ Bucket ++ "/" ++
                string:strip(MochiReq:get(raw_path), left, $/),
            mochiweb_request:new(MochiReq:get(socket),
                                 MochiReq:get(method),
                                 Path,
                                 MochiReq:get(version),
                                 MochiReq:get(headers))
    end.
