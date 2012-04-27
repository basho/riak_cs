%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module to manage storage of objects and files

-module(riak_moss_delete_marker).

-include("riak_moss.hrl").

-export([delete/3]).

%% @doc Mark all active manifests as
%%      pending_delete.
%% TODO:
%% Should we be doing anything here to
%% do garbage collection? One idea would be
%% to start a GC process in a timer, but
%% lots of 30 minute timers could queue
%% up and maybe slow things down?
-spec delete(binary(), binary(), pid()) -> ok | {error, notfound}.
delete(Bucket, Key, RiakPid) ->
    StartTime = now(),
    {ok, Pid} = riak_moss_manifest_fsm:start_link(Bucket, Key, RiakPid),
    Res = riak_moss_manifest_fsm:mark_active_as_pending_delete(Pid),
    if Res == ok ->
            riak_cs_stats:update_with_start(object_delete, StartTime);
       true ->
            ok
    end,
    Res.
