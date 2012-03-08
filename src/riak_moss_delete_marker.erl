%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module to manage storage of objects and files

-module(riak_moss_delete_marker).

-include("riak_moss.hrl").

-export([delete/2]).

%% @doc Mark the currently active
%%      manifest as deleted. If it doens't
%%      exist, return notfound.
%% TODO:
%% Should we be doing anything here to
%% do garbage collection? One idea would be
%% to start a GC process in a timer, but
%% lots of 30 minute timers could queue
%% up and maybe slow things down?
-spec delete(binary(), binary()) -> ok | {error, notfound}.
delete(Bucket, Key) ->
    {ok, Pid} = riak_moss_manifest_fsm:start_link(Bucket, Key),
    case riak_moss_manifest_fsm:get_active_manifest(Pid) of
        {ok, Manifest} ->
            DelMani = Manifest#lfs_manifest_v2{state=pending_delete},
            riak_moss_manifest_fsm:update_manifest(Pid, DelMani),
            ok;
        {error, no_active_manifest} ->
            {error, notfound}
    end.
