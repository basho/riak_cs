%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module to mirror the API of `riakc_pb_socket' so
%% to facilitate testing of the `riak_moss_writer' and
%% `riak_moss_deleter' modules.

-module(riak_socket_dummy).

%% API
-export([get/3,
         put/2]).

%% @doc Dummy get function
-spec get(pid(), binary(), binary()) -> {ok, term()}.
get(_Pid, Bucket, Key) ->
    {ok, riakc_obj:new(Bucket, Key, <<"val">>)}.

%% @doc Dummy put function
-spec put(pid(), term()) -> ok.
put(_Pid, _Obj) ->
    ok.

