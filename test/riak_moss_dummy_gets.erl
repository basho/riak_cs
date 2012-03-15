%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_dummy_gets).

%% API
-export([get_object/2,
         get_object/3]).

%% @doc Dummy get function
get_object(Bucket, Key) ->
    Manifest = riak_moss_lfs_utils:new_manifest(<<"dummy">>,
                                                <<"dummy">>,
                                                <<"uuid">>,
                                                10000, % content length
                                                <<"md5">>,
                                                dict:new(),
                                                riak_moss_lfs_utils:block_size()), % metadata
    RiakObj = riakc_obj:new_obj(Bucket, Key, [], [{dict:new(), term_to_binary(Manifest)}]),
    {ok, RiakObj}.

%% @doc Dummy get function
get_object(Bucket, Key, _RiakPid) ->
    get_object(Bucket, Key).
