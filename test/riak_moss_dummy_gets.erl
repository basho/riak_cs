%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_dummy_gets).

%% API
-export([get_object/2]).

%% @doc Dummy get function
get_object(Bucket, Key) ->
    Manifest = riak_moss_lfs_utils:new_manifest(<<"dummy">>,
                                                <<"dummy">>,
                                                <<"uuid">>,
                                                3000, % content length
                                                <<"md5">>,
                                                dict:new()), % metadata
    RiakObj = riakc_obj:new_obj(Bucket, Key, [], [{dict:new(), Manifest}]),
    {ok, RiakObj}.
