%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_object).

-compile(export_all).

new(KeyId, Bucket, Key) ->
    ?MODULE:new(KeyId, Bucket, Key, <<>>).

new(KeyId, Bucket, Key, Val) ->
    riakc_obj:new(riak_moss:make_bucket(KeyId, Bucket), Key, Val).



