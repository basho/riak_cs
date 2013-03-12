%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_bucket_name_test).

-compile(export_all).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

bucket_name_test_() ->
    [make_test_from_name_and_success(Name, true) ||
         Name <- valid_names()] ++
    [make_test_from_name_and_success(Name, false) ||
        Name <- invalid_names()].

valid_names() ->
    [<<"this.is.valid">>,
     <<"abc">>,
     <<"this-has-hyhens-and-is-valid">>,
     <<"myawsbucket">>,
     <<"my.aws.bucket">>,
     <<"myawsbucket.1">>].

invalid_names() ->
    [<<"NotValid">>,
    %% some tests from
    %% http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html].
     <<".myawsbucket">>,
     <<"myawsbucket.">>,
     <<"my..examplebucket">>,
     <<"192.168.1.1">>].

make_test_from_name_and_success(Name, Success) ->
    fun () ->
            ?assertEqual(Success,
                         riak_cs_utils:valid_bucket_name(Name))
    end.

-endif.
