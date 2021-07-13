%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
%%               2021 TI Tokyo    All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% ---------------------------------------------------------------------

-module(riak_cs_bucket_name_test).

-compile(export_all).
-compile(nowarn_export_all).

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
    %% http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
     <<"ab">>,
     <<"Abc">>,
     <<"abC">>,
     <<"my.aWs.bucket">>,
     <<".myawsbucket">>,
     <<"myawsbucket.">>,
     <<"my..examplebucket">>,
     <<"192.168.1.1">>].

make_test_from_name_and_success(Name, Success) ->
    ?_assertEqual(Success,
                  riak_cs_bucket:valid_bucket_name(Name)).

-endif.
