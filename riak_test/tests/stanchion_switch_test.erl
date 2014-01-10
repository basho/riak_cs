%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

-module(stanchion_switch_test).

%% @doc `riak_test' module for testing riak-cs-stanchion switch command.

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET,        "riak-test-bucket").

-define(BACKUP_PORT, 9096).

backup_stanchion_config() ->
    [
     rtcs:lager_config(),
     {stanchion,
      [{stanchion_port, ?BACKUP_PORT},
       {riak_pb_port, 10017}
      ]
      }].

confirm() ->
    {UserConfig, {RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup(1),

    lists:foreach(fun(RiakNode) ->
                          N = rt_cs_dev:node_id(RiakNode),
                          ?assertEqual("Current Stanchion Adderss: http://127.0.0.1:9095\n",
                                       rtcs:show_stanchion_cs(N))
                  end, RiakNodes),

    %% stanchion ops ok
    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),
    lager:info("deleting bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:delete_bucket(?TEST_BUCKET, UserConfig)),

    %% stop stanchion to check ops fails
    _ = rtcs:stop_stanchion(),

    %% stanchion ops ng; we get 500 here for sure.
    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertException(error, {aws_error, {http_error, 500, _, _}},
                    erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),

    ok = rtcs:deploy_stanchion(backup_stanchion_config()),

    %% stanchion ops ng; we get 500 here for sure.
    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertException(error, {aws_error, {http_error, 500, _, _}},
                     erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),

    %% switch stanchion here, for all CS nodes
    lists:foreach(fun(RiakNode) ->
                          N = rt_cs_dev:node_id(RiakNode),
                          rtcs:switch_stanchion_cs(N, "127.0.0.1", ?BACKUP_PORT),
                          ?assertEqual("Current Stanchion Adderss: http://127.0.0.1:9096\n",
                                       rtcs:show_stanchion_cs(N))
                  end, RiakNodes),

    %% stanchion ops ok again
    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),
    lager:info("deleting bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:delete_bucket(?TEST_BUCKET, UserConfig)),
    pass.

