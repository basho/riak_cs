%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved,
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

%% @doc PropEr test module for `riak_cs_s3_rewrite'.

-module(prop_riak_cs_s3_rewrite).

-include("riak_cs.hrl").

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

%% PropEr property
-export([prop_extract_bucket_from_host/0]).

-define(QC_OUT(P),
        on_output(fun(Str, Args) ->
                          io:format(user, Str, Args) end, P)).
-define(TESTING_TIME, 20).

%%====================================================================
%% Eunit tests
%%====================================================================

proper_test_() ->
    Tests =
        [
         {timeout, ?TESTING_TIME*2,
          ?_assertEqual(true, proper:quickcheck(?QC_OUT(prop_s3_rewrite(pathstyle))))},
         {timeout, ?TESTING_TIME*2,
          ?_assertEqual(true, proper:quickcheck(?QC_OUT(prop_s3_rewrite(hoststyle))))},
         {timeout, ?TESTING_TIME*2,
          ?_assertEqual(true, proper:quickcheck(?QC_OUT(prop_extract_bucket_from_host())))
         }],
    [{inparallel, Tests}].

%% ====================================================================
%% EQC Properties
%% ====================================================================

%% @doc This test verifies that the key for object manifest is exactly same as
%% the key before URL encoding. This is also a regression test of riak_cs#1040.
prop_s3_rewrite(Style) ->
    RewriteModule = riak_cs_s3_rewrite,
    RootHost = "s3.amazonaws.com",
    ok = application:set_env(riak_cs, cs_root_host, RootHost),
    DispatchTable = riak_cs_web:object_api_dispatch_table(),
    ?FORALL({CSBucket, CSKey, Verb, Scheme, Version},
            {riak_cs_gen:bucket(), riak_cs_gen:file_name(),
             riak_cs_gen:http_verb(), riak_cs_gen:http_scheme(),
             riak_cs_gen:http_version()},
            begin
                {Encoded, Host} = build_original_path_info(Style, CSBucket, CSKey, RootHost),

                %% Rewrite the path
                Headers0 = riak_cs_s3_rewrite_test:headers([{"host", Host}]),
                {Headers, Path} = RewriteModule:rewrite(Verb, Scheme, Version, Headers0, Encoded),

                %% Let webmachine dispatcher create the #wm_reqdata to
                %% make `riak_cs_wm_utils:extract_key/2' work,
                %% Imitating webmachine dispatcher like CS.
                RD0 = wrq:create(Verb, Scheme, Version, Encoded, Headers),
                {_Mod, _ModOpts, HostTokens, Port, PathTokens, Bindings,
                 AppRoot, StringPath} = webmachine_dispatcher:dispatch(Host, Path,
                                                                       DispatchTable,
                                                                       RD0),
                RD = wrq:load_dispatch_data(orddict:from_list(Bindings),
                                            HostTokens, Port, PathTokens, AppRoot,
                                            StringPath, RD0),

                %% Get the Bucket name and Key name to be used inside CS.
                %% Corresponding {Bucket, Key} for manifest is
                %% <<"0o:", hash(Bucket)/binary>> and Key - The key should be exactly
                %% same as the original one in the client-app before URL encoding.
                Ctx = #rcs_context{local_context=#key_context{}},
                {ok, #rcs_context{local_context=LocalCtx}} = riak_cs_wm_utils:extract_key(RD, Ctx),

                %% ?debugVal(CSKey),
                {CSBucket, CSKey} =:=
                    {LocalCtx#key_context.bucket,
                     unicode:characters_to_binary(LocalCtx#key_context.key)}
            end).

prop_extract_bucket_from_host() ->
    ?FORALL({Bucket, BaseHost},
            {riak_cs_gen:bucket_or_blank(), base_host()},
    ?IMPLIES(nomatch == re:run(Bucket, ":", [{capture, first}]),
            begin
                BucketStr = binary_to_list(Bucket),
                Host = compose_host(BucketStr, BaseHost),
                ExpectedBucket = expected_bucket(BucketStr, BaseHost),
                ResultBucket =
                    riak_cs_s3_rewrite:bucket_from_host(Host, BaseHost),
                equals(ExpectedBucket, ResultBucket)
            end)).

%%====================================================================
%% Helpers
%%====================================================================

%% @doc Create encoded HTTP URI suffix corresponding to the original key
%% And create double-quoted Path to make wm dispatch
build_original_path_info(hoststyle, CSBucket, CSKey, RootHost) ->
    Encoded = mochiweb_util:quote_plus(unicode:characters_to_list(CSKey)),
    Host = lists:flatten([binary_to_list(CSBucket), $. , RootHost]),
    {Encoded, Host};
build_original_path_info(pathstyle, CSBucket, CSKey, RootHost) ->
    Encoded0 = mochiweb_util:quote_plus(unicode:characters_to_list(CSKey)),
    Encoded = lists:flatten([$/, unicode:characters_to_list(CSBucket), $/, Encoded0]),
    {Encoded, RootHost}.

base_host() ->
    oneof(["s3.amazonaws.com", "s3.out-west.amazonaws.com"]).

compose_host([], BaseHost) ->
    BaseHost;
compose_host(Bucket, []) ->
    Bucket;
compose_host(Bucket, BaseHost) ->
    Bucket ++ "." ++  BaseHost.

expected_bucket([], _BaseHost) ->
    undefined;
expected_bucket(_Bucket, []) ->
    undefined;
expected_bucket(Bucket, _) ->
    Bucket.
