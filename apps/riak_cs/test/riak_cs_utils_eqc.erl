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

%% @doc Quickcheck test module for `riak_cs_utils'.

-module(riak_cs_utils_eqc).

-ifdef(EQC).

-include("riak_cs.hrl").
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

%%====================================================================
%% Eunit tests
%%====================================================================

eqc_test_() ->
    Time = 8,
    [
     {timeout, Time*4, ?_assertEqual(true,
                                     eqc:quickcheck(eqc:testing_time(Time,?QC_OUT(prop_md5()))))}
    ].

%% ====================================================================
%% EQC Properties
%% ====================================================================

prop_md5() ->
    _ = crypto:start(),
    ?FORALL(Bin, gen_bin(),
            crypto:hash(md5, Bin) == riak_cs_utils:md5(Bin)).

gen_bin() ->
    oneof([binary(),
           ?LET({Size, Char}, {choose(5, 2*1024*1024 + 1024), choose(0, 255)},
                list_to_binary(lists:duplicate(Size, Char)))]).

%%====================================================================
%% Helpers
%%====================================================================

-endif.
