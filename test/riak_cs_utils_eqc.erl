%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Quickcheck test module for `riak_cs_utils'.

-module(riak_cs_utils_eqc).

-ifdef(EQC).

-include_lib("riak_cs_core/include/riak_cs.hrl").
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

%% eqc property
-export([prop_chunked_md5/0]).

%% Helpers
-export([test/0,
         test/1]).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

%%====================================================================
%% Eunit tests
%%====================================================================

eqc_test_() ->
    {spawn,
     [
      {timeout, 60, ?_assertEqual(true, quickcheck(eqc:testing_time(10, ?QC_OUT(prop_chunked_md5()))))}
     ]
    }.

%% ====================================================================
%% EQC Properties
%% ====================================================================

prop_chunked_md5() ->
    ?FORALL({DataBlob, ChunkSize},
            {eqc_gen:binary(2048), riak_cs_gen:md5_chunk_size()},
            begin
                Context = crypto:md5_init(),
                ChunkedMd5Sum = crypto:md5_final(
                                  riak_cs_utils:chunked_md5(DataBlob,
                                                            Context,
                                                            ChunkSize)),
                Md5Sum = crypto:md5(DataBlob),
                equals(ChunkedMd5Sum, Md5Sum)
            end).

%%====================================================================
%% Helpers
%%====================================================================

test() ->
    test(500).

test(Iterations) ->
    eqc:quickcheck(eqc:numtests(Iterations, prop_chunked_md5())).

-endif.
