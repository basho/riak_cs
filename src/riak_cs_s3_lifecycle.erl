%%%-------------------------------------------------------------------
%%% @author UENISHI Kota <kota@basho.com>
%%% @copyright (C) 2014, UENISHI Kota
%%% @doc
%%%
%%% @end
%%% Created :  4 Sep 2014 by UENISHI Kota <kota@basho.com>
%%%-------------------------------------------------------------------
-module(riak_cs_s3_lifecycle).

-behaviour(yoyaku_stream).

%% exported APIs
-export([schedule_lifecycle/1]). %%, checkout/0, checkin/1]).

%% yoyaku_stream callbacks
-export([init/1, handle_invoke/2, merge/2,
         report_batch/1, terminate/1]).

%% schedules deletion at n days later
schedule_lifecycle({_UUID, _Manifest}) ->
    %% both deletion and archival
    R = yoyaku:do(lifecycle_collector, poom, spam, []),
    lager:debug("scheduled Yoyaku 'poom': ~p.", [R]),
    ok.

%% checkout() ->
%%     {ok, RcPid} = riak_cs_riak_client:checkout(),
%%     riak_cs_riak_client:master_pbc(RcPid).

%% checkin(RcPid) ->
%%     riak_cs_riak_client:checkin(RcPid).
    

%% == yoyaku_stream callbacks ==
init(_Options) -> {ok, state}.
handle_invoke(Poom, state) ->
    lager:debug("processed ~p", [Poom]),
    {ok, result}.
merge(result, result) -> result.
report_batch(Result) ->
    lager:debug("finished :) ~p", [Result]).
terminate(state) -> ok.
%% == yoyaku_stream callbacks end ==
