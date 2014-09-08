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
-export([schedule_lifecycle/3, status/0]). %%, checkout/0, checkin/1]).

%% yoyaku_stream callbacks
-export([init/1, handle_invoke/2, merge/2,
         report_batch/1, terminate/1]).

%% lifecycle batch state
-record(state, {
          keys_processed = 0 :: non_neg_integer(),
          keys_failed = 0    :: non_neg_integer()
         }).

%% schedules deletion at n days later
schedule_lifecycle(UUID, Manifest, _Lifecycle) ->
    %% if _Lifecycle is about TTL => {delete, ...}
    %% else if is about Archival => {archive, ...}
    Lifecycle = {delete, UUID, Manifest}, %% delete 30 days later
    %% both deletion and archival
    %% delete 30 seconds later
    R = yoyaku:do(lifecycle_collector, Lifecycle, 30, []),
    _ = lager:debug("scheduled Yoyaku '~p': ~p.", [Lifecycle, R]),
    ok.

status() ->    
    yoyaku_d:status(yoyaku_d_lifecycle_collector).

%% == yoyaku_stream callbacks ==
init(_Options) -> {ok, #state{}}.
handle_invoke(Lifecycle, State) ->
    lager:debug("processed ~p", [Lifecycle]),
    {ok, RcPid} = riak_cs_riak_client:checkout(),
    try
        case Lifecycle of
            {delete, UUID, Manifest} ->
                process_delete(RcPid, UUID, Manifest, State);
            _ ->
                {error, bad_lifecycle}
        end
    after
        riak_cs_riak_client:checkin(RcPid)
    end.

merge(#state{keys_processed=L}, #state{keys_processed=R}) ->
    #state{keys_processed=L+R}.

report_batch(#state{keys_processed=P}) ->
    lager:debug("finished :) ~p keys processed.", [P]).

terminate(_) -> ok.
%% == yoyaku_stream callbacks end ==

process_delete(RcPid, UUID, Manifest, State = #state{keys_processed=P}) ->
    {Bucket,Key} = riak_cs_manifest:bkey(Manifest),
    case riak_cs_manifest:get_manifests(RcPid, Bucket, Key) of
        {ok, RiakObject, _} ->
            case riak_cs_gc:gc_specific_manifests([UUID], RiakObject,
                                                  Bucket, Key, RcPid) of
                {ok, _} ->
                    {ok, State#state{keys_processed=P+1}};
                Error ->
                    Error
            end;
        {error, _} = E ->
            E
    end.
    
