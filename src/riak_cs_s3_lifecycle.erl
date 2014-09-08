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
-include_lib("xmerl/include/xmerl.hrl").
-include("s3_api.hrl").
-include("riak_cs.hrl").
-include_lib("eunit/include/eunit.hrl").

%% exported APIs
-export([maybe_handle_lifecycle/3,
         status/0,
         from_xml/1,
         fetch_bucket_lifecycle/2,
         schedule_restore/3,
         restore/3, restore/4]). %%, checkout/0, checkin/1]).

%% yoyaku_stream callbacks
-export([init/1, handle_invoke/2, merge/2,
         report_batch/1, terminate/1]).

%% lifecycle batch state
-record(state, {
          keys_processed = 0 :: non_neg_integer(),
          keys_failed = 0    :: non_neg_integer()
         }).

maybe_handle_lifecycle(UUID, Manifest, BucketObj) ->
    case riak_cs_bucket:get_bucket_properties(BucketObj, lifecycle) of
        {ok, LifecycleBin} ->
            {ok, Lifecycle} = riak_cs_s3_lifecycle:from_xml(LifecycleBin),
            ?debugVal(Lifecycle),
            [begin
                 ?debugVal(Rule),
                 handle_rule(UUID, Manifest, Rule)
             end || Rule <- Lifecycle#lifecycle_config_v1.rules];
        {error, property_not_defined} ->
            ok;
        Error ->
            lager:error("can't retrieve bucket lifecycle other than non-existence: ~p", [Error])
    end.

handle_rule(UUID, Manifest, #lifecycle_rule_v1{prefix=Prefix, id=ID} = Rule) ->
    {_Bucket, Key} = riak_cs_manifest:bkey(Manifest),
    %% compare Key and Prefix
    %% 
    %% ok = riak_cs_s3_lifecycle:schedule_lifecycle(UUID, Manifest0, delete),
    %%ok = riak_cs_s3_lifecycle:schedule_lifecycle(UUID, Manifest0, archive),
    Len = byte_size(Prefix),
    case binary:longest_common_prefix([Prefix, Key]) of
        Len ->
            case Rule#lifecycle_rule_v1.transition of
                undefined -> pass;
                Transition ->
                    schedule_transition(UUID, Manifest, Transition)
            end,
            case Rule#lifecycle_rule_v1.expiration of
                undefined -> pass;
                Days ->
                    schedule_expiration(UUID, Manifest, Days)
            end,
            lager:debug("handle_rule... ~p", [Rule]);
        _ ->
            lager:debug("~p doesn't match prefix ~p of rule ~p",
                        [Key, Prefix, ID])
    end.

%% schedules deletion at n days later
schedule_transition(UUID,
                    Manifest,
                    #lifecycle_transition_v1{
                       days = TransitionDate,
                       storage_class = cold_storage} = _Transition) ->

    Lifecycle = {archive, UUID, Manifest},

    case Manifest?MANIFEST.created of
        CreationDate when CreationDate >= TransitionDate ->
            %% do it right now
            R = yoyaku:do(lifecycle_collector, Lifecycle, 1, []),
            _ = lager:debug("scheduled Yoyaku '~p': ~p.", [Lifecycle, R]);

        _ ->
            %% do it later
            Now = riak_cs_wm_utils:iso_8601_datetime(),
            ReservationTime = TransitionDate - Now,
            R = yoyaku:do(lifecycle_collector, Lifecycle, ReservationTime, []),
            _ = lager:debug("scheduled Yoyaku '~p': ~p.", [Lifecycle, R])
    end.

schedule_expiration(UUID, Manifest, Days)
  when is_integer(Days) andalso Days > 0 ->

    Delay = 86400 * Days,
    Lifecycle = {delete, UUID, Manifest},
    R = yoyaku:do(lifecycle_collector, Lifecycle, Delay, []),
    _ = lager:debug("scheduled Yoyaku '~p': ~p.", [Lifecycle, R]),
    R.

schedule_block_expiration(UUID, Manifest, Days)
  when is_integer(Days) andalso Days > 0 ->
    Delay = 86400 * Days,
    Lifecycle = {block_delete, UUID, Manifest},
    R = yoyaku:do(lifecycle_collector, Lifecycle, Delay, []),
    _ = lager:debug("scheduled Yoyaku '~p': ~p.", [Lifecycle, R]),
    R.

schedule_restore(UUID, Manifest, Days)
  when is_integer(Days) andalso Days > 0 ->
    Restore = {restore, UUID, Manifest, Days},
    %% Start in 42 seconds later
    R = yoyaku:do(lifecycle_collector, Restore, 42, []),
    _ = lager:debug("scheduled Restore ~p", [Restore]),
    R.

status() ->
    yoyaku_d:status(yoyaku_d_lifecycle_collector).

-spec from_xml(string()) -> lifecycle_config(). 
from_xml(XML) when is_binary(XML) ->
    from_xml(unicode:characters_to_list(XML));
from_xml(XML) when is_list(XML) ->
    case riak_cs_xml:scan(XML) of
        {ok, #xmlElement{name='LifecycleConfiguration',
                         parents = [],
                         content=RulesAsXML}} ->
            case parse_lifecycle_rules(RulesAsXML, []) of
                {ok, Rules} ->
                    %% maybe requies Rules validation
                    %% to check undefined members in the record
                    {ok, #lifecycle_config_v1{rules=Rules}};
                Error ->
                    Error
            end;
        {ok, #xmlElement{} = _E} ->
            lager:debug("suspicious XML: ~p", [_E]),
            {error, malformed_xml};
        Error ->
            Error
    end.

parse_lifecycle_rules([], Rules) -> {ok, lists:reverse(Rules)};
parse_lifecycle_rules([#xmlElement{name='Rule',
                                   content=Content} = _RuleAsXML|L],
                      Rules) ->

    Rule = lists:foldl(fun(E,A) -> parse_lifecycle_rule(E, A) end,
                       #lifecycle_rule_v1{}, Content),
    parse_lifecycle_rules(L, [Rule|Rules]);

parse_lifecycle_rules([H|L], Rules) ->
    lager:debug("unknown bad xml: ~p", [H]),
    parse_lifecycle_rules(L, Rules).

parse_lifecycle_rule(#xmlElement{name='ID',
                                 content=R}, Rule) ->
    Bin=unicode:characters_to_binary(get_text(R)),
    Rule#lifecycle_rule_v1{id=Bin};
parse_lifecycle_rule(#xmlElement{name='Prefix',
                                 content=R}, Rule) ->
    Prefix=unicode:characters_to_binary(get_text(R)),
    Rule#lifecycle_rule_v1{prefix=Prefix};
parse_lifecycle_rule(#xmlElement{name='Status',
                                 content=R}, Rule) ->
    Status = case get_text(R) of
                 "Enabled" -> enabled;
                 "Disabled" -> diabled
             end,
    Rule#lifecycle_rule_v1{status=Status};
parse_lifecycle_rule(#xmlElement{name='Transition',
                                 content=R}, Rule) ->
    Days = %% Date in ISO8601 format
        case lists:keyfind('Days', 2, R) of
            #xmlElement{content=C} -> list_to_integer(get_text(C));
            _ -> undefined
        end,
    StorageClass =
        case lists:keyfind('StorageClass', 2, R) of
            #xmlElement{content=C2} = _ ->
                case get_text(C2) of
                    "GLACIER" -> cold_storage;
                    _ -> undefined
                end;
            _ -> undefined
        end,
    T = #lifecycle_transition_v1{days=Days,
                                 storage_class=StorageClass},
    Rule#lifecycle_rule_v1{transition=T};
parse_lifecycle_rule(#xmlElement{name='Expiration',
                                 content=R}, Rule) ->
    #xmlElement{content=C} = lists:keyfind('Days', 2, R),
    Days = list_to_integer(get_text(C)),
    Rule#lifecycle_rule_v1{expiration=Days};
parse_lifecycle_rule(#xmlText{value=" "}, Rule) ->
    Rule.

get_text([]) -> "";
get_text([#xmlText{value=" "}|L]) -> get_text(L);
get_text([#xmlText{value=S}|_]) -> S.
                

fetch_bucket_lifecycle(Bucket, RcPid) ->
    riak_cs_bucket:get_bucket_properties(Bucket, lifecycle, RcPid).

%% == yoyaku_stream callbacks ==
init(_Options) -> {ok, #state{}}.

handle_invoke(Lifecycle, State) ->
    lager:debug("processed ~p", [Lifecycle]),
    {ok, RcPid} = riak_cs_riak_client:checkout(),
    try
        case Lifecycle of
            {delete, UUID, Manifest} ->
                process_delete(RcPid, UUID, Manifest, State);
            {archive, UUID, Manifest} ->
                %% if the UUID is not the latest, then we'd delete it
                %% as versioning is not yet ready
                process_archive(RcPid, UUID, Manifest, State);
            {restore, UUID, Manifest, Days} ->
                P = State#state.keys_processed,
                case restore(RcPid, UUID, Manifest, Days) of
                    ok -> {ok, State#state{keys_processed=P+1}};
                    Error -> Error
                end;
            {block_delete, UUIDb, Manifest} ->
                P = State#state.keys_processed,
                case application:get_env(riak_cs, archive_dir) of
                    {ok, disabled} ->
                        lager:error("lifecycle archival is disabled while this cluster used to have it enabled"),
                        {error, bad_lifecycle_data};
                    {ok, Dir} when is_list(Dir) ->
                        Filename = archive_filename(Dir, Manifest, UUIDb),
                        lager:debug("block_delete; Filename is ~p / UUIDb is ~p",
                                    [Filename, mochihex:to_hex(UUIDb)]),
                        case delete_blocks_and_mark_as_archived(RcPid, Manifest, Filename) of
                            ok -> {ok, State#state{keys_processed=P+1}};
                            Error -> Error
                        end
                end;

            Other ->
                lager:debug("unknown lifecycle yoyaku found; ~p", [Other]),
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

process_archive(RcPid, UUID, Manifest0, State = #state{keys_processed=P}) ->
    %% TODO: if UUID is not the latest just delete it away. When it
    %% comes to versioning, it'll be a little bit grumpy
    
    %% spin up get_fsm
    {Bucket,Key} = riak_cs_manifest:bkey(Manifest0),
    case application:get_env(riak_cs, archive_dir) of
        undefined ->
            lager:error("Archival directory is not configured");
        {ok, disabled} ->
            lager:error("Archival directory is not configured");
        {ok, Dir} when is_list(Dir) ->
            Filename = filename:join([Dir, Bucket, Key, mochihex:to_hex(UUID)]),
            ok = filelib:ensure_dir(Filename),
            {ok, GetFsmPid} = riak_cs_get_fsm_sup:start_get_fsm(node(),
                                                                Bucket,
                                                                Key,
                                                                self(),
                                                                RcPid,
                                                                1,
                                                                1),
            lager:debug(">>> ~p ~p", [?FILE, ?LINE]),
            case riak_cs_get_fsm:get_manifest(GetFsmPid) of
                notfound ->
                    lager:debug("manifest for ~s/~s not found.", [Bucket, Key]);
                
                %% TODO: check manifest state, is it still writing, dead or ready?
                %% TODO: AND handle errors
                Manifest = ?MANIFEST{state=active} ->
                    ContentSize = riak_cs_manifest:content_length(Manifest),
                    ok = riak_cs_get_fsm:continue(GetFsmPid, {0, ContentSize-1}),
                    lager:debug(">>> ~p ~p", [?FILE, ?LINE]),
                    %% 0. TODO: think of race condition
                    %% 1. Check whether still the UUID is latest and active
                    %% 2. open up a file and write it down
                    {ok, IoDevice} = file:open(Filename, [binary, write]),
                    ok = copy_all(GetFsmPid, IoDevice),
                    ok = file:close(IoDevice),
                    ok = riak_cs_get_fsm:stop(GetFsmPid),

                    ok = delete_blocks_and_mark_as_archived(RcPid, Manifest0, Filename),

                    lager:debug("Archived ~s/~s to ~s", [Bucket, Key, Filename]);
                _Manifest = ?MANIFEST{state=State} ->
                    lager:warning("Object is not ready to archive: ~p ~s/~s (~p)",
                                  [State, Bucket, Key, UUID])
            end,
            {ok, State#state{keys_processed=P+1}};
        _ ->
            lager:error("Archival directory is not configured")
    end.

delete_blocks_and_mark_as_archived(RcPid, Manifest0, Filename) ->
    {Bucket,Key} = riak_cs_manifest:bkey(Manifest0),
    UUID = riak_cs_manifest:uuid(Manifest0),
    lager:debug(">>> ~p ~p", [?FILE, ?LINE]),
    %% 3. move mark as archived in `props`
    {ok, ManiPid} = riak_cs_manifest_fsm:start_link(Bucket, Key, RcPid),
    {ok, ActiveMani} = riak_cs_manifest_fsm:get_active_manifest(ManiPid),
    lager:debug(">>> ~p ~p", [?FILE, ?LINE]),
    Props = ActiveMani?MANIFEST.props,
    NewActiveManifest = ActiveMani?MANIFEST{props=[{archiving, Filename}|Props]},
    ok = riak_cs_manifest_fsm:update_manifest(ManiPid, NewActiveManifest),
    ok = riak_cs_manifest_fsm:stop(ManiPid),
    
    lager:debug(">>> ~p ~p", [?FILE, ?LINE]),
    %% 4. and delete all blocks
    {ok, BSPid} = riak_cs_delete_fsm:start_link(RcPid, 
                                                {UUID, NewActiveManifest},
                                                self(),
                                                "dummy", [{archived, Filename}]),
    wait_for_block_deletion(BSPid, UUID).

wait_for_block_deletion(BSPid, UUID) ->
    lager:info("Waiting for ~p", [BSPid]),
    receive
        %% delete_fsm internally updates state to archived
        {block_delete_done, UUID} ->
            lager:info("block deletion and manifest update done ~p", [UUID]),
            ok;
        ping ->
            lager:debug(">>> ~p ~p ~p", [?FILE, ?LINE, ping]),
            wait_for_block_deletion(BSPid, UUID)            
    end.

%% for console testing
restore(UUID, B, K) when is_list(UUID) ->
    UUIDBin = mochihex:to_bin(UUID),
    Bucket = list_to_binary(B),
    Key = list_to_binary(K),
    %% TODO: multibag, change this to badid
    {ok, RcPid} = riak_cs_utils:riak_connection(request_pool),
    try
        {ok, _, OrdDict} = riak_cs_manifest:get_manifests(RcPid, Bucket, Key),
        Manifest = orddict:fetch(UUIDBin, OrdDict),
        restore(RcPid, UUIDBin, Manifest, 1)
    after
        riak_cs_utils:close_riak_connection(request_pool, RcPid)
    end.

archive_filename(ArchiveDir, Manifest, UUID) ->
    lager:debug("~p ~p: ~p", [?FILE, ?LINE, Manifest?MANIFEST.props]),
    {Bucket,Key} = riak_cs_manifest:bkey(Manifest),
    case Manifest?MANIFEST.props of
        undefined ->
            filename:join([ArchiveDir, Bucket, Key, mochihex:to_hex(UUID)]);
        Props ->
            case proplists:get_value(archived, Props) of
                undefined ->
                    filename:join([ArchiveDir, Bucket, Key, mochihex:to_hex(UUID)]);
                Path ->
                    Path
            end
    end.

%% @doc pull out from file and put back to blocks bucket
restore(RcPid, UUIDa, Manifest0, Days) when is_integer(Days) andalso Days > 0 ->
    {Bucket,Key} = riak_cs_manifest:bkey(Manifest0),
    case application:get_env(riak_cs, archive_dir) of
        undefined ->
            lager:error("Archival directory is not configured");
        {ok, disabled} ->
            lager:error("Archival directory is not configured");
        {ok, Dir} when is_list(Dir) ->
            
            Filename = archive_filename(Dir, Manifest0, UUIDa),
            lager:debug("Filename: ~s / UUID: ~s", [Filename, mochihex:to_hex(UUIDa)]),
            Props = Manifest0?MANIFEST.props,
            lager:debug("Props: ~p", [Props]),

            BlockSize = riak_cs_lfs_utils:block_size(),
            Acl = Manifest0?MANIFEST.acl,
            Metadata = Manifest0?MANIFEST.metadata,

            %% TODO: specify the same UUID and reuse the blocks? oh, that's not immutable.
            %% TODO: timestamps are really brand new, so import from old manifest required
            {ok, PutFsmPid} = riak_cs_put_fsm_sup:start_put_fsm(node(),
                                                                [{Bucket,
                                                                  Key,
                                                                  Manifest0?MANIFEST.content_length,
                                                                  Manifest0?MANIFEST.content_type,
                                                                  Metadata,
                                                                  BlockSize,
                                                                  Acl,
                                                                  timer:seconds(60),
                                                                  self(),
                                                                  RcPid}]),
                                lager:debug(">>> ~p ~p => ~p", [?FILE, ?LINE, noop]),

            %% This could be very slow
            {ok, IoDevice} = file:open(Filename, [binary, read]),
                                lager:debug(">>> ~p ~p => ~p", [?FILE, ?LINE, noop]),
            %% Check metadata and size here...?
            %% If this is done by multipart, this can't be trusted...
            MD5 = binary_to_list(base64:encode(Manifest0?MANIFEST.content_md5)),
            lager:debug(">>> ~p ~p => ~p", [?FILE, ?LINE, MD5]),
            {ok, NewManifest} = copy_all_2(IoDevice, PutFsmPid, <<>>, MD5),

            %% TODO: mark manifest as restored
            OldProps = case Manifest0?MANIFEST.props of
                           undefined -> [];
                           P -> P
                       end,
            NewProps = [{archived, Filename}|proplists:delete(archived, OldProps)],
            {ok, ManiPid} = riak_cs_manifest_fsm:start_link(Bucket, Key, RcPid),
            NewManifest2 = NewManifest?MANIFEST{props=NewProps},
            ok = riak_cs_manifest_fsm:update_manifest_with_confirmation(ManiPid, NewManifest2),
            ok = riak_cs_manifest_fsm:stop(ManiPid),
            
            %% TODO: do yoyaku against restored object again, it shouldn't be ramaining again?
            UUIDb = riak_cs_manifest:uuid(NewManifest2),
            lager:debug("restoring ~s/~s from ~p to ~p succeeded", [Bucket, Key, UUIDa, UUIDb]),

            schedule_block_expiration(UUIDb, NewManifest2, Days)
    end.

%% @doc TODO: this can be much more memory efficient
copy_all_2(IoDevice, PutFsmPid, Binary, MD5) ->
    case file:read(IoDevice, 1024*1024) of
        eof ->
            ok = file:close(IoDevice),
            case byte_size(Binary) of
                0 -> pass;
                _ -> done = augment_data(PutFsmPid, Binary, true)
            end,
            riak_cs_put_fsm:finalize(PutFsmPid, MD5);
        {ok, Data} ->
            %% especially here:
            {ok, Binary2} = augment_data(PutFsmPid, <<Binary/binary, Data/binary>>, false),
            copy_all_2(IoDevice, PutFsmPid, Binary2, MD5)
    end.
                
augment_data(PutFsmPid, Binary, DoFinish) ->           
    BlockSize = riak_cs_lfs_utils:block_size(),
    case byte_size(Binary) of
        L when L >= BlockSize ->
            <<Data:BlockSize/binary, Rest/binary>> = Binary,
            riak_cs_put_fsm:augment_data(PutFsmPid, Data),
            augment_data(PutFsmPid, Rest, DoFinish);
        L when L > 0 andalso DoFinish ->
            riak_cs_put_fsm:augment_data(PutFsmPid, Binary),
            done;
        _ ->
            {ok, Binary}
    end.

%% @doc TODO to make these moving data from left to right stuff
%% like this, or like copy API, much more abstract to reduce the code
copy_all(GetFsmPid, IoDevice) ->
    try
        case riak_cs_get_fsm:get_next_chunk(GetFsmPid) of
            {done, Chunk} ->
                ok = file:write(IoDevice, Chunk);
            
            {chunk, Chunk} ->
                ok = file:write(IoDevice, Chunk),
                copy_all(GetFsmPid, IoDevice)
        end
    catch T:E ->
            lager:error("~p ~p ~p ~p", [?FILE, ?LINE, T, E])
    end.

