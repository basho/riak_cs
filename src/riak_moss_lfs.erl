%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Large file storage interface

-module(riak_moss_lfs).

-compile(export_all).

-define(DEFAULT_BLOCK_SIZE, 1000).

%% ===================================================================
%% Public API
%% ===================================================================

-spec create(string(), string(), pos_integer(), binary(), list()) ->
                    {ok, binary()} | {error, term()}.
create(Bucket, FileName, FileSize, Data, MetaData) ->
    create(Bucket, FileName, FileSize, Data, ?DEFAULT_BLOCK_SIZE, MetaData).

-spec create(string(), string(), pos_integer(), pos_integer(), binary(), list()) ->
                    {ok, binary()} | {error, term()}.
create(Bucket, FileName, FileSize, BlockSize, Data, MetaData) ->
    %% @TODO Use actual UUID
    UUID = riak_moss:unique_hex_id(),
    %% Calculate the number of blocks
    BlockCount = block_count(FileSize, BlockSize),

    %% Punting on optimization where no extra blocks are created if
    %% file size < or ~= to block size.
    Blocks = [list_to_binary(FileName ++ UUID ++ integer_to_list(X)) ||
                 X <- lists:seq(1,BlockCount+1)],
    lager:debug("Blocks for ~p: ~p", [FileName, Blocks]),
    %% Assemble the metadata
    MDDict = dict:from_list(MetaData ++
                                [{file_size, FileSize},
                                 {block_size, BlockSize},
                                 {blocks_missing, Blocks},
                                 {alive, false},
                                 {uuid, UUID}]),

    %% Punting on connection reuse for now
    %% Just open a new connection each time.
    case riakc_pb_socket:start_link("127.0.0.1", 8087) of
        {ok, Pid} ->
            Obj = riakc_obj:new(Bucket,
                                list_to_binary(FileName),
                                UUID),
            NewObj = riakc_obj:update_metadata(Obj, dict:to_list(MDDict)),
            case riakc_pb_socket:put(Pid, NewObj, [return_body]) of
                {ok, NewObj} ->
                    put(Pid, Bucket, FileName, NewObj, BlockSize, Blocks, Data);
                {ok, _} ->
                    %% Error
                    ok;
                {error, _Reason1} ->
                    ok
            end;
        {error, Reason} ->
            lager:error("Couldn't connect to Riak: ~p", [Reason])
    end.

put(Pid, _Bucket, FileName, RootObj, _BlockSize, [], _) ->
    %% Mark file as available
    RootMD = riakc_obj:get_metadata(RootObj),
    case dict:fetch(blocks_missing, RootMD) of
        [] ->
            UpdRootObj =
                riakc_obj:update_metadata(RootObj,
                                          dict:store(alive, true)),
            case riakc_pb_socket:put(Pid, UpdRootObj, [return_body]) of
                {ok, UpdRootObj} ->
                    lager:info("~p is available", [FileName]),
                    ok;
                {ok, _} ->
                    %% Error
                    lager:error("Root object update failed"),
                    {error, file_changed};
                {error, Reason} ->
                    lager:error("Root object update failed: ~p", [Reason]),
                    {error, Reason}
            end;
        _ ->
            lager:error("Failed to store file, missing block"),
            {error, missing_block}
    end;
put(Pid, Bucket, FileName, RootObj, BlockSize, [Block | RestBlocks], Data) ->
    <<_BlockData:BlockSize/binary, RestData/binary>> = Data,
    %% Write block
    put_block(Pid, Bucket, Block, Data),
    %% Update root metadata
    %% @TODO Error handling
    RootMD = riakc_obj:get_metadata(RootObj),
    [_ | RemainingBlocks] = dict:fetch(blocks_missing, RootMD),
    UpdRootObj =
        riakc_obj:update_metadata(RootObj,
                                  dict:store(blocks_missing, RemainingBlocks)),
    case riakc_pb_socket:put(Pid, UpdRootObj, [return_body]) of
        {ok, UpdRootObj} ->
            put(Pid, Bucket, FileName, UpdRootObj, BlockSize, RestBlocks, RestData);
        {ok, _} ->
            %% Error
            lager:error("Root object update failed"),
            {error, file_changed};
        {error, Reason} ->
            lager:error("Root object update failed: ~p", [Reason]),
            {error, Reason}
    end.


get(Bucket, FileName) ->
    %% Punting on connection reuse for now
    %% Just open a new connection each time.
    case riakc_pb_socket:start_link("127.0.0.1", 8087) of
        {ok, Pid} ->
            case riakc_pb_socket:get(Pid, Bucket, FileName, [head]) of
                {ok, Obj} ->
                    MD = riakc_obj:get_metadata(Obj),
                    UUID = dict:fetch(uuid, MD),
                    FileSize = dict:fetch(file_size, MD),
                    BlockSize = dict:fetch(block_size, MD),
                    BlockCount = block_count(FileSize, BlockSize),
                    [begin
                         Block = list_to_binary(FileName ++ UUID ++ integer_to_list(X)),
                         {ok, BlockObj} = riakc_pb_socket:get(Pid, Bucket, Block),
                         riakc_obj:get_value(BlockObj)
                     end ||
                        X <- lists:seq(1,BlockCount+1)];
                {error, Reason} ->
                    lager:error("Error retrieving ~p: ~p", [FileName, Reason]),
                    {error, Reason}
            end;
        {error, Reason} ->
            lager:error("Couldn't connect to Riak: ~p", [Reason])
    end.


%% ===================================================================
%% Internal functions
%% ===================================================================

put_block(Pid, Bucket, BlockKey, Data)->
    Obj = riakc_obj:new(Bucket,
                        BlockKey,
                        Data),
    case riakc_pb_socket:put(Pid, Obj) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

block_count(FileSize, BlockSize) ->
    case FileSize rem BlockSize of
        0 ->
            FileSize div BlockSize;
        _ ->
            (FileSize div BlockSize) + 1
    end.
