%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Large file storage interface

-module(riak_moss_lfs).

-include_lib("riakc/include/riakc_obj.hrl").

-compile(export_all).

-define(DEFAULT_BLOCK_SIZE, 1000).

%% ===================================================================
%% Public API
%% ===================================================================

-spec create(string(), string(), pos_integer(), binary(), list()) ->
                    {ok, binary()} | {error, term()}.
create(Bucket, FileName, FileSize, Data, MetaData) ->
    create(Bucket, FileName, FileSize, ?DEFAULT_BLOCK_SIZE, Data, MetaData).

-spec create(string(), string(), pos_integer(), pos_integer(), binary(), list()) ->
                    {ok, binary()} | {error, term()}.
create(Bucket, FileName, FileSize, BlockSize, Data, MetaData) ->
    %% @TODO Use actual UUID
    UUID = riak_moss:unique_hex_id(),
    %% Calculate the number of blocks
    BlockCount = block_count(FileSize, BlockSize),

    %% Punting on optimization where no extra blocks are created if
    %% file size < or ~= to block size.
    Blocks = [list_to_binary(FileName ++ ":" ++ UUID ++ ":" ++ integer_to_list(X)) ||
                 X <- lists:seq(1,BlockCount)],
    lager:debug("Blocks for ~p: ~p", [FileName, Blocks]),
    %% Assemble the metadata
    MDDict = dict:from_list([{?MD_USERMETA, MetaData ++
                                [{"file_size", FileSize},
                                 {"block_size", BlockSize},
                                 {"blocks_missing", term_to_binary(Blocks)},
                                 {"alive", false},
                                 {"uuid", UUID}]}]),

    %% Punting on connection reuse for now
    %% Just open a new connection each time.
    case riakc_pb_socket:start_link("127.0.0.1", 8087) of
        {ok, Pid} ->
            Obj = riakc_obj:new(list_to_binary(Bucket),
                                list_to_binary(FileName)),
            NewObj = riakc_obj:update_metadata(
                       riakc_obj:update_value(Obj, list_to_binary(UUID)), MDDict),
            case riakc_pb_socket:put(Pid, NewObj) of
                ok ->
                    {ok, StoredObj} = riakc_pb_socket:get(Pid, list_to_binary(Bucket), list_to_binary(FileName)),
                    put(Pid, Bucket, FileName, StoredObj, BlockSize, Blocks, Data);
                {error, _Reason1} ->
                    ok
            end;
        {error, Reason} ->
            lager:error("Couldn't connect to Riak: ~p", [Reason])
    end.

put(Pid, _Bucket, FileName, RootObj, _BlockSize, [], _) ->
    %% Mark file as available
    RootMD = riakc_obj:get_metadata(RootObj),
    UserMD = dict:from_list(dict:fetch(?MD_USERMETA, RootMD)),
    case binary_to_term(list_to_binary(dict:fetch("blocks_missing", UserMD))) of
        [] ->
            UserMD1 = dict:store("alive", true, UserMD),
            UpdRootObj =
                riakc_obj:update_metadata(RootObj,
                                          dict:store(?MD_USERMETA, dict:to_list(UserMD1), RootMD)),
            case riakc_pb_socket:put(Pid, UpdRootObj, [return_body]) of
                {ok, _StoredRootObj} ->
                    lager:info("~p is available", [FileName]),
                    ok;
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
    UserMD = dict:from_list(dict:fetch(?MD_USERMETA, RootMD)),
    [_ | RemainingBlocks] = binary_to_term(list_to_binary(dict:fetch("blocks_missing", UserMD))),
    UserMD1 = dict:store(<<"blocks_missing">>, term_to_binary(RemainingBlocks), UserMD),
    UpdRootObj =
        riakc_obj:update_metadata(RootObj,
                                  dict:store(?MD_USERMETA, dict:to_list(UserMD1), RootMD)),
    case riakc_pb_socket:put(Pid, UpdRootObj, [return_body]) of
        {ok, StoredRootObj} ->
            put(Pid, Bucket, FileName, StoredRootObj, BlockSize, RestBlocks, RestData);
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
                    RootMD = riakc_obj:get_metadata(Obj),
                    MD = dict:from_list(dict:fetch(?MD_USERMETA, RootMD)),
                    UUID = dict:fetch("uuid", MD),
                    FileSize = list_to_integer(dict:fetch("file_size", MD)),
                    BlockSize = list_to_integer(dict:fetch("block_size", MD)),
                    BlockCount = block_count(FileSize, BlockSize),
                    [begin
                         Block = list_to_binary(FileName ++ ":" ++ UUID ++ ":" ++ integer_to_list(X)),
                         {ok, BlockObj} = riakc_pb_socket:get(Pid, Bucket, Block),
                         riakc_obj:get_value(BlockObj)
                     end ||
                        X <- lists:seq(1,BlockCount)];
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
        ok ->
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
