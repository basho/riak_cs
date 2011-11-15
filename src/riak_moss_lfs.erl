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
    case FileSize rem BlockSize of
        0 ->
            BlockCount = FileSize div BlockSize;
        _ ->
            BlockCount = (FileSize div BlockSize) + 1
    end,
    %% Punting on optimization where no extra blocks are created if
    %% file size <= block size.
    Blocks = [list_to_binary(FileName ++ UUID ++ integer_to_list(X)) ||
                 X <- lists:seq(1,BlockCount+1)],
    %% Assemble the metadata
    MDDict = dict:from_list(MetaData ++
                                [{file_size, FileSize},
                                 {block_size, BlockSize},
                                 {blocks_missing, Blocks},
                                 {alive, false},
                                 {uuid, UUID}]),

    %% Punting on proper connection handling for now
    case riakc_pb_socket:start_link("127.0.0.1", 8087) of
        {ok, Pid} ->
            Obj = riakc_obj:new(Bucket,
                                list_to_binary(FileName),
                                UUID),
            NewObj = riakc_obj:update_metadata(Obj, dict:to_list(MDDict)),
            case riakc_pb_socket:put(Pid, NewObj, [return_body]) of
                {ok, NewObj} ->
                    %% Get the vector clock
                    VClock = ok,
                    put(Pid, FileName, VClock, BlockSize, Blocks, Data);
                {ok, _} ->
                    %% Handle conflict
                    ok;
                {error, _Reason1} ->
                    ok
            end;
        {error, Reason} ->
            lager:error("Couldn't connect to Riak: ~p", [Reason])
    end.

put(_Pid, _FileName, _VClock, _BlockSize, [], _) ->
    %% Mark file as available
    ok;
put(Pid, FileName, VClock, BlockSize, [_Block | RestBlocks], Data) ->
    <<_BlockData:BlockSize/binary, RestData/binary>> = Data,
    %% Write block
    %% Update root metadata
    put(Pid, FileName, VClock, BlockSize, RestBlocks, RestData).

%% get(FileName) ->

