%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------


-module(riak_cs_mp_upload_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-compile(export_all).
-compile({parse_transform, eqc_group_commands}).

-define(NUMTESTS, 100).
-define(DICTMODULE, orddict).

%% -------------------------------------------------------------------
%% Constants
%% -------------------------------------------------------------------

-define(BUCKET, <<"riak_cs_mp_upload_eqc">>).
-define(KEY, <<"test">>).
-define(CTYPE, <<"applicaton/octet-stream">>).
-define(USER, {"display_name",
               "329c332b9eff49899c2c6e5185de7fd97cdc6d646c55d3e84ef19006c746c368",
               "LAHU4GBJIRQD55BJNET7"}).

%% -------------------------------------------------------------------
%% Types
%% -------------------------------------------------------------------

-type upload_id()   :: string().
-type upload_ids()  :: list(upload_id()).
-type part_id()     :: pos_integer().
-type etag()        :: string().
-type value()       :: binary().
-type part()        :: {part_id(), etag(), value()}.
-type parts()       :: list(part()).
-type uploaded()    :: ?DICTMODULE:?DICTMODULE(upload_id(), parts()).
-type complete()    :: {upload_id(), parts()}.
-type completes()   :: list(complete()).
-type aborts()      :: list(upload_id()).
%%-type pids()        :: list(pid()).

-record(state, {initiated   :: upload_ids(),
                uploaded    :: uploaded(),
                completed   :: completes(),
                aborted     :: aborts()}).

%% -------------------------------------------------------------------
%% Statem Callbacks
%% -------------------------------------------------------------------

initial_state() ->
    #state{initiated    = [],
           uploaded     = ?DICTMODULE:new(),
           completed    = [],
           aborted      = []}.

next_state(S=#state{initiated=Initiated}, UploadID,
           {call, ?MODULE, initiate_multipart_upload_wrapper, _Args}) ->
    S#state{initiated=[UploadID | Initiated]};
next_state(S=#state{uploaded=Uploaded}, MD5,
           {call, ?MODULE, upload_part_wrapper, [UploadID, PartNumber, Blob]}) ->
    NewUploaded = add_part_to_dict(Uploaded, UploadID, {PartNumber, MD5, Blob}),
    S#state{uploaded=NewUploaded}.

command(S) ->
    %% * initiate [x]
    %% * upload part [x]
    %% * complete
    %% * abort
    %% * read

    oneof([initiate(S)] ++ [upload_part(S) || [] =/= S#state.initiated]).

%% -------------------------------------------------------------------
%% Initiate

%% commands broken out for ease of reading
initiate(_S) ->
    {call, ?MODULE, initiate_multipart_upload_wrapper, [{var, pids}, nat()]}.

initiate_multipart_upload_wrapper(Pids, Nat) ->
    PidIndex = (Nat * length(Pids)) rem length(Pids),
    Pid = lists:nth(PidIndex + 1, Pids),
    {ok, Id} = riak_cs_mp_utils:initiate_multipart_upload(?BUCKET, ?KEY, ?CTYPE, ?USER, [], Pid),
    Id.

%% -------------------------------------------------------------------
%% Upload Part

upload_part(#state{initiated=UploadIDs}) ->
    %% need to somehow filter out the upload
    %% ids that are not in the aborted state,
    %% at least at first
    UploadID = elements(UploadIDs),
    Value = non_empty_binary(),
    PartID = nat(),
    {call, ?MODULE, upload_part_wrapper, [UploadID, PartID, Value]}.

upload_part_wrapper(UploadId, PartNumber, Blob) ->
    Size = byte_size(Blob),
    {upload_part_ready, PartUUID, PutPid} =
        riak_cs_mp_utils:upload_part(?BUCKET, ?KEY, UploadId, PartNumber, Size, ?USER),
    {ok, MD5} = riak_cs_mp_utils:upload_part_1blob(PutPid, Blob),
    ok = riak_cs_mp_utils:upload_part_finished(?BUCKET, ?KEY, UploadId,
                                               PartNumber, PartUUID, MD5, ?USER),
    MD5.

%% -------------------------------------------------------------------
%% (Pre|Post)conditions

precondition(_S, _Call) ->
    true.

postcondition(_S, _Call, _Result) ->
    true.

%% -------------------------------------------------------------------
%% Properties
%% -------------------------------------------------------------------

prop_mp_upload() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
            %%{ok, Pid} = riak_cs_utils:riak_connection(),
            %%ok = application:start(poolboy),
            %%ok = application:start(riak_cs),
            {ok, Pid} = riakc_pb_socket:start_link("localhost", 8087, []),
            R={_H, _S, Res} = run_commands(?MODULE, Cmds, [{pids, [Pid]}]),
            pretty_commands(?MODULE, Cmds, R, Res==ok)
            end).

%% -------------------------------------------------------------------
%% Test Helpers

test() ->
    test(?NUMTESTS).

test(NumTests) ->
    quickcheck(numtests(NumTests,
                        prop_mp_upload())).

%% -------------------------------------------------------------------
%% Test Start Helpers

start_deps() ->
    application:start(inets),
    application:start(folsom),
    application:start(crypto),
    application:start(mochiweb),
    application:start(webmachine),
    application:start(lager),
    application:start(poolboy),
    application:set_env(riak_cs, connection_pools, [{request_pool, {5, 0}}]),
    application:start(riak_cs).

stop_deps() ->
    application:stop(riak_cs),
    application:unset_env(riak_cs, connection_pools),
    application:stop(poolboy),
    application:stop(lager),
    application:stop(webmachine),
    application:stop(mochiweb),
    application:stop(crypto),
    application:stop(folsom),
    application:stop(inets).

%% -------------------------------------------------------------------
%% Generators
%% -------------------------------------------------------------------

non_empty_binary() ->
    ?SUCHTHAT(X, binary(), X =/= <<>>).

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

add_part_to_dict(Dict, Key, Part) ->
    Fun = fun(List) -> [Part | List] end,
    ?DICTMODULE:update(Key, Fun, [], Dict).
