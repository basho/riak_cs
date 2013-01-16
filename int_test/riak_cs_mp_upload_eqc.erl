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

-define(DICTMODULE, orddict).

%% Constants

-define(BUCKET, <<"riak_cs_mp_upload_eqc">>).
-define(KEY, <<"test">>).
-define(CTYPE, <<"applicaton/octet-stream">>).
-define(USER, {"display_name",
               "329c332b9eff49899c2c6e5185de7fd97cdc6d646c55d3e84ef19006c746c368",
               "LAHU4GBJIRQD55BJNET7"}).

%% Types

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
-type pids()        :: list(pid()).

-record(state, {initiated   :: upload_ids(),
                uploaded    :: uploaded(),
                completed   :: completes(),
                aborted     :: aborts(),
                pids        :: pids()}).

initial_state() ->
    {ok, Pid} = riak_cs_utils:riak_connection(),
    #state{initiated    = [],
           uploaded     = ?DICTMODULE:new(),
           completed    = [],
           aborted      = [],
           pids         = [Pid]}.

next_state(S=#state{initiated=Initiated}, {ok, UploadID},
           {call, riak_cs_mp_utils, initiate_multipart_upload, _Args}) ->
    S#state{initiated=[UploadID | Initiated]}.

command(S) ->
    %% * initiate
    %% * upload part
    %% * complete
    %% * abort
    %% * read
    oneof([initiate(S)]).

%% commands broken out for ease of reading
initiate(#state{pids=Pids}) ->
    {call, riak_cs_mp_utils, initiate_multipart_upload,
     [?BUCKET, ?KEY, ?CTYPE, ?USER, [], elements(Pids)]}.

preconditions(_S, _Call, _Result) ->
    true.

postcondition(_S, _Call, _Result) ->
    true.

prop_mp_upload() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
            {_H, _S, Res} = run_commands(?MODULE, Cmds),
            Res==ok
            end).

%% Helpers

add_part_to_dict(Dict, Key, Part) ->
    Fun = fun(List) -> [Part | List] end,
    ?DICTMODULE:update(Key, Fun, [], Dict).
