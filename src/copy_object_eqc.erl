-module(copy_object_eqc).

%% API
-export([run/0]).

-ifndef(EQC).

run() ->
    {error, eqc_not_installed}.

-else.

-include_lib("eqc/include/eqc.hrl").

-define(TEST_ITERATIONS, 1).
-define(QC_OUT(P),
    eqc:on_output(fun(Str, Args) ->
                io:format(user, Str, Args) end, P)).

-include_lib("riak_cs.hrl").

-include_lib("eunit/include/eunit.hrl").

%% keys for non-multipart objects
-define(SRC_BUCKET, "copy-object-test-bucket-src").
-define(DST_BUCKET, "copy-object-test-bucket-dst").


run() ->
    run_eqc_tests(),
    pass.

run_eqc_tests() ->
    ?assertEqual(true,
        eqc:quickcheck(?QC_OUT(eqc:numtests(?TEST_ITERATIONS, prop_copy())))).

%% ====================================================================
%% eqc property
%% ====================================================================
prop_copy() ->
    ?FORALL({CopyCtx, Data, PutArgs}, copy_gen(),
        begin
            {ok, SrcManifest} = put_data(Data, PutArgs),
            NewCopyCtx = CopyCtx#copy_ctx{src_manifest=SrcManifest},
            ?assertEqual(ok, riak_cs_copy_object:copy(NewCopyCtx)),
            {ok, RetrievedData} = get_data(NewCopyCtx, crypto:hash(md5, Data)),
            io:format("length(Data) = ~p~n", [length(Data)]),
            RetrievedData =:= Data
        end).

%% ====================================================================
%% Internal Functions
%% ====================================================================
get_data(#copy_ctx{dst_bucket=Bucket, 
                   dst_key=Key}, MD5) ->
    FetchConcurrency = riak_cs_lfs_utils:fetch_concurrency(),
    BufferFactor = riak_cs_lfs_utils:get_fsm_buffer_size_factor(),
    {ok, RiakcPid} = riak_cs_utils:riak_connection(),
    {ok, Pid} = riak_cs_get_fsm_sup:start_get_fsm(node(), Bucket, Key, self(),
       RiakcPid, FetchConcurrency, BufferFactor),
    Result = get_chunks(Pid, MD5, <<>>),
    riak_cs_utils:close_riak_connection(RiakcPid),
    Result.

get_chunks(Pid, MD5, Data) ->
    case riak_cs_get_fsm:get_next_chunk(Pid) of
        {done, <<>>} ->
            {ok, Data};
        {chunk, Chunk} ->
            get_chunks(Pid, MD5, <<Data/binary, Chunk/binary>>)
    end.

put_data(Data, PutArgs) ->
    {ok, RiakcPid} = riak_cs_utils:riak_connection(),
    Args = erlang:append_element(PutArgs, RiakcPid),
    {ok, Pid} = riak_cs_put_fsm_sup:start_put_fsm(node(), Args),
    riak_cs_put_fsm:augment_data(Pid, Data),
    riak_cs_put_fsm:finalize(Pid, crypto:hash(md5, Data)),
    riak_cs_utils:close_riak_connection(RiakcPid).

%% ====================================================================
%% EQC Generators
%% ====================================================================
-define(keyid, <<"somekeyid">>).
-define(display_name, <<"Jimmy">>).
-define(canonical_id, <<"12345-jimmy">>).

copy_gen() ->
    CopyCtx = copy_ctx(),
    DataSize = data_size(),
    Data = binary(DataSize),
    Acl = acl(),
    {CopyCtx, Data, put_args(DataSize, Acl)}.

put_args(DataSize, Acl) ->
    {?SRC_BUCKET, key(), DataSize, <<"application/binary">>, 
    [], ?DEFAULT_LFS_BLOCK_SIZE, Acl, timer:seconds(60), self()}.

acl() ->
    ?ACL{owner={?display_name, ?canonical_id},
         grants=['FULL_CONTROL'],
         creation_time=os:timestamp()}.

data_size() ->
    random:uniform(10000).

dst_bucket() ->
    <<?DST_BUCKET>>.

metadata() ->
    [].

key() ->
    binary(10).

copy_ctx() ->
    #copy_ctx{src_manifest=undefined,
              dst_bucket=dst_bucket(),
              dst_key=key(),
              dst_metadata=metadata(),
              dst_acl=acl()}.

-endif.
