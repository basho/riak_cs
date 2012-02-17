-module(basho_bench_quasi_uuid).
-compile(export_all).

key_can_now(Id, BucketName, SimultaneousUploads, ChunksPerFile, ShuffleP) ->
    %% Create the skeleton of the list of keys to put in some random order

    SUs = lists:seq(1, SimultaneousUploads),
    %% Each simulated upload = 1 UUID needed.
    Su2UUID = dict:from_list([{X, uuid16_like()} || X <- SUs]),
    %% Ts = all combinations of block # and UUID.
    Ts = lists:flatten([[{ChunkNum, dict:fetch(UploadNum, Su2UUID)} ||
                            UploadNum <- SUs] ||
                           ChunkNum <- lists:seq(1, ChunksPerFile)]),
    ToDos = if ShuffleP ->
                    %% Shuffle!  Add random number to front of
                    %% 2-tuple, sort, strip off rnd.
                    random:seed(now()),
                    _T1 = now(),
                    [X || {_Rnd, X} <- lists:sort([{random:uniform(1000), Y} ||
                                                      Y <- Ts])];
               true ->
                    Ts
            end,
    DKey = {Id, todos},
    %% We could be marginally faster if the sext:encoding were done
    %% before making the closure ... but being I/O-bound, it doesn't
    %% matter.
    fun() ->
            case get(DKey) of
                [{ChunkNum, UUID}|Rest] ->
                    put(DKey, Rest),
                    sext:encode({BucketName, ChunkNum, UUID});
                [] ->
                    throw({stop, empty_keygen});
                undefined ->
                    [{ChunkNum, UUID}|Rest] = ToDos,
                    put(DKey, Rest),
                    sext:encode({BucketName, ChunkNum, UUID})
            end
    end.

uuid16_like() ->
    <<X:(16*8)>> = crypto:rand_bytes(16),
    list_to_binary(http_util:integer_to_hexlist(X)).
