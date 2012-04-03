-module(riak_moss_legacy_shim).

-export([v1metadata_guard/1, v2manifest_guard/1]).

manifest_type(RiakcPid, Bucket, Key) ->
    ManifestBucket = riak_moss_utils:to_bucket_name(objects, Bucket),
    case riakc_pb_socket:get(RiakcPid, ManifestBucket, Key) of
        {ok, Object} ->
            {ok, v2manifest};
        {error, notfound} ->
            PrefixedBucket = riak_moss_reader:to_bucket_name(objects, Bucket),
            case riakc_pb_socket:get(RiakcPid, PrefixedBucket, Key) of
                {ok, Obj} -> {ok, v1metadata};
                {error, notfound} ->
                    {error, notfound}
            end
    end.
    
v2manifest_guard(RD) ->
    Bucket = list_to_binary(wrq:path_info(bucket, RD)),
    ManifestBucket = riak_moss_utils:to_bucket_name(objects, Bucket),
    
    Key = case wrq:path_tokens(RD) of
        [] ->
            undefined;
        KeyTokens ->
            mochiweb_util:unquote(string:join(KeyTokens, "/"))
    end,
    
    case riak_moss_utils:riak_connection() of
        {ok, Pid} ->
            case riakc_pb_socket:get(Pid, ManifestBucket, Key) of
                {ok, _Object} -> true;
                _ -> false
            end;
        {error, _Reason} -> false
    end.
    
v1metadata_guard(RD) ->
    Bucket = list_to_binary(wrq:path_info(bucket, RD)),
    PrefixedBucket = riak_moss_reader:to_bucket_name(objects, Bucket),
    
    Key = case wrq:path_tokens(RD) of
        [] ->
            undefined;
        KeyTokens ->
            mochiweb_util:unquote(string:join(KeyTokens, "/"))
    end,

    case riak_moss_utils:riak_connection() of
        {ok, Pid} ->
            case riakc_pb_socket:get(Pid, PrefixedBucket, Key) of
                {ok, _Object} -> true;
                _ -> false
            end;
        {error, _Reason} -> false
    end.