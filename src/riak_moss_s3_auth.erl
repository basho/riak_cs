%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_s3_auth).

-behavior(riak_moss_auth).

-include("riak_moss.hrl").

-spec authenticate(term(), [string()]) -> {ok, #moss_user{}}
                                              | {ok, unknown}
                                              | {error, atom()}.
-export([authenticate/2]).

authenticate(RD, [KeyID, Signature]) ->
    case riak_moss_riakc:get_user(KeyID) of
        {ok, User} ->
            case check_auth(User#moss_user.key_id,
                            User#moss_user.key_secret,
                            RD,
                            Signature) of
                true ->
                    {ok, User};
                _ ->
                    {error, invalid_authentication}
            end;
        _ ->
            {error, invalid_authentication}
    end.

extract_amz_meta(RD) ->
    lists:filter(fun({K,_V}) ->
                    lists:prefix(
                        "x-amz-",
                        string:to_lower(any_to_list(K)))
                end,
                mochiweb_headers:to_list(wrq:req_headers(RD))).

any_to_list(V) when is_list(V) ->
    V;
any_to_list(V) when is_atom(V) ->
    atom_to_list(V);
any_to_list(V) when is_binary(V) ->
    binary_to_list(V);
any_to_list(V) when is_integer(V) ->
    integer_to_list(V).


-define(ROOT_HOST, "s3.amazonaws.com").
-define(SUBRESOURCES, ["acl", "location", "logging", "notification", "partNumber",
                       "policy", "requestPayment", "torrent", "uploadId", "uploads",
                       "versionId", "versioning", "versions", "website"]).

canonicalize_qs(QS) ->
    canonicalize_qs(QS, []).

canonicalize_qs([], []) ->
    [];
canonicalize_qs([], Acc) ->
    lists:flatten(["?", Acc]);
canonicalize_qs([{K, []}|T], Acc) ->
    case lists:member(K, ?SUBRESOURCES) of
        true ->
            canonicalize_qs(T, [K|Acc]);
        false ->
            canonicalize_qs(T)
    end;
canonicalize_qs([{K, V}|T], Acc) ->
    case lists:member(K, ?SUBRESOURCES) of
        true ->
            canonicalize_qs(T, [[K, "=", V]|Acc]);
        false ->
            canonicalize_qs(T)
    end.

bucket_from_host(HostHeader) ->
    BaseTokens = string:tokens(?ROOT_HOST, "."),
    case string:tokens(HostHeader, ".") of
        [H|BaseTokens] ->
            H;
        _ ->
            undefined
    end.

canonicalize_resource(RD) ->
    case bucket_from_host(wrq:get_req_header("host", RD)) of
        undefined ->
            [wrq:path(RD)];
        Bucket ->
            ["/", Bucket, wrq:path(RD)]
    end.

check_auth(_KeyID, KeyData, RD, Signature) ->
    AmzHeaders = [[string:to_lower(K), ":", V, "\n"] ||
                     {K, V} <- ordsets:to_list(ordsets:from_list(extract_amz_meta(RD)))],
    Resource = [canonicalize_resource(RD),
                canonicalize_qs(lists:sort(wrq:req_qs(RD)))],
    CMD5 = wrq:get_req_header("content-md5", RD),
    CType = wrq:get_req_header("content-type", RD),
    STS = [atom_to_list(wrq:method(RD)), "\n",
           case CMD5 of undefined -> ""; O -> O end, "\n",
           case CType of undefined -> ""; O -> O end, "\n",
           wrq:get_req_header("date", RD), "\n",
           AmzHeaders,
           Resource],
    Sig = base64:encode_to_string(crypto:sha_mac(KeyData, STS)),
    Sig == Signature.

