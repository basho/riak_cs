%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_wm_utils).

-export([service_available/2,
         parse_auth_header/2,
         ensure_doc/1,
         user_record_to_proplist/1]).

-include("riak_moss.hrl").
-include_lib("webmachine/include/webmachine.hrl").

service_available(RD, Ctx) ->
    %% TODO:
    %% At some point in the future
    %% this needs to check if we have
    %% an alive Riak server. Although
    %% maybe it makes more sense to be
    %% optimistic and wait untl we actually
    %% check the ACL?

    %% For now we just always
    %% return true
    {true, RD, Ctx}.

%% @doc Parse an authentication header string and determine
%%      the appropriate module to use to authenticate the request.
%%      The passthru auth can be used either with a KeyID or
%%      anonymously by leving the header empty.
-spec parse_auth_header(string(), boolean()) -> {ok, atom(), [string()]} | {error, term()}.
parse_auth_header(KeyID, true) when KeyID =/= undefined ->
    {ok, riak_moss_passthru_auth, [KeyID]};
parse_auth_header(_, true) ->
    {ok, riak_moss_passthru_auth, []};
parse_auth_header(undefined, false) ->
    {ok, riak_moss_blockall_auth, [unkown_auth_scheme]};
parse_auth_header("AWS " ++ Key, _) ->
    case string:tokens(Key, ":") of
        [KeyId, KeyData] ->
            {ok, riak_moss_s3_auth, [KeyId, KeyData]};
        Other -> Other
    end;
parse_auth_header(_, _) ->
    {ok, riak_moss_blockall_auth, [unkown_auth_scheme]}.


%% @doc Utility function for accessing
%%      a riakc_obj without retrieving
%%      it again if it's already in the
%%      Ctx
-spec ensure_doc(term()) -> term().
ensure_doc(Ctx=#key_context{doc=undefined, bucket=Bucket, key=Key}) ->
    %% TODO:
    %% need to do some error
    %% checking here to match
    %% {error, Reason} too
    MOSSBucket = riak_moss:to_bucket_name(objects, list_to_binary(Bucket)),
    case riak_moss_riakc:get_object(MOSSBucket, Key) of
        {ok, RiakObject} ->
            Ctx#key_context{doc=RiakObject};
        {error, notfound} ->
            Ctx#key_context{doc=notfound}
    end;
ensure_doc(Ctx) -> Ctx.

%% @doc Convert a moss_user record
%%      into a property list, likely
%%      for json encoding
-spec user_record_to_proplist(term()) -> list().
user_record_to_proplist(#moss_user{name=Name,
                                   key_id=KeyID,
                                   key_secret=KeySecret,
                                   buckets = Buckets}) ->
    [{<<"name">>, list_to_binary(Name)},
     {<<"key_id">>, list_to_binary(KeyID)},
     {<<"key_secret">>, list_to_binary(KeySecret)},
     {<<"buckets">>, Buckets}].
