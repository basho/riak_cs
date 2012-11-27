%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc

-module(riak_cs_list_objects).

-include("riak_cs.hrl").
-include("list_objects.hrl").

%% API
-export([new_request/1,
         new_request/3,
         new_response/4]).


%%%===================================================================
%%% API
%%%===================================================================

%% Request
%%--------------------------------------------------------------------

-spec new_request(binary()) -> list_object_request().
new_request(Name) ->
    new_request(Name, 1000, []).

-spec new_request(binary(), pos_integer(), list()) -> list_object_request().
new_request(Name, MaxKeys, Options) ->
    process_options(#list_objects_request_v1{name=Name,
                                             max_keys=MaxKeys},
                    Options).

%% @private
-spec process_options(list_object_request(), list()) ->
    list_object_request().
process_options(Request, Options) ->
    lists:foldl(fun process_options_helper/2,
                Request,
                Options).

process_options_helper({prefix, Val}, Req) ->
    Req#list_objects_request_v1{prefix=Val};
process_options_helper({delimiter, Val}, Req) ->
    Req#list_objects_request_v1{delimiter=Val};
process_options_helper({marker, Val}, Req) ->
    Req#list_objects_request_v1{marker=Val}.

%% Response
%%--------------------------------------------------------------------

-spec new_response(list_object_request(),
                   IsTruncated :: boolean(),
                   CommonPrefixes :: list(list_objects_common_prefixes()),
                   ObjectContents :: list(list_objects_key_content())) ->
    list_object_response().
new_response(?LOREQ{name=Name,
                    max_keys=MaxKeys,
                    prefix=Prefix,
                    delimiter=Delimiter,
                    marker=Marker},
             IsTruncated, CommonPrefixes, ObjectContents) ->
    ?LORESP{name=Name,
            max_keys=MaxKeys,
            prefix=Prefix,
            delimiter=Delimiter,
            marker=Marker,
            is_truncated=IsTruncated,
            contents=ObjectContents,
            common_prefixes=CommonPrefixes}.

%% ====================================================================
%% Internal functions
%% ====================================================================
