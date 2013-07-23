%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% ---------------------------------------------------------------------

%% @doc

-module(riak_cs_list_objects).

-include("riak_cs.hrl").
-include("list_objects.hrl").

%% API
-export([new_request/1,
         new_request/3,
         new_response/4,
         manifest_to_keycontent/1]).

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

%% Rest
%%--------------------------------------------------------------------

-spec manifest_to_keycontent(lfs_manifest()) -> list_objects_key_content().
manifest_to_keycontent(?MANIFEST{bkey={_Bucket, Key},
                                 created=Created,
                                 content_md5=ContentMd5,
                                 content_length=ContentLength,
                                 acl=ACL}) ->

    LastModified = list_to_binary(riak_cs_wm_utils:to_iso_8601(Created)),

    %% Etag
    ETagString = riak_cs_utils:etag_from_binary(ContentMd5),
    Etag = list_to_binary(ETagString),

    Size = ContentLength,
    Owner = acl_to_owner(ACL),
    %% hardcoded since we don't support reduced redundancy or glacier
    StorageClass = <<"STANDARD">>,

    #list_objects_key_content_v1{key=Key,
                              last_modified=LastModified,
                              etag=Etag,
                              size=Size,
                              owner=Owner,
                              storage_class=StorageClass}.

%% ====================================================================
%% Internal functions
%% ====================================================================

-spec acl_to_owner(acl()) -> list_objects_owner().
acl_to_owner(?ACL{owner=Owner}) ->
    {DisplayName, CanonicalId, _KeyId} = Owner,
    CanonicalIdBinary = list_to_binary(CanonicalId),
    DisplayNameBinary = list_to_binary(DisplayName),
    #list_objects_owner_v1{id=CanonicalIdBinary,
                           display_name=DisplayNameBinary}.
