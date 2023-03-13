%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
%%               2021, 2022 TI Tokyo    All Rights Reserved.
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
-include("riak_cs_web.hrl").

%% API
-export([new_request/2,
         new_request/4,
         new_response/5,
         manifest_to_keycontent/2]).

%%%===================================================================
%%% API
%%%===================================================================

%% Request
%%--------------------------------------------------------------------

-spec new_request(list_objects_req_type(), binary()) -> list_object_request().
new_request(Type, Name) ->
    new_request(Type, Name, 1000, []).

-spec new_request(list_objects_req_type(), binary(), pos_integer(), list()) -> list_object_request().
new_request(Type, Name, MaxKeys, Options) ->
    process_options(#list_objects_request{req_type = Type,
                                          name = Name,
                                          max_keys = MaxKeys},
                    Options).

%% @private
-spec process_options(list_object_request(), list()) ->
    list_object_request().
process_options(Request, Options) ->
    lists:foldl(fun process_options_helper/2,
                Request,
                Options).

process_options_helper({prefix, Val}, Req) ->
    Req#list_objects_request{prefix = Val};
process_options_helper({delimiter, Val}, Req) ->
    Req#list_objects_request{delimiter = Val};
process_options_helper({marker, Val}, Req) ->
    Req#list_objects_request{marker = Val}.

%% Response
%%--------------------------------------------------------------------

-spec new_response(list_object_request(),
                   IsTruncated :: boolean(),
                   NextMarker :: next_marker(),
                   CommonPrefixes :: list(list_objects_common_prefixes()),
                   ObjectContents :: list(list_objects_key_content())) ->
    list_objects_response() | list_object_versions_response().
new_response(?LOREQ{req_type = objects,
                    name = Name,
                    max_keys = MaxKeys,
                    prefix = Prefix,
                    delimiter = Delimiter,
                    marker = Marker},
             IsTruncated, NextMarker, CommonPrefixes, ObjectContents) ->
    ?LORESP{name = Name,
            max_keys = MaxKeys,
            prefix = Prefix,
            delimiter = Delimiter,
            marker = Marker,
            next_marker = NextMarker,
            is_truncated = IsTruncated,
            contents = ObjectContents,
            common_prefixes = CommonPrefixes};

new_response(?LOREQ{req_type = versions,
                    name = Name,
                    max_keys = MaxKeys,
                    prefix = Prefix,
                    delimiter = Delimiter,
                    marker = Marker},
             IsTruncated, NextMarker, CommonPrefixes, ObjectContents) ->
    {KeyMarker, VersionIdMarker} = safe_decompose_key(Marker),
    {NextKeyMarker, NextVersionIdMarker} = safe_decompose_key(NextMarker),
    ?LOVRESP{name = Name,
             max_keys = MaxKeys,
             prefix = Prefix,
             delimiter = Delimiter,
             key_marker = KeyMarker,
             version_id_marker = VersionIdMarker,
             next_key_marker = NextKeyMarker,
             next_version_id_marker = NextVersionIdMarker,
             is_truncated = IsTruncated,
             contents = ObjectContents,
             common_prefixes = CommonPrefixes}.

safe_decompose_key(undefined) -> {undefined, undefined};
safe_decompose_key(K) -> rcs_common_manifest:decompose_versioned_key(K).

%% Rest
%%--------------------------------------------------------------------

-spec manifest_to_keycontent(list_objects_req_type(), lfs_manifest()) ->
          list_objects_key_content() | list_object_versions_key_content().
manifest_to_keycontent(ReqType, ?MANIFEST{bkey = {_Bucket, Key},
                                          created = Created,
                                          content_md5 = ContentMd5,
                                          content_length = ContentLength,
                                          vsn = Vsn,
                                          acl = ACL}) ->

    LastModified = list_to_binary(riak_cs_wm_utils:to_iso_8601(Created)),

    %% Etag
    ETagString = riak_cs_utils:etag_from_binary(ContentMd5),
    Etag = list_to_binary(ETagString),

    Size = ContentLength,
    Owner = acl_to_owner(ACL),
    %% hardcoded since we don't support reduced redundancy or glacier
    StorageClass = <<"STANDARD">>,

    case ReqType of
        versions ->
            ?LOVKC{key = Key,
                   last_modified = LastModified,
                   etag = Etag,
                   is_latest = true,
                   version_id = Vsn,
                   size = ContentLength,
                   owner = Owner,
                   storage_class = StorageClass};
        objects ->
            ?LOKC{key = Key,
                  last_modified = LastModified,
                  etag = Etag,
                  size = Size,
                  owner = Owner,
                  storage_class = StorageClass}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

acl_to_owner(?ACL{owner = #{display_name := DisplayName,
                            canonical_id := CanonicalId}}) ->
    #list_objects_owner{id = CanonicalId,
                        display_name = DisplayName}.
