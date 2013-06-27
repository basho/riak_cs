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
%%
%% @doc WM callback module for S3 list multipart uploads.
%%
%% TODO: Intentionally not (yet) implemented:
%%
%% * list multipart uploads output: maximum 1000 results grouping
%% * list multipart uploads output: upload 'Initiator' ARN data

-module(riak_cs_wm_bucket_uploads).

-export([init/1,
         authorize/2,
         content_types_provided/2,
         to_xml/2,
         allowed_methods/0,
         malformed_request/2,
         content_types_accepted/2,
         multiple_choices/2,
         finish_request/2]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-define(RIAKCPOOL, bucket_list_pool).

-spec init(#context{}) -> {ok, #context{}}.
init(Ctx) ->
    {ok, Ctx#context{local_context=#key_context{},
                     riakc_pool=?RIAKCPOOL}}.

-spec malformed_request(#wm_reqdata{}, #context{}) -> {false, #wm_reqdata{}, #context{}}.
malformed_request(RD,Ctx=#context{local_context=LocalCtx0}) ->
    Bucket = list_to_binary(wrq:path_info(bucket, RD)),
    LocalCtx = LocalCtx0#key_context{bucket=Bucket},
    {false, RD, Ctx#context{local_context=LocalCtx}}.

-spec authorize(#wm_reqdata{}, #context{}) -> {boolean() | {halt, non_neg_integer()}, #wm_reqdata{}, #context{}}.
authorize(RD, Ctx) ->
    riak_cs_wm_utils:bucket_access_authorize_helper(bucket_uploads, false, RD, Ctx).

%% @doc Get the list of methods this resource supports.
-spec allowed_methods() -> [atom()].
allowed_methods() ->
    ['GET'].

to_xml(RD, Ctx=#context{local_context=LocalCtx,
                        riakc_pid=RiakcPid}) ->
    #key_context{bucket=Bucket} = LocalCtx,
    User = riak_cs_mp_utils:user_rec_to_3tuple(Ctx#context.user),
    Opts = make_list_mp_uploads_opts(RD),
    case riak_cs_mp_utils:list_multipart_uploads(Bucket, User, Opts, RiakcPid) of
        {ok, {Ds, Commons}} ->
            Us = [{'Upload',
                   [
                    {'Key', [unicode:characters_to_list(D?MULTIPART_DESCR.key, unicode)]},
                    {'UploadId', [binary_to_list(base64url:encode(D?MULTIPART_DESCR.upload_id))]},
                    {'Initiator',               % TODO: replace with ARN data?
                     [{'ID', [D?MULTIPART_DESCR.owner_key_id]},
                      {'DisplayName', [D?MULTIPART_DESCR.owner_display]}
                     ]},
                    {'Owner',
                     [{'ID', [D?MULTIPART_DESCR.owner_key_id]},
                      {'DisplayName', [D?MULTIPART_DESCR.owner_display]}
                     ]},
                    %% Just ignore the value in `D?MULTIPART_DESCR.storage_class',
                    %% since there was a bug where it was writen as `regular'.
                    {'StorageClass', ["STANDARD"]},
                    {'Initiated', [D?MULTIPART_DESCR.initiated]}
                   ]
                  } || D <- Ds],
            Cs = [{'CommonPrefixes',
                   [
                    % WTH? The pattern [Common | _] can never match the type []
                    {'Prefix', [binary_to_list(Common)]}
                   ]} || Common <- Commons],
            Get = fun(Name) -> case proplists:get_value(Name, Opts) of
                                   undefined -> [];
                                   X         -> binary_to_list(X)
                               end
                  end,
            XmlDoc = {'ListMultipartUploadsResult',
                       [{'xmlns', "http://s3.amazonaws.com/doc/2006-03-01/"}],
                       [
                        {'Bucket', [binary_to_list(Bucket)]},
                        {'KeyMarker', [Get(key_marker)]},
                        {'NextKeyMarker', []},      % TODO
                        {'NextUploadIdMarker', [Get(upload_id_marker)]},
                        {'Delimiter', [Get(delimiter)]},
                        {'Prefix', [Get(prefix)]},
                        {'MaxUploads', ["1000"]},     % TODO
                        {'IsTruncated', ["false"]}   % TODO
                      ] ++ Us ++ Cs
                     },
            Body = riak_cs_xml:export_xml([XmlDoc]),
            {Body, RD, Ctx};
        {error, Reason} ->
            riak_cs_s3_response:api_error(Reason, RD, Ctx)
    end.

multiple_choices(RD, Ctx) ->
    {false, RD, Ctx}.

finish_request(RD, Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"finish_request">>, [0], []),
    {true, RD, Ctx}.

-spec content_types_provided(#wm_reqdata{}, #context{}) -> {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_provided(RD, Ctx=#context{}) ->
    Method = wrq:method(RD),
    if Method == 'GET' ->
            {[{?XML_TYPE, to_xml}], RD, Ctx};
       true ->
            %% this shouldn't ever be called, it's just to
            %% appease webmachine
            {[{"text/plain", unused_callback}], RD, Ctx}
    end.

-spec content_types_accepted(#wm_reqdata{}, #context{}) -> {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_accepted(RD, Ctx) ->
    riak_cs_mp_utils:make_content_types_accepted(RD, Ctx).

make_list_mp_uploads_opts(RD) ->
    Params1 = [{"delimiter", delimiter},
               {"max-uploads", max_uploads},
               {"prefix", prefix}],
    Params2 = [{"key-marker", key_marker},
               {"upload-id-marker", upload_id_marker}],
    assemble_options(Params1, undefined, RD) ++
        assemble_options(Params2, <<>>, RD).

assemble_options(Parameters, Default, RD) ->
    [case wrq:get_qs_value(Name, RD) of
         undefined -> {PropName, Default};
         X         -> {PropName, list_to_binary(X)}
     end || {Name, PropName} <- Parameters].
