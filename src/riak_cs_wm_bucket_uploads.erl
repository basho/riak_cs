%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
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

-spec init(#context{}) -> {ok, #context{}}.
init(Ctx) ->
    {ok, Ctx#context{local_context=#key_context{}}}.

-spec malformed_request(#wm_reqdata{}, #context{}) -> {false, #wm_reqdata{}, #context{}}.
malformed_request(RD,Ctx=#context{local_context=LocalCtx0}) ->
    Bucket = list_to_binary(wrq:path_info(bucket, RD)),
    LocalCtx = LocalCtx0#key_context{bucket=Bucket},
    {false, RD, Ctx#context{local_context=LocalCtx}}.

-spec authorize(#wm_reqdata{}, #context{}) -> 
                       {boolean() | {halt, term()}, #wm_reqdata{}, #context{}}.

authorize(RD, Ctx=#context{riakc_pid=RiakcPid, local_context=LocalCtx}) ->
    Bucket = LocalCtx#key_context.bucket,
    ReqAccess = riak_cs_acl_utils:requested_access('GET', not_really),
    case {riak_cs_utils:check_bucket_exists(Bucket, RiakcPid),
          riak_cs_acl_utils:check_grants(Ctx#context.user, Bucket,
                                         ReqAccess, RiakcPid)} of
        {{ok, _}, true} ->
            {false, RD, Ctx};
        {{ok, _}, false} ->
            {{halt, 403}, RD, Ctx};
        {{error, Reason}, _} ->
            riak_cs_s3_response:api_error(Reason, RD, Ctx);
        _X ->
            {{halt, 404}, RD, Ctx}
    end.

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
        {ok, {Ds, Common}} ->
            Us = [{'Upload',
                   [
                    {'Key', [D?MULTIPART_DESCR.key]},
                    {'UploadId', [binary_to_list(base64url:encode(D?MULTIPART_DESCR.upload_id))]},
                    {'Initiator',               % TODO: replace with ARN data?
                     [{'ID', [D?MULTIPART_DESCR.owner_key_id]},
                      {'DisplayName', [D?MULTIPART_DESCR.owner_display]}
                     ]},
                    {'Owner',
                     [{'ID', [D?MULTIPART_DESCR.owner_key_id]},
                      {'DisplayName', [D?MULTIPART_DESCR.owner_display]}
                     ]},
                    {'StorageClass', [string:to_upper(atom_to_list(D?MULTIPART_DESCR.storage_class))]},
                    {'Initiated', [D?MULTIPART_DESCR.initiated]}
                   ]
                  } || D <- Ds],
            Cs = [{'CommonPrefixes',
                   [
                    {'Prefix', [C]}
                   ]} || C <- Common],
            Get = fun(Name) -> case proplists:get_value(Name, Opts) of
                                   undefined -> [];
                                   X         -> X
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
            Body = riak_cs_s3_response:export_xml([XmlDoc]),
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
    %% Weird: wrq:req_qs(RD) returns [], so that any call to
    %% wrq:get_ws_value(Name, RD) will also fail.  {sigh}
    %% Get the original and parse it instead.
    {_, Qs} = riak_cs_s3_rewrite:original_resource(RD),
    Ps = [{"delimiter", delimiter},
          {"key-marker", key_marker},
          {"max-uploads", max_uploads},
          {"prefix", prefix},
          {"upload-id-marker", upload_id_marker}
         ],
    lists:append([case proplists:get_value(Name, Qs) of
                      undefined -> [];
                      X         -> [{PropName, X}]
                  end || {Name, PropName} <- Ps]).

