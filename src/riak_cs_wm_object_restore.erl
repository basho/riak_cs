%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_cs_wm_object_restore).

-export([init/1,
         authorize/2,
         %% content_types_provided/2,
         generate_etag/2,
         last_modified/2,
         allowed_methods/0,
         malformed_request/2,
         content_types_accepted/2,
         process_post/2,
         valid_entity_length/2]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("webmachine/include/wm_reqstate.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-spec init(#context{}) -> {ok, #context{}}.
init(Ctx) ->
    {ok, Ctx#context{local_context=#key_context{}}}.

-spec malformed_request(#wm_reqdata{}, #context{}) -> {false, #wm_reqdata{}, #context{}}.
malformed_request(RD, Ctx) ->
    ContextWithKey = riak_cs_wm_utils:extract_key(RD, Ctx),
    case riak_cs_wm_utils:has_canned_acl_and_header_grant(RD) of
        true ->
            riak_cs_s3_response:api_error(canned_acl_and_header_grant,
                                          RD, ContextWithKey);
        false ->
            {false, RD, ContextWithKey}
    end.

%% @doc Get the type of access requested and the manifest with the
%% object ACL and compare the permission requested with the permission
%% granted, and allow or deny access. Returns a result suitable for
%% directly returning from the {@link forbidden/2} webmachine export.
-spec authorize(#wm_reqdata{}, #context{}) ->
    {boolean() | {halt, term()}, #wm_reqdata{}, #context{}}.
authorize(RD, Ctx0=#context{local_context=LocalCtx0,
                            riak_client=RcPid}) ->
    Method = wrq:method(RD),
    RequestedAccess =
        riak_cs_acl_utils:requested_access(Method, false),
    LocalCtx = riak_cs_wm_utils:ensure_doc(LocalCtx0, RcPid),

    Ctx = Ctx0#context{requested_perm=RequestedAccess, local_context=LocalCtx},
    authorize(RD, Ctx,
              LocalCtx#key_context.bucket_object,
              Method, LocalCtx#key_context.manifest).

authorize(RD, Ctx, notfound = _BucketObj, _Method, _Manifest) ->
    riak_cs_wm_utils:respond_api_error(RD, Ctx, no_such_bucket);
authorize(RD, Ctx, _BucketObj, _Method, notfound = _Manifest) ->
    riak_cs_wm_utils:respond_api_error(RD, Ctx, no_such_key);
authorize(RD, Ctx, _BucketObj, _Method, _Manifest) ->
    riak_cs_wm_utils:object_access_authorize_helper(object, true, RD, Ctx).

%% @doc Get the list of methods this resource supports.
-spec allowed_methods() -> [atom()].
allowed_methods() ->
    %% POST is supported for restoring from cold storage
    ['POST'].

-spec valid_entity_length(#wm_reqdata{}, #context{}) -> {boolean(), #wm_reqdata{}, #context{}}.
valid_entity_length(RD, Ctx=#context{response_module=ResponseMod}) ->
    case catch(
           list_to_integer(
             wrq:get_req_header("Content-Length", RD))) of
        Length when is_integer(Length) ->

            %% We just don't accept too large body for just requesting object restore
            case Length =< riak_cs_lfs_utils:max_content_len() of
                false ->
                    ResponseMod:api_error(entity_too_large, RD, Ctx);
                true ->
                    {true, RD, Ctx}
            end;
        _ ->
            {false, RD, Ctx}
    end.

%% -spec content_types_provided(#wm_reqdata{}, #context{}) -> {[{string(), atom()}], #wm_reqdata{}, #context{}}.
%% content_types_provided(RD, Ctx=#context{local_context=LocalCtx,
%%                                         riak_client=RcPid}) ->
    
%%     Mfst = LocalCtx#key_context.manifest,
%%     %% TODO:
%%     %% As I understand S3, the content types provided
%%     %% will either come from the value that was
%%     %% last PUT or, from you adding a
%%     %% `response-content-type` header in the request.
%%     Method = wrq:method(RD),
%%     if Method == 'GET'; Method == 'HEAD' ->
%%             UpdLocalCtx = riak_cs_wm_utils:ensure_doc(LocalCtx, RcPid),
%%             ContentType = binary_to_list(Mfst?MANIFEST.content_type),
%%             case ContentType of
%%                 _ ->
%%                     UpdCtx = Ctx#context{local_context=UpdLocalCtx},
%%                     {[{ContentType, produce_body}], RD, UpdCtx}
%%             end;
%%        true ->
%%             %% TODO this shouldn't ever be called, it's just to
%%             %% appease webmachine
%%             {[{"text/plain", produce_body}], RD, Ctx}
%%     end.

-spec generate_etag(#wm_reqdata{}, #context{}) -> {string(), #wm_reqdata{}, #context{}}.
generate_etag(RD, Ctx=#context{local_context=LocalCtx}) ->
    Mfst = LocalCtx#key_context.manifest,
    ETag = riak_cs_manifest:etag_no_quotes(Mfst),
    {ETag, RD, Ctx}.

-spec last_modified(#wm_reqdata{}, #context{}) -> {calendar:datetime(), #wm_reqdata{}, #context{}}.
last_modified(RD, Ctx=#context{local_context=LocalCtx}) ->
    Mfst = LocalCtx#key_context.manifest,
    ErlDate = riak_cs_wm_utils:iso_8601_to_erl_date(Mfst?MANIFEST.created),
    {ErlDate, RD, Ctx}.

-spec process_post(#wm_reqdata{}, #context{}) -> {true, #wm_reqdata{}, #context{}}.
process_post(RD, Ctx = #context{local_context=LocalCtx0,
                                riak_client=_RcPid}) ->
    #key_context{bucket=Bucket, key=Key, manifest=Manifest} = LocalCtx0,
    case restore_expireation_days(wrq:req_body(RD)) of
        {ok, Days} ->
            %% Do schedule some restore
            UUID = riak_cs_manifest:uuid(Manifest),
            %% TODO: check whether this UUID is really archived
            %% There must be race condition where this UUID blocks are not yet archived,
            %% even the lifecycle was changed and won't be archived any more ...
            %% But so far I take this granted that this is the right UUID to take
            lager:info("Restoring ~p,~p scheduled (expiration: ~p days)", [Bucket, Key, Days]),
            R = riak_cs_s3_lifecycle:schedule_restore(UUID, Manifest, Days),
            lager:debug("schedule result: ~p", [R]),
            {true, RD, Ctx};
        {error, E} ->
            riak_cs_s3_response:api_error(E, RD, Ctx)
    end.

-spec content_types_accepted(#wm_reqdata{}, #context{}) -> {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_accepted(RD, Ctx) ->
    content_types_accepted(wrq:get_req_header("Content-Type", RD), RD, Ctx).

-spec content_types_accepted(undefined | string(), #wm_reqdata{}, #context{}) ->
                                    {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_accepted(CT, RD, Ctx)
  when CT =:= undefined;
       CT =:= [] ->
    content_types_accepted("application/octet-stream", RD, Ctx);
content_types_accepted(CT, RD, Ctx=#context{local_context=LocalCtx0}) ->
    %% This was shamelessly ripped out of
    %% https://github.com/basho/riak_kv/blob/0d91ca641a309f2962a216daa0cee869c82ffe26/src/riak_kv_wm_object.erl#L492
    {Media, _Params} = mochiweb_util:parse_header(CT),
    case string:tokens(Media, "/") of
        [_Type, _Subtype] ->
            %% accept whatever the user says
            LocalCtx = LocalCtx0#key_context{putctype=Media},
            {[{Media, add_acl_to_context_then_accept}], RD, Ctx#context{local_context=LocalCtx}};
        _ ->
            %% TODO:
            %% Maybe we should have caught
            %% this in malformed_request?
            {[],
             wrq:set_resp_header(
               "Content-Type",
               "text/plain",
               wrq:set_resp_body(
                 ["\"", Media, "\""
                  " is not a valid media type"
                  " for the Content-type header.\n"],
                 RD)),
             Ctx}
    end.

restore_expireation_days(Bin) when is_binary(Bin) ->
    restore_expireation_days(unicode:characters_to_list(Bin));
restore_expireation_days(XMLString) when is_list(XMLString) ->
    case riak_cs_xml:scan(XMLString) of
        {ok, #xmlElement{} = Tree} ->
            case xmerl_xpath:string("//RestoreRequest/Days/text()", Tree) of
                [#xmlText{value=V}] ->
                    {ok, list_to_integer(V)};
                _Other ->
                    %% maybe long dom tree root other than RestoreRequest
                    {error, malformed_xml}
            end;
        Error ->
            Error
    end.

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
xml_parse_test() ->
    XML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<RestoreRequest xmlns=\"http://s3.amazonaws.com/doc/2006-3-01\">"
        "<Days>7</Days>"
        "</RestoreRequest>",
    ?assertEqual({ok, 7}, restore_expireation_days(XML)).

-endif.
