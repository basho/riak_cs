%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_wm_key).

-export([init/1,
         service_available/2,
         forbidden/2,
         content_types_provided/2,
         malformed_request/2,
         produce_body/2,
         allowed_methods/2,
         content_types_accepted/2,
         accept_body/2,
         delete_resource/2,
         valid_entity_length/2]).

-include("riak_moss.hrl").
-include_lib("webmachine/include/webmachine.hrl").

init(Config) ->
    %% Check if authentication is disabled and
    %% set that in the context.
    AuthBypass = proplists:get_value(auth_bypass, Config),
    {ok, #key_context{context=#context{auth_bypass=AuthBypass}}}.

-spec extract_paths(term(), term()) -> term().
extract_paths(RD, Ctx) ->
    Bucket = wrq:path_info(bucket, RD),
    Key = wrq:path_info(key, RD),
    Ctx#key_context{bucket=Bucket, key=Key}.

-spec service_available(term(), term()) -> {true, term(), term()}.
service_available(RD, Ctx) ->
    case riak_moss_wm_utils:service_available(RD, Ctx) of
        {true, ServiceRD, ServiceCtx} ->
            %% this fills in the bucket and key
            %% part of the context so they are
            %% available in the rest of the
            %% chain
            NewCtx = extract_paths(ServiceRD, ServiceCtx),
            {true, ServiceRD, NewCtx};
        {false, _, _} ->
            {false, RD, Ctx}
    end.

-spec malformed_request(term(), term()) -> {false, term(), term()}.
malformed_request(RD, Ctx) ->
    case wrq:method(RD) of
        'GET' ->
            DocCtx = riak_moss_wm_utils:ensure_doc(Ctx),
            case DocCtx#key_context.doc of
                notfound ->
                    %% more or less ripped
                    %% from riak_kv_wm_object.erl
                    {{halt, 404},
                        wrq:set_resp_header("Content-Type", "text/plain",
                                            RD),
                        DocCtx};
                _ ->
                    {false, RD, DocCtx}
            end;
        _ ->
            {false, RD, Ctx}
    end.

%% @doc Check to see if the user is
%%      authenticated. Normally with HTTP
%%      we'd use the `authorized` callback,
%%      but this is how S3 does things.
forbidden(RD, Ctx=#key_context{context=#context{auth_bypass=AuthBypass}}) ->
    AuthHeader = wrq:get_req_header("authorization", RD),
    case riak_moss_wm_utils:parse_auth_header(AuthHeader, AuthBypass) of
        {ok, AuthMod, Args} ->
            case AuthMod:authenticate(RD, Args) of
                {ok, User} ->
                    %% Authentication succeeded
                    NewInnerCtx = Ctx#key_context.context#context{user=User},
                    {false, RD, Ctx#key_context{context=NewInnerCtx}};
                {error, _Reason} ->
                    %% Authentication failed, deny access
                    riak_moss_s3_response:api_error(access_denied, RD, Ctx)
            end
    end.

%% @doc Get the list of methods this resource supports.
-spec allowed_methods(term(), term()) -> {[atom()], term(), term()}.
allowed_methods(RD, Ctx) ->
    %% TODO: POST
    {['HEAD', 'GET', 'DELETE', 'PUT'], RD, Ctx}.

valid_entity_length(RD, Ctx) ->
    case wrq:method(RD) of
        'PUT' ->
            case catch(
                   list_to_integer(
                     wrq:get_req_header("Content-Length", RD))) of
                Length when is_integer(Length) ->
                    case Length =< ?MAX_CONTENT_LENGTH of
                        false ->
                            riak_moss_s3_response:api_error(
                              entity_too_large, RD, Ctx);
                        true ->
                            {true, RD, Ctx#key_context{size=Length}}
                    end;

                _ ->
                    {false, RD, Ctx}
            end;
        _ ->
            {true, RD, Ctx}
    end.


-spec content_types_provided(term(), term()) ->
    {[{string(), atom()}], term(), term()}.
content_types_provided(RD, Ctx) ->
    %% TODO:
    %% As I understand S3, the content types provided
    %% will either come from the value that was
    %% last PUT or, from you adding a
    %% `response-content-type` header in the request.
    Method = wrq:method(RD),
    if Method == 'GET'; Method == 'HEAD' ->
            DocCtx = riak_moss_wm_utils:ensure_doc(Ctx),
            Doc = DocCtx#key_context.doc,
            case riakc_obj:get_content_type(Doc) of
                undefined ->
                    {[{"application/octet-stream", produce_body}], RD, Ctx};
                ContentType ->
                    {[{ContentType, produce_body}], RD, Ctx}
            end;
       true ->
            %% TODO
            %% this shouldn't ever be
            %% called, it's just to appease
            %% webmachine
            {[{"text/plain", produce_body}], RD, Ctx}
    end.

-spec produce_body(term(), term()) -> {iolist()|binary(), term(), term()}.
produce_body(RD, Ctx) ->
    DocCtx = riak_moss_wm_utils:ensure_doc(Ctx),
    Doc = DocCtx#key_context.doc,
    case Doc of
        notfound ->
            {{halt, 404}, RD, DocCtx};
        _ ->
            {riakc_obj:get_value(Doc), RD, DocCtx}
    end.

%% @doc Callback for deleting an object.
-spec delete_resource(term(), term()) -> boolean().
delete_resource(RD, Ctx=#key_context{bucket=Bucket, key=Key}) ->
    case riak_moss_riakc:delete_object(Bucket, Key) of
        ok ->
            {true, RD, Ctx};
        %% TODO:
        %% What could this error even be?
        _ ->
            {false, RD, Ctx}
    end.

-spec content_types_accepted(term(), term()) ->
    {[{string(), atom()}], term(), term()}.
content_types_accepted(RD, Ctx) ->
    case wrq:get_req_header("Content-Type", RD) of
        undefined ->
            {[{"binary/octet-stream", accept_body}], RD, Ctx};
        %% This was shamelessly ripped out of
        %% https://github.com/basho/riak_kv/blob/0d91ca641a309f2962a216daa0cee869c82ffe26/src/riak_kv_wm_object.erl#L492
        CType ->
            Media = hd(string:tokens(CType, ";")),
            case string:tokens(Media, "/") of
                [_Type, _Subtype] ->
                    %% accept whatever the user says
                    {[{Media, accept_body}], RD, Ctx#key_context{putctype=Media}};
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
            end
    end.

-spec accept_body(term(), term()) ->
    {true, term(), term()}.
accept_body(RD, Ctx=#key_context{}) ->
    accept_streambody(RD, Ctx, <<>>, wrq:stream_req_body(RD, ?DEFAULT_LFS_BLOCK_SIZE)).

accept_streambody(RD, Ctx=#key_context{}, Buf, {Data, Next}) ->
    NewBuf = <<Buf/binary,Data/binary>>,
    if is_function(Next) ->
            accept_streambody(RD, Ctx, NewBuf, Next());
       Next =:= done ->
            write_object(RD, Ctx, NewBuf)
    end.

%% TODO:
%% We need to do some checking to make sure
%% the bucket exists for the user who is doing
%% this PUT
write_object(RD, Ctx=#key_context{bucket=Bucket,key=Key,context=Context,putctype=CType,size=Size}, Data) ->
    User = Context#context.user,
    KeyID = User#moss_user.key_id,
    {ok, Pid} = riak_moss_put_fsm:start_link(Bucket, Key, Size, ?DEFAULT_LFS_BLOCK_SIZE,
                                             Data, 60000),
    %% we should be ripping some metadata
    %% out of the request headers
    %Metadata = dict:from_list([{<<"content-type">>, CType}]),
    %riak_moss_riakc:put_object(KeyID, Bucket, Key, Data, Metadata),
    {true, wrq:set_resp_header("ETag",
      "\"" ++ riak_moss:binary_to_hexlist(crypto:md5(Data)) ++ "\"", RD), Ctx}.

