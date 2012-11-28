%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_wm_object_acl).

-export([init/1,
         allowed_methods/0,
         malformed_request/2,
         authorize/2,
         content_types_accepted/2,
         content_types_provided/2,
         accept_body/2,
         produce_body/2]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").

init(Ctx) ->
    {ok, Ctx#context{local_context=#key_context{}}}.

%% TODO: copy/pasted from from wm_object, move somewhere common
malformed_request(RD,Ctx=#context{local_context=LocalCtx0}) ->    
    Bucket = list_to_binary(wrq:path_info(bucket, RD)),
    %% need to unquote twice since we re-urlencode the string during rewrite in 
    %% order to trick webmachine dispatching
    Key = mochiweb_util:unquote(mochiweb_util:unquote(wrq:path_info(object, RD))),
    LocalCtx = LocalCtx0#key_context{bucket=Bucket, key=Key},
    {false, RD, Ctx#context{local_context=LocalCtx}}.

%% @doc Get the type of access requested and the manifest with the
%% object ACL and compare the permission requested with the permission
%% granted, and allow or deny access. Returns a result suitable for
%% directly returning from the {@link forbidden/2} webmachine export.
authorize(RD, Ctx0=#context{local_context=LocalCtx0, riakc_pid=RiakPid}) ->
    Method = wrq:method(RD),
    RequestedAccess =
        %% This is really the only difference between authorize/2 in this module and riak_cs_wm_object
        riak_cs_acl_utils:requested_access(Method, true),
    LocalCtx = riak_cs_wm_utils:ensure_doc(LocalCtx0, RiakPid),
    Ctx = Ctx0#context{requested_perm=RequestedAccess,local_context=LocalCtx},
    check_permission(Method, RD, Ctx, LocalCtx#key_context.manifest).

%% @doc Final step of {@link forbidden/2}: Authentication succeeded,
%% now perform ACL check to verify access permission.
check_permission('GET', RD, Ctx, notfound) ->
    {{halt, 404}, maybe_log_user(RD, Ctx), Ctx};
check_permission('HEAD', RD, Ctx, notfound) ->
    {{halt, 404}, maybe_log_user(RD, Ctx), Ctx};
check_permission(_, RD, Ctx=#context{requested_perm=RequestedAccess,local_context=LocalCtx}, Mfst) ->
    #key_context{bucket=Bucket} = LocalCtx,
    RiakPid = Ctx#context.riakc_pid,
    case Ctx#context.user of
        undefined ->
            User = CanonicalId = undefined;
        User ->
            CanonicalId = User?RCS_USER.canonical_id
    end,
    case Mfst of
        notfound ->
            ObjectAcl = undefined;
        _ ->
            ObjectAcl = Mfst?MANIFEST.acl
    end,
    case riak_cs_acl:object_access(Bucket,
                                   ObjectAcl,
                                   RequestedAccess,
                                   CanonicalId,
                                   RiakPid) of
        true ->
            %% actor is the owner
            AccessRD = riak_cs_access_logger:set_user(User, RD),
            UserStr = User?RCS_USER.canonical_id,
            UpdLocalCtx = LocalCtx#key_context{owner=UserStr},
            {false, AccessRD, Ctx#context{local_context=UpdLocalCtx}};
        {true, OwnerId} ->
            %% bill the owner, not the actor
            AccessRD = riak_cs_access_logger:set_user(OwnerId, RD),
            UpdLocalCtx = LocalCtx#key_context{owner=OwnerId},
            {false, AccessRD, Ctx#context{local_context=UpdLocalCtx}};
        false ->
            %% ACL check failed, deny access
            riak_cs_wm_utils:deny_access(RD, Ctx)
    end.

%% @doc Only set the user for the access logger to catch if there is a
%% user to catch.
maybe_log_user(RD, Context) ->
    case Context#context.user of
        undefined ->
            RD;
        User ->
            riak_cs_access_logger:set_user(User, RD)
    end.

%% @doc Get the list of methods this resource supports.
-spec allowed_methods() -> [atom()].
allowed_methods() ->
    ['HEAD', 'GET', 'PUT'].


-spec content_types_provided(term(), term()) ->
    {[{string(), atom()}], term(), term()}.
content_types_provided(RD, Ctx=#context{local_context=LocalCtx,
                                        riakc_pid=RiakcPid}) ->
    Mfst = LocalCtx#key_context.manifest,
    %% TODO:
    %% As I understand S3, the content types provided
    %% will either come from the value that was
    %% last PUT or, from you adding a
    %% `response-content-type` header in the request.
    Method = wrq:method(RD),
    if Method == 'GET'; Method == 'HEAD' ->
            UpdLocalCtx = riak_cs_wm_utils:ensure_doc(LocalCtx, RiakcPid),
            ContentType = binary_to_list(Mfst?MANIFEST.content_type),
            case ContentType of
                _ ->
                    UpdCtx = Ctx#context{local_context=UpdLocalCtx},
                    {[{ContentType, produce_body}], RD, UpdCtx}
            end;
       true ->
            %% TODO this shouldn't ever be called, it's just to
            %% appease webmachine
            {[{"text/plain", produce_body}], RD, Ctx}
    end.

-spec content_types_accepted(term(), term()) ->
    {[{string(), atom()}], term(), term()}.
content_types_accepted(RD, Ctx=#context{local_context=LocalCtx0}) ->
    case wrq:get_req_header("Content-Type", RD) of
        undefined ->
            DefaultCType = "application/octet-stream",
            LocalCtx = LocalCtx0#key_context{putctype=DefaultCType},
            {[{DefaultCType, accept_body}],
             RD,
             Ctx#context{local_context=LocalCtx}};
        %% This was shamelessly ripped out of
        %% https://github.com/basho/riak_kv/blob/0d91ca641a309f2962a216daa0cee869c82ffe26/src/riak_kv_wm_object.erl#L492
        CType ->
            {Media, _Params} = mochiweb_util:parse_header(CType),
            case string:tokens(Media, "/") of
                [_Type, _Subtype] ->
                    %% accept whatever the user says
                    LocalCtx = LocalCtx0#key_context{putctype=Media},
                    {[{Media, accept_body}], RD, Ctx#context{local_context=LocalCtx}};
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
   

-spec produce_body(term(), term()) -> {iolist()|binary(), term(), term()}.
produce_body(RD, Ctx=#context{local_context=LocalCtx,
                              requested_perm='READ_ACP',
                              start_time=StartTime,
                              user=User}) ->
    #key_context{get_fsm_pid=GetFsmPid, manifest=Mfst} = LocalCtx,
    {Bucket, File} = Mfst?MANIFEST.bkey,
    BFile_str = [Bucket, $,, File],
    UserName = riak_cs_wm_utils:extract_name(User),
    riak_cs_wm_dtrace:dt_object_entry(?MODULE, <<"object_get_acl">>, 
                                      [], [UserName, BFile_str]),
    riak_cs_get_fsm:stop(GetFsmPid),
    ok = riak_cs_stats:update_with_start(object_get_acl, StartTime),
    Acl = Mfst?MANIFEST.acl,
    case Acl of
        undefined ->
            riak_cs_wm_dtrace:dt_object_return(?MODULE, <<"object_get_acl">>, 
                                               [-1], [UserName, BFile_str]),
            {riak_cs_acl_utils:empty_acl_xml(), RD, Ctx};
        _ -> 
            riak_cs_wm_dtrace:dt_object_return(?MODULE, <<"object_get_acl">>, 
                                               [-2], [UserName, BFile_str]),
            {riak_cs_acl_utils:acl_to_xml(Acl), RD, Ctx}
    end.        

-spec accept_body(term(), term()) ->
    {boolean() | {halt, term()}, term(), term()}.
accept_body(RD, Ctx=#context{local_context=#key_context{get_fsm_pid=GetFsmPid, 
                                                        manifest=Mfst, 
                                                        owner=Owner, 
                                                        key=KeyStr,
                                                        bucket=Bucket},
                             user=User,
                             requested_perm='WRITE_ACP',
                             riakc_pid=RiakPid})  when Bucket /= undefined, 
                                                        KeyStr /= undefined, 
                                                        Mfst /= undefined, 
                                                        RiakPid /= undefined ->
    BFile_str = [Bucket, $,, KeyStr],
    UserName = riak_cs_wm_utils:extract_name(User),
    riak_cs_wm_dtrace:dt_object_entry(?MODULE, <<"object_put_acl">>, 
                                      [], [UserName, BFile_str]),
    riak_cs_get_fsm:stop(GetFsmPid),
    Body = binary_to_list(wrq:req_body(RD)),
    case Body of
        [] ->
            %% Check for `x-amz-acl' header to support
            %% the use of a canned ACL.
            Acl = riak_cs_acl_utils:canned_acl(
                    wrq:get_req_header("x-amz-acl", RD),
                    {User?RCS_USER.display_name,
                     User?RCS_USER.canonical_id,
                     User?RCS_USER.key_id},
                    Owner,
                    RiakPid);
        _ ->
            Acl = riak_cs_acl_utils:acl_from_xml(Body,
                                                   User?RCS_USER.key_id,
                                                   RiakPid)
    end,
    %% Write new ACL to active manifest
    Key = list_to_binary(KeyStr),
    case riak_cs_utils:set_object_acl(Bucket, Key, Mfst, Acl, RiakPid) of
        ok ->
            riak_cs_wm_dtrace:dt_object_return(?MODULE, <<"object_put_acl">>, 
                                               [200], [UserName, BFile_str]),
            {{halt, 200}, RD, Ctx};
        {error, Reason} ->
            Code = riak_cs_s3_response:status_code(Reason),
            riak_cs_wm_dtrace:dt_object_return(?MODULE, <<"object_put_acl">>, 
                                               [Code], [UserName, BFile_str]),
            riak_cs_s3_response:api_error(Reason, RD, Ctx)
    end.


