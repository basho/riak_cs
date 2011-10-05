%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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
%% -------------------------------------------------------------------

-module(riak_moss_resource).

-export([init/1,
         service_available/2,
         content_types_accepted/2,
         content_types_provided/2,
         accept_body/2,
         malformed_request/2,
         produce_body/2,
         delete_resource/2,
         allowed_methods/2]).

-include("riak_moss.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-define(ROOT_HOST, "s3.amazonaws.com").

-record(ctx, {
          host,
          method,
          resource,
          subresource,
          mode,
          pid,
          bucket,
          key,
          user}).

parse_auth_header("AWS " ++ Key) ->
    case string:tokens(Key, ":") of
        [KeyId, KeyData] ->
            {ok, KeyId, KeyData};
        Other -> Other
    end.


canonicalize_resource(RD) ->
    lists:flatten(moss_auth:canonicalize_resource(RD) ++
        moss_auth:canonicalize_qs(lists:sort(wrq:req_qs(RD)))).


make_request(RD, Ctx, User) ->
    parse_resource(RD,
                   Ctx#ctx{
                     resource=canonicalize_resource(RD),
                     method=wrq:method(RD),
                     host=moss_auth:bucket_from_host(wrq:get_req_header(host, RD)),
                     subresource=string:str(canonicalize_resource(RD), "?") /= 0,
                     user=User}).

parse_resource(RD, Ctx=#ctx{host=undefined, mode=service}) ->
    Ctx;
parse_resource(RD, Ctx=#ctx{host=Host, mode=service}) ->
    Ctx#ctx{mode=bucket,bucket=Host};
parse_resource(RD, Ctx=#ctx{host=undefined, mode=bucket}) ->
    Ctx#ctx{bucket=wrq:path_info(bucket, RD)};
parse_resource(RD, Ctx=#ctx{host=Host, mode=bucket}) ->
    Ctx#ctx{mode=key,bucket=Host,key=wrq:path_info(bucket, RD)};
parse_resource(RD, Ctx=#ctx{host=undefined, mode=key}) ->
    Ctx#ctx{mode=key,bucket=wrq:path_info(bucket, RD),
            key=wrq:path_info(key, RD)}.


make_context(RD, Ctx) ->
    case parse_auth_header(wrq:get_req_header("authorization", RD)) of
        {ok, KeyId, Signature} ->
            case riak_moss_riakc:get_user(KeyId) of
                {ok, User} ->
                    Authed = moss_auth:check_auth(User#rs3_user.key_id,
                                                  User#rs3_user.key_data,
                                                  RD,
                                                  Signature),
                    {Authed, make_request(RD, Ctx, User)};
                _ ->
                    {api_error, invalid_access_key_id}
            end;
        Error -> Error
    end.

write_error(StatusCode, Code, Message, RD, Ctx) ->
    ReqID = <<"">>,
    Msg = [
           <<"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n">>,
           <<"<Error>\r\n">>,
           <<"  <Code>">>, Code, <<"</Code>\r\n">>,
           <<"  <Message>">>, Message, <<"</Message>\r\n">>,
           <<"  <Resource>">>, wrq:path(RD), <<"</Resource>\r\n">>,
           <<"  <RequestId>">>, ReqID, <<"</RequestId>\r\n">>,
           <<"</Error>\r\n">>],
    {{halt, StatusCode}, wrq:set_resp_body(Msg, RD), Ctx}.

error_message(invalid_access_key_id) ->
    <<"The AWS Access Key Id you provided does not exist in our records.">>;
error_message(access_denied) ->
    <<"Access Denied">>;
error_message(bucket_not_empty) ->
    <<"The bucket you tried to delete is not empty.">>.

api_error(E=invalid_access_key_id, RD, Ctx) ->
    write_error(403, "InvalidAccessKeyId", error_message(E), RD, Ctx);
api_error(E=access_denied, RD, Ctx) ->
    write_error(403, "AccessDenied", error_message(E), RD, Ctx);
api_error(E=bucket_not_empty, RD, Ctx) ->
    write_error(409, "BucketNotEmpty", error_message(E), RD, Ctx).

init([{mode, Mode}]) ->
    {ok, #ctx{mode=Mode}}.

service_available(RD, Ctx) ->
    case make_context(RD, Ctx) of
        {true, NewCtx} ->
            {true,
             RD,
             NewCtx};
        {false, NewCtx} ->
            api_error(access_denied, RD, NewCtx);
        {api_error, APIError} ->
            api_error(APIError, RD, Ctx);
        Error ->
            {false,
             wrq:set_resp_body(
               io_lib:format("Unable to connect to Riak: ~p~n", [Error]),
               wrq:set_resp_header("Content-Type", "text/plain", RD)),
             Ctx}
    end.

malformed_request(RD, Ctx) ->
    {false, RD, Ctx}.

%% @spec allowed_methods(reqdata(), context()) ->
%%          {[method()], reqdata(), context()}
%% @doc Get the list of methods this resource supports.
%%      Properties allows HEAD, GET, and PUT.
allowed_methods(RD, Ctx) ->
    {['HEAD', 'GET', 'PUT', 'DELETE'], RD, Ctx}.


%% @spec content_types_accepted(reqdata(), context()) ->
%%          {[{ContentType::string(), Acceptor::atom()}],
%%           reqdata(), context()}
content_types_accepted(RD, Ctx) ->
    case wrq:get_req_header("content-type", RD) of
        undefined ->
            {[{"application/octet-stream", accept_body}], RD, Ctx};
        CType ->
            {[{CType, accept_body}], RD, Ctx}
    end.

content_types_provided(RD, Ctx) ->
    {[{"application/octet-stream", produce_body}], RD, Ctx}.

delete_resource(RD, Ctx=#ctx{subresource=true}) ->
    {{halt, 204}, wrq:set_resp_header("Content-Type", "text/plain", RD), Ctx};
delete_resource(RD, Ctx=#ctx{mode=bucket, bucket=Bucket, user=User}) ->
    {ok, C} = riak_moss:moss_client(User),
    Keys = C:list_bucket(Bucket),
    io:format("keys: ~p~n", [Keys]),
    case Keys of
        [] ->
            OldBuckets = User#rs3_user.buckets,
            NewUser = User#rs3_user{
                        buckets=lists:keydelete(Bucket, 2, OldBuckets)},
            riak_moss_riakc:do_save_user(NewUser),
            {true, RD, Ctx};
        _ ->
            api_error(bucket_not_empty, RD, Ctx)
    end;
delete_resource(RD, Ctx=#ctx{user=User, bucket=Bucket, key=Key}) ->
    {ok, C} = riak_moss:moss_client(User),
    case C:delete_object(Bucket, Key) of
        ok ->
            {true, RD, Ctx};
        O ->
            {false, RD, Ctx}
    end.

accept_body(ReqData, Ctx=#ctx{bucket=Bucket,key=undefined,method='PUT',user=User}) ->
    ok = riak_moss_riakc:create_bucket(User#rs3_user.key_id, Bucket),
    RD2 = wrq:set_resp_header("ETag", "\"" ++ binary_to_hexlist(crypto:md5(wrq:req_body(ReqData))) ++ "\"", ReqData),
    {{halt, 200}, RD2, Ctx};
accept_body(ReqData, Ctx=#ctx{bucket=Bucket,key=Key,method='PUT',user=User}) ->
    RD2 = wrq:set_resp_header("ETag", "\"" ++ binary_to_hexlist(crypto:md5(wrq:req_body(ReqData))) ++ "\"", ReqData),
    {ok, C} = riak_moss:moss_client(User),
    case C:put_object(Bucket, Key, wrq:req_body(ReqData)) of
        ok ->
            C:stop(),
            {{halt, 200}, RD2, Ctx};
        _ ->
            C:stop(),
            {{halt, 500}, RD2, Ctx}
    end.

produce_body(RD, Ctx=#ctx{subresource=true}) ->
    {<<>>, wrq:set_resp_header("Content-Type", "text/plain", RD), Ctx};
produce_body(ReqData, Ctx=#ctx{user=User, bucket=undefined, key=undefined}) ->
    %% get user id
    %% list buckets and filter
    Name = User#rs3_user.name,
    KeyId = User#rs3_user.key_id,
    XmlHead = [
<<"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n">>,
<<"<ListAllMyBucketsResult xmlns=\"http://doc.s3.amazonaws.com/2006-03-01\">\r\n">>,
<<"  <Owner>\r\n">>],

    XmlMid = [
<<"  </Owner>\r\n">>,
<<"  <Buckets>\r\n">>],

    XmlTail = [
<<"  </Buckets>\r\n">>,
<<"</ListAllMyBucketsResult>\r\n">>],
    XmlOwner = [
<<"    <ID>">>, KeyId, <<"</ID>\r\n">>,
<<"    <DisplayName>">>, Name, <<"</DisplayName>\r\n">>],

    XmlBuckets = [ [<<"    <Bucket><Name>">>, B#rs3_bucket.name, <<"</Name><CreationDate>">>, B#rs3_bucket.creation_date, <<"</CreationDate></Bucket>\r\n">>] ||
                     B <- User#rs3_user.buckets],

    Xml = [XmlHead, XmlOwner, XmlMid, XmlBuckets, XmlTail],
    {Xml, ReqData, Ctx};
produce_body(RD, Ctx=#ctx{user=User, bucket=Bucket, key=undefined}) ->
    XmlHead = [
<<"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n">>,
<<"<ListBucketResult xmlns=\"http://doc.s3.amazonaws.com/2006-03-01\">\r\n">>,
<<"  <Name>">>, Bucket, <<"</Name>\r\n">>,
<<"  <IsTruncated>false</IsTruncated>\r\n">>],

    XmlTail = [
<<"</ListBucketResult>\r\n">>],

    KeyID = User#rs3_user.key_id,
    Name = User#rs3_user.name,

    XmlOwner = [
<<"    <Owner>\r\n">>,
<<"      <ID>">>, KeyID, <<"</ID>\r\n">>,
<<"      <DisplayName>">>, Name, <<"</DisplayName>\r\n">>,
<<"    </Owner>\r\n">>],

    {ok, C} = riak_moss:moss_client(User),
    Keys = C:list_bucket(Bucket),
    C:stop(),
    XmlContents = [ [ <<"  <Contents>\r\n">>, <<"    <Key>">>, mochiweb_util:unquote(Key), <<"</Key>\r\n">>, <<"    <LastModified>">>, httpd_util:rfc1123_date(), <<"</LastModified>\r\n">>, <<"    <Size>">>, integer_to_list(Size), <<"</Size>\r\n">>, XmlOwner, <<"  </Contents>\r\n">> ]
                    || {Key, Size} <- Keys],
    Xml = [XmlHead, XmlContents, XmlTail],
    {Xml, RD, Ctx};
produce_body(ReqData, Ctx=#ctx{user=User, bucket=Bucket, key=Key}) ->
    {ok, C} = riak_moss:moss_client(User),
    case C:get_object(Bucket, Key) of
        {ok, Obj} ->
            {riakc_obj:get_value(Obj), ReqData, Ctx};
        _ ->
            {<<>>, ReqData, Ctx}
    end.

%% @spec (binary()) -> string()
%% @doc Convert the passed binary into a string where the numbers are represented in hexadecimal (lowercase and 0 prefilled).
binary_to_hexlist(Bin) ->
    XBin =
        [ begin
              Hex = erlang:integer_to_list(X, 16),
              if
                  X < 16 ->
                      lists:flatten(["0" | Hex]);
                  true ->
                      Hex
              end
          end || X <- binary_to_list(Bin)],
    string:to_lower(lists:flatten(XBin)).
