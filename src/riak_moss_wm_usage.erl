%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Webmachine resource for serving usage stats.
%%
%% `GET /usage/USER_KEY?s=STARTTIME&e=ENDTIME&a=ACCESS&b=STORAGE'
%%
%% Default `s' is the beginning of the previous period.  If no `e' is
%% given, the return only includes data for the period containing `s'.
%%
%% The `a' and `b' parameters default to `false'. Set `a' to `true' to
%% get access usage stats.  Set `b' to `true' to get storage usage
%% stats.
%%
%% This service is only available if a connection to Riak can be made.
%%
%% This resource only exists if the user named by the key exists.
%%
%% JSON response looks like:
%% ```
%% {"access":[{"node":"riak_moss@127.0.0.1",
%%             "samples":[{"start_time":"20120113T194510Z",
%%                         "end_time":"20120113T194520Z",
%%                         "bytes_out":22,
%%                         "bytes_in":44},
%%                        {"start_time":"20120113T194520Z",
%%                         "end_time":"20120113T194530Z",
%%                         "bytes_out":66,
%%                         "bytes_in":88}
%%                       ]}
%%           ],
%%  "storage":"not_requested"}
%% '''
%%
%% XML response looks like:
%% ```
%% <?xml version="1.0" encoding="UTF-8"?>
%%   <Usage>
%%     <Access>
%%       <Node name="riak_moss@127.0.0.1">
%%         <Sample start="20120113T194510Z" end="20120113T194520Z">
%%           <BytesOut>22</BytesOut>
%%           <BytesIn>44</BytesIn>
%%         </Sample>
%%         <Sample start="20120113T194520Z" end="20120113T194530Z">
%%           <BytesOut>66</BytesOut>
%%           <BytesIn>88</BytesIn>
%%         </Sample>
%%       </Node>
%%     </Access>
%%   <Storage>not_requested</Storage>
%% </Usage>
%% '''
%%
%% TODO: what auth is needed here?
-module(riak_moss_wm_usage).

-export([
         init/1,
         service_available/2,
         malformed_request/2,
         resource_exists/2,
         content_types_provided/2,
         produce_json/2,
         produce_xml/2
        ]).

-include_lib("webmachine/include/webmachine.hrl").

-define(JSON_TYPE, "application/json").
-define(XML_TYPE, "application/xml").

-record(ctx, {
          riak :: pid(),
          user :: riak_moss:moss_user(),
          start_time :: calendar:datetime(),
          end_time :: calendar:datetime()
         }).

init([]) ->
    {ok, #ctx{}}.

service_available(RD, Ctx) ->
    case riak_moss_utils:riak_connection() of
        {ok, Riak} ->
            {true, RD, Ctx#ctx{riak=Riak}};
        {error, _} ->
            {false, error_msg(RD, <<"Usage database connection failed">>), Ctx}
    end.

malformed_request(RD, Ctx) ->
    case parse_start_time(RD) of
        {ok, Start} ->
            case parse_end_time(RD, Start) of
                {ok, End} ->
                    {false, RD,
                     Ctx#ctx{start_time=lists:min([Start, End]),
                             end_time=lists:max([Start, End])}};
                error ->
                    {true, error_msg(RD, <<"Invalid end-time format">>), Ctx}
            end;
        error ->
            {true, error_msg(RD, <<"Invalid start-time format">>), Ctx}
    end.

resource_exists(RD, #ctx{riak=Riak}=Ctx) ->
    case riak_moss_utils:get_user(user_key(RD), Riak) of
        {ok, User} ->
            {true, RD, Ctx#ctx{user=User}};
        {error, _} ->
            {false, error_msg(RD, <<"Unknown user">>), Ctx}
    end.

content_types_provided(RD, Ctx) ->
    {[{?JSON_TYPE, produce_json},
      {?XML_TYPE, produce_xml}],
     RD, Ctx}.

%% JSON Production %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
produce_json(RD, Ctx) ->
    Access = maybe_access(RD, Ctx),
    Storage = maybe_storage(RD, Ctx),
    MJ = {struct, [{access, mochijson_access(Access)},
                   {storage, mochijson_storage(Storage)}]},
    {mochijson2:encode(MJ), RD, Ctx}.

mochijson_access(Msg) when is_atom(Msg) ->
    Msg;
mochijson_access(Access) ->
    orddict:fold(
      fun(Node, Samples, Acc) ->
              [{struct, [{node, Node},
                         {samples, [{struct, S} || S <- Samples]}]}
               |Acc]
      end,
      [],
      Access).

mochijson_storage(Msg) when is_atom(Msg) ->
    Msg;
mochijson_storage(Storage) ->
    [ {struct, S} || S <- Storage ].

%% XML Production %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
produce_xml(RD, Ctx) ->
    Access = maybe_access(RD, Ctx),
    Storage = maybe_storage(RD, Ctx),
    Doc = [{'Usage', [{'Access', xml_access(Access)},
                      {'Storage', xml_storage(Storage)}]}],
    {riak_moss_s3_response:export_xml(Doc), RD, Ctx}.

xml_access(Msg) when is_atom(Msg) ->
    [atom_to_list(Msg)];
xml_access(Access) ->
    orddict:fold(
      fun(Node, Samples, Acc) ->
              [{'Node', [{name, Node}],
                [xml_access_sample(S) || S <- Samples]}
               |Acc]
      end,
      [],
      Access).

xml_access_sample(Sample) ->
    {value, {<<"start_time">>,S}, SampleS} =
        lists:keytake(<<"start_time">>, 1, Sample),
    {value, {<<"end_time">>,E}, Rest} =
        lists:keytake(<<"end_time">>, 1, SampleS),

    {'Sample', [{start, S}, {'end', E}],
     [{xml_name(K), [mochinum:digits(V)]} || {K, V} <- Rest]}.

xml_name(<<"bytes_out">>)  -> 'BytesOut';
xml_name(<<"bytes_in">>)   -> 'BytesIn';
xml_name(Other)            -> binary_to_atom(Other, latin1).

xml_storage(Msg) when is_atom(Msg) ->
    [atom_to_list(Msg)];
xml_storage(Storage) ->
    [xml_storage_sample(S) || S <- Storage].

xml_storage_sample(Sample) ->
    {value, {<<"time">>, T}, SampleS} =
        lists:keytake(<<"time">>, 1, Sample),
    {value, {<<"bytes">>, Y}, _} =
        lists:keytake(<<"bytes">>, 1, SampleS),

    {'Sample', [{time, T}], [{bytes, [mochinum:digits(Y)]}]}.

%% Internals %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
user_key(RD) ->
    mochiweb_util:unquote(wrq:path_info(user, RD)).

maybe_access(RD, #ctx{riak=Riak, start_time=Start, end_time=End}) ->
    case true_param(RD, "a") of
        true ->
            riak_moss_access:get_usage(user_key(RD), Start, End, Riak);
        false ->
            not_requested
    end.

%% TODO: this is just "now", not the span they asked for
maybe_storage(RD, #ctx{riak=Riak}) ->
    case true_param(RD, "b") of
        true  ->
            case riak_moss_storage:sum_user(Riak, user_key(RD)) of
                {ok, Buckets} ->
                    [[{<<"time">>, riak_moss_access:iso8601(
                                     calendar:universal_time())},
                      {<<"bytes">>, lists:sum([ Y || {_B, Y} <- Buckets,
                                                     is_integer(Y) ])}]];
                {error, _Error} ->
                    error
            end;
        false ->
            not_requested
    end.

true_param(RD, Param) ->
    lists:member(wrq:get_qs_value(Param, RD),
                 ["","t","true","1","y","yes"]).

parse_start_time(RD) ->
    time_param(RD, "s", calendar:universal_time()).

parse_end_time(RD, StartTime) ->
    time_param(RD, "e", StartTime).

time_param(RD, Param, Default) ->
    case wrq:get_qs_value(Param, RD) of
        undefined ->
            {ok, Default};
        TimeString ->
            riak_moss_access:datetime(TimeString)
    end.

error_msg(RD, Message) ->
    {CTP, _, _} = content_types_provided(RD, #ctx{}),
    PTypes = [Type || {Type,_Fun} <- CTP],
    AcceptHdr = wrq:get_req_header("accept", RD),
    case webmachine_util:choose_media_type(PTypes, AcceptHdr) of
        ?JSON_TYPE=Type ->
            Body = json_error_msg(Message);
        _ ->
            Type = ?XML_TYPE,
            Body = xml_error_msg(Message)
    end,
    wrq:set_resp_header("content-type", Type, wrq:set_resp_body(Body, RD)).

json_error_msg(Message) when is_list(Message) ->
    json_error_msg(list_to_binary(Message));
json_error_msg(Message) ->
    MJ = {struct, [{error, {struct, [{message, Message}]}}]},
    mochijson2:encode(MJ).

xml_error_msg(Message) when is_binary(Message) ->
    xml_error_msg(binary_to_list(Message));
xml_error_msg(Message) ->
    Doc = [{'Error', [{'Messsage', [Message]}]}],
    riak_moss_s3_response:export_xml(Doc).
