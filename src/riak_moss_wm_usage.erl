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

-ifdef(TEST).
-ifdef(EQC).
-compile([export_all]).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("webmachine/include/webmachine.hrl").
-include("rts.hrl").

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
mochijson_access({Access, Errors}) ->
    orddict:fold(
      fun(Node, Samples, Acc) ->
              [{struct, [{node, Node},
                         {samples, [{struct, S} || S <- Samples]}]}
               |Acc]
      end,
      [{struct, [{errors, [ {struct, mochijson_sample_error(E)}
                            || E <- Errors ]}]}],
      Access).

mochijson_sample_error({{Start, End}, Reason}) ->
    [{?START_TIME, rts:iso8601(Start)},
     {?END_TIME, rts:iso8601(End)},
     {<<"reason">>, mochijson_reason(Reason)}].

mochijson_reason(Reason) ->
    if is_atom(Reason) -> atom_to_binary(Reason, latin1);
       is_binary(Reason) -> Reason;
       true -> list_to_binary(io_lib:format("~p", [Reason]))
    end.

mochijson_storage(Msg) when is_atom(Msg) ->
    Msg;
mochijson_storage({Storage, Errors}) ->
    [ mochijson_storage_sample(S) || S <- Storage ]
        ++ [{struct, [{errors, [mochijson_sample_error(E)
                                || E <- Errors]}]}].

mochijson_storage_sample(Sample) ->
    {struct, Sample}.

%% XML Production %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
produce_xml(RD, Ctx) ->
    Access = maybe_access(RD, Ctx),
    Storage = maybe_storage(RD, Ctx),
    Doc = [{'Usage', [{'Access', xml_access(Access)},
                      {'Storage', xml_storage(Storage)}]}],
    {riak_moss_s3_response:export_xml(Doc), RD, Ctx}.

xml_access(Msg) when is_atom(Msg) ->
    [atom_to_list(Msg)];
xml_access({Access, Errors}) ->
    orddict:fold(
      fun(Node, Samples, Acc) ->
              [{'Node', [{name, Node}],
                [xml_sample(S) || S <- Samples]}
               |Acc]
      end,
      [{'Errors', [ xml_sample_error(E) || E <- Errors ]}],
      Access).

xml_sample(Sample) ->
    {value, {?START_TIME,S}, SampleS} =
        lists:keytake(?START_TIME, 1, Sample),
    {value, {?END_TIME,E}, Rest} =
        lists:keytake(?END_TIME, 1, SampleS),

    {'Sample', [{start, S}, {'end', E}],
     [{xml_name(K), [mochinum:digits(V)]} || {K, V} <- Rest]}.

xml_sample_error({{Start, End}, Reason}) ->
    %% cheat to make errors structured exactly like samples
    FakeSample = [{?START_TIME, rts:iso8601(Start)},
                  {?END_TIME, rts:iso8601(End)}],
    {Tag, Props, Contents} = xml_sample(FakeSample),

    XMLReason = xml_reason(Reason),
    {Tag, Props, [{'Reason', [XMLReason]}|Contents]}.

xml_name(<<"bytes_out">>)  -> 'BytesOut';
xml_name(<<"bytes_in">>)   -> 'BytesIn';
xml_name(Other)            -> binary_to_atom(Other, latin1).

xml_reason(Reason) ->
    [if is_atom(Reason) -> atom_to_binary(Reason, latin1);
       is_binary(Reason) -> Reason;
       true -> io_lib:format("~p", [Reason])
     end].

xml_storage(Msg) when is_atom(Msg) ->
    [atom_to_list(Msg)];
xml_storage({Storage, Errors}) ->
    [xml_sample(S) || S <- Storage]
        ++ [{'Errors', [xml_sample_error(E) || E <- Errors ]}].
    
%% Internals %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
user_key(RD) ->
    mochiweb_util:unquote(wrq:path_info(user, RD)).

maybe_access(RD, Ctx) ->
    usage_if(RD, Ctx, "a", riak_moss_access).

maybe_storage(RD, Ctx) ->
    usage_if(RD, Ctx, "b", riak_moss_storage).

usage_if(RD, #ctx{riak=Riak, start_time=Start, end_time=End},
         QParam, Module) ->
    case true_param(RD, QParam) of
        true ->
            Module:get_usage(Riak, user_key(RD), Start, End);
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
            datetime(TimeString)
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

%% @doc Produce a datetime tuple from a ISO8601 string
-spec datetime(binary()|string()) -> {ok, calendar:datetime()} | error.
datetime(Binary) when is_binary(Binary) ->
    datetime(binary_to_list(Binary));
datetime(String) when is_list(String) ->
    case catch io_lib:fread("~4d~2d~2dT~2d~2d~2dZ", String) of
        {ok, [Y,M,D,H,I,S], _} ->
            {ok, {{Y,M,D},{H,I,S}}};
        %% TODO: match {more, _, _, RevList} to allow for shortened
        %% month-month/etc.
        _ ->
            error
    end.

-ifdef(TEST).
-ifdef(EQC).


datetime_test() ->
    true = eqc:quickcheck(datetime_invalid_prop()).

%% make sure that datetime correctly returns 'error' for invalid
%% iso8601 date strings
datetime_invalid_prop() ->
    ?FORALL(L, list(char()),
            case datetime(L) of
                {{_,_,_},{_,_,_}} ->
                    %% really, we never expect this to happen, given
                    %% that a random string is highly unlikely to be
                    %% valid iso8601, but just in case...
                    valid_iso8601(L);
                error ->
                    not valid_iso8601(L)
            end).

%% a string is considered valid iso8601 if it is of the form
%% ddddddddZddddddT, where d is a digit, Z is a 'Z' and T is a 'T'
valid_iso8601(L) ->
    length(L) == 4+2+2+1+2+2+2+1 andalso
        string:chr(L, $Z) == 4+2+2+1 andalso
        lists:all(fun is_digit/1, string:substr(L, 1, 8)) andalso
        string:chr(L, $T) == 16 andalso
        lists:all(fun is_digit/1, string:substr(L, 10, 15)).

is_digit(C) ->
    C >= $0 andalso C =< $9.

-endif. % EQC
-endif. % TEST
