%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_wm_service).

-export([init/1,
         service_available/2,
         forbidden/2,
         content_types_provided/2,
         malformed_request/2,
         to_json/2,
         allowed_methods/2]).

-include("riak_moss.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

init(Config) ->
    %% Check if authentication is disabled and
    %% set that in the context.
    AuthBypass = proplists:get_value(auth_bypass, Config),
    {ok, #context{auth_bypass=AuthBypass}}.

-spec service_available(term(), term()) -> {true, term(), term()}.
service_available(RD, Ctx) ->
    riak_moss_wm_utils:service_available(RD, Ctx).

-spec malformed_request(term(), term()) -> {false, term(), term()}.
malformed_request(RD, Ctx) ->
    {false, RD, Ctx}.

%% @doc Check to see if the user is
%%      authenticated. Normally with HTTP
%%      we'd use the `authorized` callback,
%%      but this is how S3 does things.
forbidden(RD, Ctx=#context{auth_bypass=AuthBypass}) ->
    AuthHeader = wrq:get_req_header("authorization", RD),
    case riak_moss_wm_utils:parse_auth_header(AuthHeader, AuthBypass) of
        {ok, AuthMod, Args} ->
            case AuthMod:authenticate(RD, Args) of
                {ok, unknown} ->
                    %% this resource doesn't support
                    %% anonymous users
                    {true, RD, Ctx};
                {ok, User} ->
                    %% Authentication succeeded
                    {false, RD, Ctx#context{user=User}};
                {error, _Reason} ->
                    %% Authentication failed, deny access
                    {true, RD, Ctx}
            end
    end.

%% @doc Get the list of methods this resource supports.
-spec allowed_methods(term(), term()) -> {[atom()], term(), term()}.
allowed_methods(RD, Ctx) ->
    %% TODO:
    %% The docs seem to suggest GET is the only
    %% allowed method. It's worth checking to see
    %% if HEAD is supported too.
    {['GET'], RD, Ctx}.

-spec content_types_provided(term(), term()) ->
    {[{string(), atom()}], term(), term()}.
content_types_provided(RD, Ctx) ->
    %% TODO:
    %% This needs to be xml soon, but for now
    %% let's do json.
    {[{"application/json", to_json}], RD, Ctx}.


%% TODO:
%% This spec will need to be updated
%% when we change this to allow streaming
%% bodies.
-spec to_json(term(), term()) ->
    {iolist(), term(), term()}.

to_json(RD, Ctx=#context{user=User}) ->
    Buckets = riak_moss_riakc:get_buckets(User),
    %% we use list_to_binary because
    %% mochijson2 will convert binaries
    %% to strings
    BucketNames = [list_to_binary(B#moss_bucket.name) || B <- Buckets],
    {mochijson2:encode(BucketNames), RD, Ctx}.

-ifdef(TEST).
test_test() ->
    application:set_env(riak_moss, moss_ip, "127.0.0.1"),
    application:set_env(riak_moss, moss_port, 8080),
    %% Start erlang node
    application:start(sasl),
    TestNode = list_to_atom("testnode" ++ integer_to_list(element(3, now()))),
    net_kernel:start([TestNode, shortnames]),
    application:start(lager),
    application:start(riakc),
    application:start(inets),
    application:start(mochiweb),
    application:start(crypto),
    application:start(webmachine),
    application:start(riak_moss),

    BucketNames = ["foo", "bar", "baz"],
    UserName = "fooser",
    {ok, User} = riak_moss_riakc:create_user(UserName),
    KeyID = User#moss_user.key_id,
    [riak_moss_riakc:create_bucket(KeyID, Name) || Name <- BucketNames],
    {ok, UpdatedUser} = riak_moss_riakc:get_user(User#moss_user.key_id),
    CorrectJsonBucketNames = mochijson2:encode([list_to_binary(Name) || Name <- lists:reverse(BucketNames)]),
    {ResultToTest, _, _} = to_json(fake_rd, #context{user=UpdatedUser}),
    ?assertEqual(CorrectJsonBucketNames, ResultToTest).
-endif.
