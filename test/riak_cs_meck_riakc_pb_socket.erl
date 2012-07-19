%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_meck_riakc_pb_socket).

-include("riak_moss.hrl").

-define(PBS, riakc_pb_socket).
-define(TAB, ?MODULE).

-compile(export_all).
-export([get_injectable_faults/0,
         setup_verbose/0, setup/0, teardown/0,
         user1/0, user1_details/0]).

%% Cut-and-paste from riakc_obj.erl, naughty but fun
-record(riakc_obj, {
          bucket,
          key,
          vclock,
          contents,
          updatemetadata,
          updatevalue
         }).

get_injectable_faults() ->
    lists:usort([Op || {Op, _Fun} <- get_expects()]).

setup_verbose() ->
    error_logger:warning_msg("Starting testing mode via ~s\n", [?MODULE]),
    setup(),
    riakc_pb_socket:put(x, riak_cs_meck_riakc_pb_socket:user1()).

setup() ->
    meck:new(?PBS, [passthrough]),
    Es = get_expects(),
    [meck:expect(?PBS, Name, Fun) || {Name, Fun} <- Es],
    ets:new(?TAB, [named_table, public, ordered_set]),

    %% Helpers to work around Stanchion
    meck:new(riak_moss_utils, [passthrough]),
    %% meck:expect(riak_moss_utils, create_bucket, fun create_bucket/5),
    meck:expect(riak_moss_utils, update_key_secret, fun(X) -> {gotcha, X} end),
    meck:expect(riak_moss_utils, bucket_fun, fun utils_bucket_fun/6),
    %% meck:expect(riak_moss_utils, delete_bucket, fun delete_bucket/4),
    meck:expect(riak_moss_utils, active_manifests, fun utils_active_manifests/3),
    StartFakeRiakC = fun() -> {ok, spawn(fun() -> timer:sleep(15000) end)} end,
    StopFakeRiakC = fun(_) -> ok end,
    meck:expect(riak_moss_utils, riak_connection, StartFakeRiakC),
    meck:expect(riak_moss_utils, close_riak_connection, StopFakeRiakC),

    meck:new(stanchion_utils, [passthrough]),
    meck:expect(stanchion_utils, riak_connection, StartFakeRiakC),
    meck:expect(stanchion_utils, close_riak_connection, StopFakeRiakC),

    ok.

get_expects() ->
    [{start, fun start/2}, {start, fun start/3},
     %% yes, same as start
     {start_link, fun start/2},  {start_link, fun start/3},
     {get, fun get/3}, {get, fun get/4}, {get, fun get/5},
     {put, fun put/2}, {put, fun put/3}, {put, fun put/4},
     {delete, fun delete/3}, {delete, fun delete/4},
     {delete, fun delete/5}, {list_keys, fun list_keys/2},
     {get_bucket, fun get_bucket/2}].

teardown() ->
    catch meck:unload(?PBS),
    catch meck:unload(riak_moss_utils),
    catch meck:unload(stanchion_utils),
    catch ets:delete(?TAB),
    ok.

start(Address, Port) ->
    maybe_fault(start, fun() -> start3(Address, Port, []) end).

start(Address, Port, Options) ->
    maybe_fault(start, fun() -> start3(Address, Port, Options) end).

start_link(Address, Port) ->
    maybe_fault(start_link, fun() -> start3(Address, Port, []) end).

start_link(Address, Port, Options) ->
    maybe_fault(start_link, fun() -> start3(Address, Port, Options) end).

start3(_Address, _Port, _Options) ->
    {ok, spawn(fun() -> timer:sleep(15000) end)}.

stop(_Pid) ->
    ok.

get(_Pid, Bucket, Key) ->
    get(_Pid, Bucket, Key, x).

get(_Pid, Bucket, Key, X) ->
    get(_Pid, Bucket, Key, X, x).

get(_Pid, Bucket, Key, _Options, _Timeout) ->
    NoFault =
        fun() ->
                case ets:lookup(?TAB, {Bucket, Key}) of
                    [] ->
                        {error, notfound};
                    [{_, Obj}] ->
                        {ok, Obj}
                end
        end,
    maybe_fault(get, NoFault).

put(_Pid, Obj) ->
    put(_Pid, Obj, x).

put(_Pid, Obj, X) ->
    put(_Pid, Obj, X, x).

put(_Pid, Obj0, _Options, _Timeout) ->
    NoFault =
        fun() ->
                Bucket = riakc_obj:bucket(Obj0),
                Key = riakc_obj:key(Obj0),
                V = riakc_obj:get_update_value(Obj0),
                MDL = dict:to_list(riakc_obj:get_update_metadata(Obj0)),
                MD = dict:from_list(lists:map(
                                      fun({<<"X-Riak-Meta">>, XRMList}) ->
                                              {<<"X-Riak-Meta">>, XRMList};
                                         (Else2) ->
                                              Else2
                                      end, MDL)),
                Obj = Obj0#riakc_obj{contents = [{MD, V}],
                                     updatemetadata = undefined,
                                     updatevalue = undefined},
                ets:insert(?TAB, {{Bucket, Key}, Obj}),
                ok
        end,
    maybe_fault(put, NoFault).

delete(_Pid, Bucket, Key) ->
    delete(_Pid, Bucket, Key, x).

delete(_Pid, Bucket, Key, X) ->
    delete(_Pid, Bucket, Key, X, x).

delete(_Pid, Bucket, Key, _Options, _Timeout) ->
    NoFault =
        fun() ->
                case ets:member(?TAB, {Bucket, Key}) of
                    false ->
                        {error, notfound};
                    true ->
                        ets:delete(?TAB, {Bucket, Key}),
                        ok
                end
        end,
    maybe_fault(delete, NoFault).

list_keys(_Pid, Bucket) ->
    NoFault =
        fun() ->
                {ok, [K || {{B, K}, _Obj} <- ets:tab2list(?TAB), B == Bucket]}
        end,
    maybe_fault(list_keys, NoFault).

get_bucket(_Pid, _Bucket) ->
    NoFault =
        fun() ->
                {ok,[{n_val,3},{allow_mult,true}]}
        end,
    maybe_fault(get_bucket, NoFault).

utils_bucket_fun(BucketOp, Bucket, ACL, KeyId, _AdminCreds, _StanchionData) ->
    fun() ->
            stanchion_utils:do_bucket_op(Bucket, list_to_binary(KeyId), ACL,
                                         BucketOp)
    end.

%% delete_bucket(User, _VClock, Bucket, _RiakPid) ->
%%     io:format("delete_bucket, "),
%%     KeyId = User?RCS_USER.key_id,
%%     stanchion_utils:do_bucket_op(Bucket, list_to_binary(KeyId), ?ACL{}, delete).

utils_active_manifests(ManifestBucket, Prefix, _RiakPid) ->
    PrefixLen = size(Prefix),
    Objs = [Obj || {{Bucket, Key}, Obj} <- ets:tab2list(?TAB),
                   Bucket == ManifestBucket,
                   begin <<KP:PrefixLen/binary, _/binary>> = Key,
                         KP == Prefix end],
    try
        %% Half of the map below is necessary: we need to call
        %% map_keys_and_manifests().  However, the ?PBS:get() call
        %% is there only for the reason to allow a fault-injection
        %% framework to have the option of interrupting our work
        %% (by screwing up a Riak client get()) so that we in turn
        %% can return an error tuple.
        AMs = lists:flatten(
                lists:map(
                  fun(O) ->
                          B = riakc_obj:bucket(O),
                          K = riakc_obj:key(O),
                          case ?PBS:get(foopid, B, K, [], infinity) of
                              {ok, _} ->
                                  riak_moss_utils:map_keys_and_manifests(
                                    O, unused, meck_testing);
                              Else ->
                                  throw({sim_riak_failure, Else})
                          end
                  end, Objs)),
        {ok, AMs}
    catch throw:{sim_riak_failure, Error} ->
            Error
    end.

maybe_fault(FaultType, NoFaultFun) ->
    io:format("~p, ", [FaultType]),
    case riak_cs_fault_injection:get_fault(get) of
        none ->
            NoFaultFun();
        Fault ->
            riak_cs_fault_injection:apply(Fault)
    end.

user1_details() ->
    [{display_name, "foobar"},
     {canonical_id, "59363e3467cea950d5889d1db9d291f4ee22c64083687ff59ecb24880ea6b767"},
     {key_id, "NP3ZEHK_H9MBSHEP2XWS"},
     {secret_key, "Nnc4WfGomD0XBAviP0fbr-yOiKRrhZH5KDX_Nw=="}].

user1() ->
    {riakc_obj,
     <<"moss.users">>,
     <<"NP3ZEHK_H9MBSHEP2XWS">>,
     <<107,206, 97,96,96, 96,202,96, 202,5,82, 28,202, 156,255,
       126,250, 247,52, 248,100, 48,37,114, 228,177, 50,4,173,
       79,59,201, 135,44, 245,93, 249,15,80, 74,24,40, 37,108,92,
       120,146, 47,11,0>>,
     [{{dict,3, 16,16,8, 80,48, {[],[], [],[], [],[], [],[], [],[],
        [],[], [],[], [],[]}, {{[],[], [],[], [],[], [],[], [],[],
        [[<<"X-Riak-VTag">>, 53, 53, 120, 100, 73, 119, 67, 85, 115,
        82, 120, 52, 73, 52, 56, 113, 53, 76, 100, 51, 65, 67]],
        [[<<"index">>, {"c_id_bin",
        "59363e3467cea950d5889d1db9d291f4ee22c64083687ff59ecb24880ea6b767"},
        {"email_bin", "foobar@foobar.com"}]], [],
        [[<<"X-Riak-Last-Modified">>| {1341, 962003, 474895}]], [],
        []}}}, <<131,104,9,100,0,11,114,99,115,95,117,115,101,114,95,118,50,107,0,7,102,111,111,32,98,97,114,107,0,6,102,111,111,98,97,114,107,0,17,102,111,111,98,97,114,64,102,111,111,98,97,114,46,99,111,109,107,0,20,78,80,51,90,69,72,75,95,72,57,77,66,83,72,69,80,50,88,87,83,107,0,40,78,110,99,52,87,102,71,111,109,68,48,88,66,65,118,105,80,48,102,98,114,45,121,79,105,75,82,114,104,90,72,53,75,68,88,95,78,119,61,61,107,0,64,53,57,51,54,51,101,51,52,54,55,99,101,97,57,53,48,100,53,56,56,57,100,49,100,98,57,100,50,57,49,102,52,101,101,50,50,99,54,52,48,56,51,54,56,55,102,102,53,57,101,99,98,50,52,56,56,48,101,97,54,98,55,54,55,106,100,0,7,101,110,97,98,108,101,100>>}],
     undefined,
     undefined}.
