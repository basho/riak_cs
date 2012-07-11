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
-export([setup/0, teardown/0]).
-export([start/3, start_link/3, stop/1,
         get/5, put/4]).

%% Cut-and-paste from riakc_obj.erl, naughty but fun
-record(riakc_obj, {
          bucket,
          key,
          vclock,
          contents,
          updatemetadata,
          updatevalue
         }).

setup() ->
    meck:new(?PBS, [passthrough]),
    Es = [{get, fun get/3}, {get, fun get/4}, {get, fun get/5},
          {put, fun put/2}, {put, fun put/3}, {put, fun put/4},
          {delete, fun delete/3}, {delete, fun delete/4},
          {delete, fun delete/5}],
    [meck:expect(?PBS, Name, Fun) || {Name, Fun} <- Es],
    ets:new(?TAB, [named_table, public, ordered_set]),

    %% Helpers to work around Stanchion
    meck:new(riak_moss_utils, [passthrough]),
    meck:expect(riak_moss_utils, create_bucket, fun create_bucket/5),
    meck:expect(riak_moss_utils, delete_bucket, fun delete_bucket/4),
    meck:expect(riak_moss_utils, riak_connection, fun() -> {ok, spawn(fun() -> timer:sleep(5000) end)} end),
    meck:expect(riak_moss_utils, close_riak_connection, fun(_) -> ok end),

    %% Temp helper to avoid spinning up a GET fsm on a put operation
    %% (to check ACL of existing object??)
    meck:new(riak_moss_wm_key, [passthrough]),
    meck:expect(riak_moss_wm_key, get_access_and_manifest, fun(X, Y) -> {false, X, Y} end),
    %% meck:new(riak_moss_get_fsm, [passthrough]),
    %% %% meck:expect(riak_moss_get_fsm, get_manifest, fun(_) -> notfound end),
    %% meck:new(riak_moss_acl, [passthrough]),
    %% %% meck:expect(riak_moss_acl, object_access, fun(_,_,_,_,_) -> true end),

    ok.

teardown() ->
    catch meck:unload(?PBS),
    catch meck:unload(riak_moss_utils),
    catch meck:unload(riak_moss_wm_key),
    catch meck:unload(riak_moss_get_fsm),
    catch meck:unload(riak_moss_acl),
    catch ets:delete(?TAB),
    ok.

start(_Address, _Port, _Options) ->
    {ok, ?MODULE}.

start_link(Address, Port, Options) ->
    start(Address, Port, Options).

stop(_Pid) ->
    ok.

get(_Pid, Bucket, Key) ->
    get(_Pid, Bucket, Key, x).

get(_Pid, Bucket, Key, X) ->
    get(_Pid, Bucket, Key, X, x).

get(_Pid, Bucket, Key, _Options, _Timeout) ->
    io:format("get, "),
    case ets:lookup(?TAB, {Bucket, Key}) of
        [] ->
            {error, notfound};
        [{_, Obj}] ->
            {ok, Obj}
    end.

put(_Pid, Obj) ->
    put(_Pid, Obj, x).

put(_Pid, Obj, X) ->
    put(_Pid, Obj, X, x).

put(_Pid, Obj0, _Options, _Timeout) ->
    io:format("put, "),
    Bucket = riakc_obj:bucket(Obj0),
    Key = riakc_obj:key(Obj0),
    V = riakc_obj:get_update_value(Obj0),
    MDL = dict:to_list(riakc_obj:get_update_metadata(Obj0)),
    MD = dict:from_list(lists:map(
                          fun({<<"X-Riak-Meta">>, XRMList}) ->
                                  {<<"X-Riak-Meta">>,
                                   lists:map(
                                     fun({MK,MV}) when is_binary(MK) ->
                                             {binary_to_list(MK),
                                              binary_to_list(MV)};
                                        (Else) ->
                                             Else
                                     end, XRMList)};
                             (Else2) ->
                                  Else2
                          end, MDL)),
    Obj = Obj0#riakc_obj{contents = [{MD, V}],
                         updatemetadata = undefined, updatevalue = undefined},
    ets:insert(?TAB, {{Bucket, Key}, Obj}),
    ok.

delete(_Pid, Bucket, Key) ->
    delete(_Pid, Bucket, Key, x).

delete(_Pid, Bucket, Key, X) ->
    delete(_Pid, Bucket, Key, X, x).

delete(_Pid, Bucket, Key, _Options, _Timeout) ->
    io:format("delete, "),
    case ets:member(?TAB, {Bucket, Key}) of
        false ->
            {error, notfound};
        true ->
            ets:delete(?TAB, {Bucket, Key}),
            ok
    end.

create_bucket(User, _VClock, Bucket, ACL, _RiakPid) ->
    KeyId = User?RCS_USER.key_id,
    stanchion_utils:do_bucket_op(Bucket, list_to_binary(KeyId), ACL, create).

delete_bucket(User, _VClock, Bucket, _RiakPid) ->
    KeyId = User?RCS_USER.key_id,
    stanchion_utils:do_bucket_op(Bucket, list_to_binary(KeyId), ?ACL{}, delete).

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
        []}}}, <<131,104, 9,100,0, 11,114, 99,115, 95,117, 115,101,
        114,95, 118,50, 107,0,7, 102,111, 111,32, 98,97, 114,107,
        0,6,102, 111,111, 98,97, 114,107, 0,17, 102,111, 111,98,
        97,114, 64,102, 111,111, 98,97, 114,46, 99,111, 109,107,
        0,20,78, 80,51, 90,69, 72,75, 95,72, 57,77, 66,83, 72,69,
        80,50, 88,87, 83,107, 0,40,78, 110,99, 52,87, 102,71, 111,109,
        68,48, 88,66, 65,118, 105,80, 48,102, 98,114, 45,121, 79,105,
        75,82, 114,104, 90,72, 53,75, 68,88, 95,78, 119,61, 61,107,
        0,64,53, 57,51, 54,51, 101,51, 52,54, 55,99, 101,97, 57,53,
        48,100, 53,56, 56,57, 100,49, 100,98, 57,100, 50,57, 49,102,
        52,101, 101,50, 50,99, 54,52, 48,56, 51,54, 56,55, 102,102,
        53,57, 101,99, 98,50, 52,56, 56,48, 101,97, 54,98, 55,54,
        55,108, 0,0,0,2, 104,6, 100,0, 14,109, 111,115, 115,95,
        98,117, 99,107, 101,116, 95,118, 49,107, 0,5,116, 101,115,
        116,50, 100,0,7, 99,114, 101,97, 116,101, 100,107, 0,24,50,
        48,49, 50,45, 48,55, 45,49, 48,84, 50,51, 58,49, 51,58, 50,51,
        46,48, 48,48, 90,104, 3,98,0, 0,5,61, 98,0,14, 173,211,
        98,0,7, 61,85, 100,0,9, 117,110, 100,101, 102,105, 110,101,
        100,104, 6,100,0, 14,109, 111,115, 115,95, 98,117, 99,107,
        101,116, 95,118, 49,107, 0,4,116, 101,115, 116,100, 0,7,99,
        114,101, 97,116, 101,100, 107,0, 24,50, 48,49, 50,45, 48,55,
        45,48, 50,84, 50,51, 58,52, 56,58, 51,52, 46,48, 48,48,
        90,104, 3,98,0, 0,5,61, 98,0,4, 42,18, 98,0,14, 7,249,
        100,0,9, 117,110, 100,101, 102,105, 110,101, 100,106, 100,0,7,
        101,110, 97,98, 108,101, 100>>}],
     undefined,
     undefined}.
