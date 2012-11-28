%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_wm_dtrace).

-export([dt_entry/2,
         dt_entry/4,
         dt_service_entry/2,
         dt_service_entry/4,
         dt_bucket_entry/4,
         dt_object_entry/4,
         dt_return/4,
         dt_return_bool/3,
         dt_service_return/4,
         dt_bucket_return/4,
         dt_object_return/4]).

-include("riak_cs.hrl").

dt_entry(Mod, Func) ->
    dt_entry(Mod, Func, [], []).

dt_entry({Mod, SubMod}, Func, Ints, Strings) when is_atom(Mod), is_atom(SubMod)->
    dt_entry(common_submod_to_bin(Mod, SubMod), Func, Ints, Strings);
dt_entry(Mod, Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, Ints, Mod, Func, Strings).

dt_service_entry(Mod, Func) ->
    dt_service_entry(Mod, Func, [], []).
dt_service_entry(Mod, Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_SERVICE_OP, 1, Ints, Mod, Func, Strings).

dt_bucket_entry(Mod, Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_BUCKET_OP, 1, Ints, Mod, Func, Strings).

dt_object_entry(Mod, Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_OBJECT_OP, 1, Ints, Mod, Func, Strings).

dt_return_bool(Mod, Func, true) ->
    dt_return(Mod, Func, [1], []);
dt_return_bool(Mod, Func, false) ->
    dt_return(Mod, Func, [0], []).

dt_return({Mod, SubMod}, Func, Ints, Strings) when is_atom(Mod), is_atom(SubMod)->
    dt_return(common_submod_to_bin(Mod, SubMod), Func, Ints, Strings);
dt_return(Mod, Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 2, Ints, Mod, Func, Strings).

dt_service_return(Mod, Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_SERVICE_OP, 2, Ints, Mod, Func, Strings).

dt_bucket_return(Mod, Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_BUCKET_OP, 2, Ints, Mod, Func, Strings).

dt_object_return(Mod, Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_OBJECT_OP, 2, Ints, Mod, Func, Strings).


%% ===================================================================
%% Internal Functions
%% ===================================================================

common_submod_to_bin(Mod, SubMod) ->
    <<(atom_to_binary(Mod, latin1))/binary, 
      "/", 
      (atom_to_binary(SubMod, latin1))/binary>>.
