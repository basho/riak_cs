%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(riak_cs_dtrace).

-export([dtrace/1, dtrace/3, dtrace/4, dtrace/6]).
-include("riak_cs.hrl").
-export([dt_wm_entry/2,
         dt_wm_entry/4,
         dt_service_entry/2,
         dt_service_entry/4,
         dt_bucket_entry/4,
         dt_object_entry/4,
         dt_wm_return/2,
         dt_wm_return/4,
         dt_wm_return_bool/3,
         dt_service_return/4,
         dt_bucket_return/4,
         dt_object_return/4]).
-export([t/1, t/2, tt/3]).                      % debugging use only


-define(MAGIC, '**DTRACE*SUPPORT**').

dtrace(ArgList) ->
    case get(?MAGIC) of
        undefined ->
            case application:get_env(riak_cs, dtrace_support) of
                {ok, true} ->
                    case string:to_float(erlang:system_info(version)) of
                        {5.8, _} ->
                            %% R14B04
                            put(?MAGIC, dtrace),
                            dtrace(ArgList);
                        {Num, _} when Num > 5.8 ->
                            %% R15B or higher, though dyntrace option
                            %% was first available in R15B01.
                            put(?MAGIC, dyntrace),
                            dtrace(ArgList);
                        _ ->
                            put(?MAGIC, unsupported),
                            false
                    end;
                _ ->
                    put(?MAGIC, unsupported),
                    false
            end;
        dyntrace ->
            erlang:apply(dyntrace, p, ArgList);
        dtrace ->
            erlang:apply(dtrace, p, ArgList);
        _ ->
            false
    end.

dtrace(Int0, Ints, Strings) when is_integer(Int0) ->
    case get(?MAGIC) of
        unsupported ->
            false;
        _ ->
            dtrace([Int0] ++ Ints ++ Strings)
    end.

dtrace(Int0, Ints, String0, Strings) when is_integer(Int0) ->
    case get(?MAGIC) of
        unsupported ->
            false;
        _ ->
            dtrace([Int0] ++ Ints ++ [String0] ++ Strings)
    end.

%% NOTE: Due to use of ?MODULE, we may have cases where the type
%%       of String0 is an atom and not a string/iodata.

dtrace(Int0, Int1, Ints, String0, String1, Strings)
  when is_integer(Int0), is_integer(Int1) ->
    case get(?MAGIC) of
        unsupported ->
            false;
        _ ->
            S0 = if is_atom(String0) -> erlang:atom_to_binary(String0, latin1);
                    true             -> String0
                 end,
            dtrace([Int0, Int1] ++ Ints ++ [S0, String1] ++ Strings)
    end.

t(L) ->                                  % debugging/micro-performance
    dtrace(L).

t(Ints, Strings) ->                      % debugging/micro-performance
    dtrace([77] ++ Ints ++ ["entry"] ++ Strings).

tt(Int0, Ints, Strings) ->                     % debugging/micro-performance
    case get(?MAGIC) of
        X when X == dyntrace; X == dtrace ->
            dtrace([Int0] ++ Ints ++ Strings);
        _ ->
            false
    end.

dt_wm_entry(Mod, Func) ->
    dt_wm_entry(Mod, Func, [], []).

dt_wm_entry({Mod, SubMod}, Func, Ints, Strings) when is_atom(Mod), is_atom(SubMod)->
    dt_wm_entry(common_submod_to_bin(Mod, SubMod), Func, Ints, Strings);
dt_wm_entry(Mod, Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, Ints, Mod, Func, Strings).

dt_service_entry(Mod, Func) ->
    dt_service_entry(Mod, Func, [], []).
dt_service_entry(Mod, Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_SERVICE_OP, 1, Ints, Mod, Func, Strings).

dt_bucket_entry(Mod, Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_BUCKET_OP, 1, Ints, Mod, Func, Strings).

dt_object_entry(Mod, Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_OBJECT_OP, 1, Ints, Mod, Func, Strings).

dt_wm_return_bool(Mod, Func, true) ->
    dt_wm_return(Mod, Func, [1], []);
dt_wm_return_bool(Mod, Func, false) ->
    dt_wm_return(Mod, Func, [0], []).

dt_wm_return(Mod, Func) ->
    dt_wm_return(Mod, Func, [], []).

dt_wm_return({Mod, SubMod}, Func, Ints, Strings) when is_atom(Mod), is_atom(SubMod)->
    dt_wm_return(common_submod_to_bin(Mod, SubMod), Func, Ints, Strings);
dt_wm_return(Mod, Func, Ints, Strings) ->
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
