%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
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
%%-------------------------------------------------------------------

-module(intercept).
%% Export explicit API but also send compile directive to export all
%% because some of these private functions are useful in their own
%% right.
-export([add/3, add/4, clean/1]).
-compile(export_all).

-type abstract_code() :: term().
-type form() :: term().
-type proplist(K, V) :: proplists:proplist(K, V).
-type fun_name() :: atom().
-type fun_type() :: fun_name() | tuple().
-type target_fun() :: {fun_name(), arity()}.
-type intercept_fun() :: fun_type().
-type mapping() :: proplist(target_fun(), intercept_fun()).
-type form_mod() :: fun((form()) -> form()).
-type code_mod() :: fun((form(), abstract_code()) -> abstract_code()).

%% The "original" is the `Target' module with the suffix `_orig'.  It
%% is where original code for the `Target' module resides after
%% intercepts are added.
-define(ORIGINAL(Mod), list_to_atom(atom_to_list(Mod) ++ "_orig")).
-define(FAKE_LINE_NO,1).

%% @doc Add intercepts against the `Target' module.
%%
%% `Target' - The module on which to intercept calls.
%%            E.g. `hashtree'.
%%
%% `Intercept' - The module containing intercept definitions.
%%               E.g. `hashtree_intercepts'
%%
%% `Mapping' - The mapping from target functions to intercept
%%             functions.
%%
%%             E.g. `[{{update_perform,2}, sleep_update_perform}]'
-spec add(module(), module(), mapping()) -> ok.
add(Target, Intercept, Mapping, OutDir) ->
    Original = ?ORIGINAL(Target),
    TargetAC = get_abstract_code(Target),

    ProxyAC = make_proxy_abstract_code(Target, Intercept, Mapping,
                                       Original, TargetAC),
    OrigAC = make_orig_abstract_code(Target, Original, TargetAC),

    ok = compile_and_load(Original, OrigAC, OutDir),
    ok = compile_and_load(Target, ProxyAC, OutDir).

add(Target, Intercept, Mapping) ->
    add(Target, Intercept, Mapping, undefined).

%% @doc Cleanup proxy and backuped original module
-spec clean(module()) -> ok|{error, term()}.
clean(Target) ->
    _ = code:purge(Target),
    _ = code:purge(?ORIGINAL(Target)),
    case code:load_file(Target) of
        {module, Target} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%% @private
%%
%% @doc Compile the abstract code `AC' and load it into the code server.
-spec compile_and_load(module(), abstract_code(), undefined | string()) -> ok.
compile_and_load(Module, AC, OutDir) ->
    {ok, Module, Bin} = compile:forms(AC,[debug_info]),
    ModStr = atom_to_list(Module),
    _ = is_list(OutDir) andalso
        file:write_file(filename:join(OutDir, ModStr ++ ".beam"), Bin),
    {module, Module} = code:load_binary(Module, ModStr, Bin),
    ok.

%% @private
-spec make_orig_abstract_code(module(), module(), abstract_code()) ->
                                      abstract_code().
make_orig_abstract_code(Target, OrigName, TargetAC) ->
    export_all(move_all_funs(Target, change_module_name(OrigName, TargetAC))).

%% @private
%%
%% @doc Make the abstract code for the proxy module.  The proxy module
%%      sits in place of the original module and decides whether to
%%      forward to the `Intercept' or the `Original' depending on the
%%      `Mapping'.
-spec make_proxy_abstract_code(module(), module(), mapping(),
                               module(), abstract_code()) ->
                                      abstract_code().
make_proxy_abstract_code(Target, Intercept, Mapping, Original, TargetAC) ->
    AC1 = forward_all(Original, TargetAC),
    AC2 = export_all(change_module_name(Target, AC1)),
    apply_intercepts(AC2, Intercept, Mapping).


%% @private
%%
%% @doc Apply intercepts to the abstract code `AC' based on `Mapping'.
-spec apply_intercepts(abstract_code(), module(), mapping()) -> abstract_code().
apply_intercepts(AC, Intercept, Mapping) ->
    apply_to_funs(mapping_fun(Intercept, Mapping), AC).

%% @private
%%
%% @doc Return a form modifier function that uses `Mapping' to
%%      determine if a function should be modified to forward to the
%%      `Intercept' module.
-spec mapping_fun(module(), proplists:proplist()) -> form_mod().
mapping_fun(Intercept, Mapping) ->
    fun(Form) ->
            Key = {fun_name(Form), fun_arity(Form)},
            case proplists:get_value(Key, Mapping, '$none') of
                '$none' ->
                    Form;
                InterceptFun ->
                    forward(Intercept, InterceptFun, Form)
            end
    end.

%% @private
%%
%% @doc Modify the abstract code `AC' to forward all function calls to
%%      `Module' and move the original definitions under
%%      `<function_name>_orig'.
-spec move_all_funs(module(), abstract_code()) -> abstract_code().
move_all_funs(Module, AC) ->
    lists:reverse(lists:foldl(move_all_funs(Module), [], AC)).

%% @private
%%
%% @doc Return a function which folds over the abstract code of a
%%      module, represented by `Form'.  Every function is modified to
%%      forward to `ModuleName' and it's original definition is stored
%%      under `<function_name>_orig'.
-spec move_all_funs(module()) -> code_mod().
move_all_funs(ModuleName) ->
    fun(Form, NewAC) ->
            case is_fun(Form) of
                false ->
                    [Form|NewAC];
                true ->
                    %% Move current function code under different name
                    Name = fun_name(Form),
                    OrigForm = setelement(3, Form, ?ORIGINAL(Name)),

                    %% Modify original function to forward to `ModuleName'
                    FwdForm = forward(ModuleName, Name, Form),
                    [FwdForm,OrigForm|NewAC]
            end
    end.

%% @private
%%
%% @doc Modify all function definitions in the abstract code `AC' to
%%      forward to `Module:FunName_orig'.
-spec forward_all(module(), abstract_code()) -> abstract_code().
forward_all(Module, AC) ->
    F = fun(Form) ->
                forward(Module, ?ORIGINAL(fun_name(Form)), Form)
        end,
    apply_to_funs(F, AC).

%% @private
%%
%% @doc Modify the function `Form' to forward to `Module:Fun'.
-spec forward(module(), fun_type(), form()) -> form().
forward(Module, Fun, Form) ->
    Clause = hd(fun_clauses(Form)),
    Args = clause_args(Clause),
    NumArgs = length(Args),
    GenArgs = [{var,?FAKE_LINE_NO,list_to_atom("Arg" ++ integer_to_list(I))}
               || I <- lists:seq(1,NumArgs)],
    Clause2 = clause_set_args(Clause, GenArgs),
    Clause3 = clause_clear_guards(Clause2),
    Body = [{call, 1,
             case Fun of
                 Fun when is_atom(Fun) ->
                     {remote,1,{atom,1,Module},{atom,1,Fun}};
                 %% If Fun is a tuple, it's a pair comprising a list of
                 %% local variables to capture and an anonymous function
                 %% that's already in the abstract format. The anonymous
                 %% function uses the local variables.
                 {FreeVars, AnonFun} ->
                     generate_fun_wrapper(FreeVars, AnonFun, NumArgs)
             end, GenArgs}],
    Clause4 = clause_set_body(Clause3, Body),
    fun_set_clauses(Form, [Clause4]).

change_module_name(NewName, AC) ->
    lists:keyreplace(module, 3, AC, {attribute,1,module,NewName}).

%% @private
%%
%% @doc Generate an anonymous function wrapper that sets up calls for an
%%      anonymous function interceptor.
%%
%% This function returns the abstract code equivalent of the following
%% code. If you change this code, please update this comment.
%%
%% fun(__A0_, __A1_, ...) ->
%%     __Bindings0_ = lists:foldl(fun({__Bn_,__Bv_},__Acc_) ->
%%                                    erl_eval:add_binding(__Bn_,__Bv_,__Acc_)
%%                                end,
%%                                erl_eval:new_bindings(),
%%                                <free vars from generate_freevars>),
%%     __Bindings = lists:foldl(fun({{var,_,__Vn_},__V_},__Acc) ->
%%                                  erl_eval:add_binding(__Vn_,__V_,__Acc_)
%%                              end,
%%                              __Bindings0_,
%%                              <__A0_ etc. args from generate_freevars>),
%%     erl_eval:expr(<abstract code for AnonFun(__A0_, __A1_, ...)>,
%%                   __Bindings_, none, none, value).
%%
generate_fun_wrapper(FreeVars, AnonFun, NumArgs) ->
    L = ?FAKE_LINE_NO,
    Args = [{var,L,list_to_atom(lists:flatten(["__A",Var+$0],"_"))} ||
               Var <- lists:seq(1, NumArgs)],
    {'fun',L,
     {clauses,
      [{clause,L,Args,[],
        [{match,L+1,
          {var,L+1,'__Bindings0_'},
          {call,L+1,
           {remote,L+1,{atom,L+1,lists},{atom,L+1,foldl}},
           [{'fun',L+1,
             {clauses,
              [{clause,L+1,
                [{tuple,L+1,[{var,L+1,'__Bn_'},{var,L+1,'__Bv_'}]},
                 {var,L+1,'__Acc_'}],
                [],
                [{call,L+2,
                  {remote,L+2,
                   {atom,L+2,erl_eval},
                   {atom,L+2,add_binding}},
                  [{var,L+2,'__Bn_'},{var,L+2,'__Bv_'},{var,L+2,'__Acc_'}]}]
               }]}},
            {call,L+3,
             {remote,L+3,{atom,L+3,erl_eval},{atom,L+3,new_bindings}},[]},
            generate_freevars(FreeVars,L+3)]}},
         {match,L+4,
          {var,L+4,'__Bindings_'},
          {call,L+4,
           {remote,L+4,{atom,L+4,lists},{atom,L+4,foldl}},
           [{'fun',L+4,
             {clauses,
              [{clause,L+4,
                [{tuple,L+4,[{tuple,L+4,[{atom,L+4,var},{var,L+4,'_'},
                                         {var,L+4,'__Vn_'}]},{var,L+4,'__V_'}]},
                 {var,L+4,'__Acc_'}],
                [],
                [{call,L+5,
                  {remote,L+5,
                   {atom,L+5,erl_eval},
                   {atom,L+5,add_binding}},
                  [{var,L+5,'__Vn_'},{var,L+5,'__V_'},{var,L+5,'__Acc_'}]}]
               }]}},
            {var,L+6,'__Bindings0_'},
            lists:foldl(fun(V,Acc) ->
                                AV = erl_parse:abstract(V),
                                {cons,L+6,{tuple,L+6,[AV,V]},Acc}
                        end,{nil,L+6},Args)]}},
         {call,L+7,
          {remote,L+7,
           {atom,L+7,erl_eval},
           {atom,L+7,expr}},
          [erl_parse:abstract({call,L+7,AnonFun,
                               [{var,L+7,V} || {var,_,V} <- Args]},L+7),
           {var,L+7,'__Bindings_'},
           {atom,L+7,none},
           {atom,L+7,none},
           {atom,L+7,value}]}]}]}}.

%% @private
%%
%% @doc Convert generate_fun_wrapper freevars to abstract code
generate_freevars([], L) ->
    {nil,L};
generate_freevars([FreeVar|FreeVars], L) ->
    {cons,L,
     generate_freevar(FreeVar,L),
     generate_freevars(FreeVars,L)}.

%% @private
%%
%% @doc Convert one freevar to abstract code
%%
%% This returns an abstract format tuple representing a freevar as
%% {VarName, VarValue}. For function values we check their env for their
%% own freevars, but if no env is available, we raise an error. Pids,
%% ports, and references have no abstract format, so they are first
%% converted to binaries and the abstract format of the binary is used
%% instead. Their abstract format values generated here convert them back
%% from binaries to terms when accessed.
generate_freevar({Name,Var},L) when is_function(Var) ->
    {env, Env} = erlang:fun_info(Var, env),
    case Env of
        [] ->
            error({badarg, Var});
        [FreeVars,_,_,Clauses] ->
            {arity, Arity} = erlang:fun_info(Var, arity),
            AnonFun = {'fun',L,{clauses,Clauses}},
            {tuple,L,
             [{atom,L,Name},
              generate_fun_wrapper(FreeVars, AnonFun, Arity)]}
    end;
generate_freevar({Name,Var}, L)
  when is_pid(Var); is_port(Var); is_reference(Var) ->
    NVar = term_to_binary(Var),
    {tuple,L,
     [{atom,L,Name},
      {call,L,
       {remote,L,{atom,L,erlang},{atom,L,binary_to_term}},
       [erl_parse:abstract(NVar)]}]};
generate_freevar(NameVar, L) ->
    erl_parse:abstract(NameVar,L).

%% @private
%%
%% @doc Add the `export_all' compile directive to the abstract code `AC'.
export_all(AC) ->
    [A,B|Rest] = AC,
    [A,B,{attribute,2,compile,export_all}|Rest].

%% @private
%%
%% @doc Apply the form modify `F' to all forms in `AC' that are
%%      function definitions.
-spec apply_to_funs(form_mod(), abstract_code()) -> abstract_code().
apply_to_funs(F, AC) ->
    F2 = apply_if_fun_def(F),
    lists:map(F2, AC).

%% @private
%%
%% @doc Get the abstract code for `Module'.  This function assumes
%%      code is compiled with `debug_info'.
-spec get_abstract_code(module()) -> abstract_code().
get_abstract_code(Module) ->
    {_, Bin, _} = code:get_object_code(Module),
    {ok,{_,[{abstract_code,{_,AC}}]}} = beam_lib:chunks(Bin, [abstract_code]),
    AC.

%% @private
apply_if_fun_def(Fun) ->
    fun(Form) when element(1, Form) == function -> Fun(Form);
       (Form) -> Form
    end.

%% @private
is_fun(Form) ->
    element(1, Form) == function.

%% @private
clause_args(Form) ->
    element(3, Form).

%% @private
clause_set_args(Form, Args) ->
    setelement(3, Form, Args).

%% @private
clause_clear_guards(Form) ->
    setelement(4, Form, []).

%% @private
clause_set_body(Form, Body) ->
    setelement(5, Form, Body).

%% @private
fun_arity(Form) ->
    element(4, Form).

%% @private
fun_clauses(Form) ->
    element(5, Form).

%% @private
fun_set_clauses(Form, Clauses) ->
    setelement(5, Form, Clauses).

%% @private
fun_name(Form) ->
    element(3, Form).
