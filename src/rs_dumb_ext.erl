%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% So, this is an ugly hack.  But I won't want to write a NIF yet, and
%% drivers are usually even more fiddly work than NIFs.  So, here we
%% are.
%%
%% Prerequisites: Set things up per the instructions found in the
%%                "README.txt" file at:
%%                http://www.snookles.com/scotttmp/jerasure-hack-201210/
%%
%% -------------------------------------------------------------------

-module(rs_dumb_ext).

-behaviour(gen_server).

-include("rs_erasure_encoding.hrl").

%% API
-export([start_link/2,
         encode/5,
         decode/7,
         stop/1]).
-export([t_iter_encode/6]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          exe :: string(),
          dir :: string()
         }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(JerasureEncDecWrapper, WorkDir) ->
    gen_server:start_link(?MODULE, {JerasureEncDecWrapper, WorkDir},
                          []).

encode(Pid, Alg, K, M, Bin)
  when is_atom(Alg), is_integer(K), is_integer(M), is_binary(Bin) ->
    gen_server:call(Pid, {encode, Alg, K, M, Bin}, infinity).

decode(Pid, Alg, BinSize, K, M, Ks, Ms)
  when is_atom(Alg), is_integer(BinSize),
       is_integer(K), is_integer(M),
       is_list(Ks), is_list(Ms) ->
    VerifyFun = fun({Num, Bin}) when is_integer(Num), Num > 0,
                                     is_binary(Bin) ->
                        true;
                   (_) ->
                        false
                end,
    true = lists:all(VerifyFun, Ks),
    true = lists:all(VerifyFun, Ms),
    gen_server:call(Pid, {decode, Alg, BinSize, K, M, Ks, Ms}, infinity).

stop(Pid) ->
    gen_server:call(Pid, {stop}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init({JerasureEncDecWrapper, WorkDir}) ->
    ok = clean_up_dir(WorkDir),
    true = filelib:is_file(JerasureEncDecWrapper),
    {ok, #state{exe = JerasureEncDecWrapper,
                dir = WorkDir}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({encode, Alg, K, M, Bin}, _From, State) ->
    Res = do_encode(Alg, K, M, Bin, State#state.dir, State#state.exe),
    {reply, Res, State};
handle_call({decode, Alg, BinSize, K, M, Ks, Ms}, _From, State) ->
    Res = do_decode(Alg, BinSize, K, M, Ks, Ms, State#state.dir, State#state.exe),
    {reply, Res, State};
handle_call({stop}, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    Reply = neverNeVaH,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, State) ->
    clean_up_dir(State#state.dir),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_encode(Alg, K, M, Bin, Dir, Exe) ->
    Alg = ?ALG_CAUCHY_GOOD_V0,
    try
        clean_up_dir(Dir),
        TmpFile = filename:join(Dir, "file"),
        ok = my_write_file(TmpFile, Bin),

        Cmd = make_encoder_cmd(Alg, K, M, Bin, Dir, Exe, TmpFile),
        %% TODO: Oops, I need something other than exit status, drat!
        "0\n" = os:cmd(Cmd),

        Ks = [filename:join(Dir, "file_k" ++ integer_to_list(X)) ||
                 X <- lists:seq(1, K)],
        Ms = [filename:join(Dir, "file_m" ++ integer_to_list(X)) ||
                 X <- lists:seq(1, M)],
        {read_frags(Ks), read_frags(Ms)}                 
    catch X:Y ->
            {error, {X, Y, erlang:get_stacktrace()}}
    end.

do_decode(Alg, BinSize, K, M, Ks, Ms, Dir, Exe) ->
    Alg = ?ALG_CAUCHY_GOOD_V0,
    true = (length(Ks) < K),
    true = (length(Ms) =< M),
    try
        clean_up_dir(Dir),
        [write_km_file(Dir, k, Num, Bin) || {Num, Bin} <- Ks],
        [write_km_file(Dir, m, Num, Bin) || {Num, Bin} <- Ms],
        Out = filename:join(Dir, "file"),
        Decoded = filename:join(Dir, "file_decoded"),

        Meta = filename:join(Dir, "file_meta.txt"),
        MetaData = io_lib:format(
                     "~s\n~w\n~w ~w 4 64 ~w\ncauchy_good\n3\n1\n",
                     [Out, BinSize, K, M, BinSize]),
        ok = my_write_file(Meta, MetaData),

        Cmd = make_decoder_cmd(Alg, Dir, Exe, Out),
        %% TODO: Oops, I need something other than exit status, drat!
        "0\n" = os:cmd(Cmd),

        {ok, DecodedBin} = file:read_file(Decoded),
        DecodedBin
    catch X:Y ->
            {error, {X, Y, erlang:get_stacktrace()}}
    end.
    
clean_up_dir(Dir) ->
    %% TmpDir = filename:join(Dir, "bogus-name-for-ensure_dir"),
    %% ok = filelib:ensure_dir(TmpDir),
    %% Files = filelib:wildcard(filename:join(Dir, "*")),
    %% [ok = file:delete(F) || F <- Files],
    os:cmd("rm -rf " ++ Dir ++ "/*"),
    ok.

make_encoder_cmd(?ALG_CAUCHY_GOOD_V0, K, M, _Bin, Dir, Exe, TmpFile) ->
    flat("~s encoder ~s ~s ~w ~w cauchy_good 4 64 0\n",
         [Exe, Dir, TmpFile, K, M]).

make_decoder_cmd(?ALG_CAUCHY_GOOD_V0, Dir, Exe, OutFile) ->
    flat("~s decoder ~s ~s\n", [Exe, Dir, OutFile]).

flat(Fmt, Args) ->
    lists:flatten(io_lib:format(Fmt, Args)).

read_frags(Files) ->
    [begin
         {ok, Bin} = my_read_file(F),
         Bin
     end || F <- Files].

write_km_file(Dir, Type, Num, Bin)
  when is_list(Dir) andalso
       (Type == k orelse Type == m) andalso
       is_integer(Num) andalso Num > 0 andalso
       is_binary(Bin) ->
    Path = filename:join(Dir, "file_" ++ atom_to_list(Type) ++
                             integer_to_list(Num)),
    ok = my_write_file(Path, Bin).

%% WARNING: Do not use on files larger than 16MB.

my_read_file(Path) ->
    {ok, FH} = file:open(Path, [read, binary, raw]),
    Res = file:read(FH, 16*1024*1024),
    file:close(FH),
    Res.

my_write_file(Path, Bin) ->
    {ok, FH} = file:open(Path, [write, binary, raw]),
    Res = file:write(FH, Bin),
    file:close(FH),
    Res.

%% Goofy little performance test ... which shows that there's a pretty
%% horrible bottleneck in here somewhere: 1 proc can do ~35MByte/sec
%% but 8 procs can only do about ~48 MByte/sec.
%%
%% Encoders = [begin {ok, Pid} = rs_dumb_ext:start_link("/Users/fritchie/src/erasure-encoding-libs/jerasure.osx-mountain-lion/bin/enc-dec-wrapper.sh", "/tmp/qwer" ++ integer_to_list(X)), Pid end || X <- lists:seq(1,8)].
%% [spawn(fun() -> Res = timer:tc(fun() -> catch rs_dumb_ext:t_iter_encode(L100, Eee, alg_cauchy_good0, 9, 4, B1Mr) end), io:format("Res ~P\n", [Res, 10]) end) || Eee <- Encoders].

t_iter_encode(IterItemList, Pid, Alg, K, M, Bin) ->
    [begin
         {_, _} = encode(Pid, Alg, K, M, Bin),
         ok
     end || _ <- IterItemList].
