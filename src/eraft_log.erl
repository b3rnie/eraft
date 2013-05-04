%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%% @copyright Bjorn Jensen-Urstad 2013
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(eraft_log).
-behaviour(gen_server).

%%%_* Exports ==========================================================
-export([ start_link/0
        , append/1
        , read/1
	, truncate_head/1
	, truncate_tail/1
        ]).

%% gen_server
-export([ init/1
        , terminate/2
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , code_change/3
        ]).

%%%_* Includes =========================================================
-include_lib("stdlib2/include/prelude.hrl").
-include_lib("kernel/include/file.hrl").

%%%_* Macros ===========================================================
-define(entry_log,        16#AA:8).
-define(entry_trunc_head, 16#BB:8).
-define(entry_trunc_tail, 16#CC:8).

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(f, { name      :: list()
	   , start     :: integer()
	   , fd        :: _
	   , size      :: integer()
	   }).

-record(s, { path      :: list()
	   , size      :: integer()
	   , files     :: list()
	   , min_n     :: integer()
	   , max_n     :: integer()
	   }).

%%%_ * API -------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc append(Term) -> {ok, integer()} | {error, _}.
append(Term) ->
  gen_server:call(?MODULE, {append, Term}).

%% @doc read(Entry) -> {ok, term()} | {error, _}.
read(N) ->
  gen_server:call(?MODULE, {read, N}).

%% @doc truncate_head(N) -> ok | {error, _}.
%% remove entry N and everything after it
truncate_head(N) ->
  gen_server:call(?MODULE, {truncate, head, N}).

%% @doc truncate_tail(N) -> ok | {error, _}.
%% remove entry N and everything before it
truncate_tail(N) ->
  gen_server:call(?MODULE, {truncate, tail, N}).

%%%_ * gen_server callbacks --------------------------------------------
init([]) ->
  {ok, Path} = application:get_env(eraft, log_path),
  {ok, Size} = application:get_env(eraft, log_size),
  eraft_util:ensure_path(Path),
  case file:list_dir(Path) of
    {ok, []}    -> init_new(Path, Size);
    {ok, Files} -> init_old(Path, Size, sort(Files))
  end.

terminate(_Rsn, S) ->
  lists:foreach(fun(F) -> ok = file:close(F#.fd) end, S#s.files).

handle_call({append, Term}, _From, S) ->
  [F0|Fs]     = maybe_switch_log(S#s.files, S#s.size),
  {F, Offset} = do_append(S#s.max_n+1, Term, F0),
  N2O         = update_index(Offset, S#s.max_n+1, S#s.n2o),
  {reply, {ok, S#s.next}, S#s{files=[F|Fs],
			      n2o=N2O
			      max_n=S#s.max_n+1
			     }};

handle_call({read, N}, _From, S)
  when N > S#s.max_n;
       N < S#s.min_n ->
  {reply, {error, bounds}, S};

handle_call({read, N}, _From, S) ->
  {reply, {ok, do_read(dict:fetch(N, S#s.n2o), S#s.files)}, S};

handle_call({truncate, _, N}, _From, S)
  when N < S#s.min_n;
       N > S#s.max_n ->
  {reply, {error, bounds}, S};

handle_call({truncate, Type, N}, _From, S) ->
  Fs0 = maybe_switch_log(S#s.files, S#s.size),
  Fs1 = do_truncate(Fs0, Type, N),
  Fs  = do_gc(Fs1, Type, N),
  N20 = update_index(),
  {reply, ok, S}.

handle_cast(Msg, S) ->
  {stop, {bad_cast, Msg}, S}.

handle_info(Msg, S) ->
  ?warning("~p", [Msg]),
  {noreply, S}.

%%%_ * Internals init --------------------------------------------------
init_new(Path, Size) ->
  File = log_filename(Path, 0),
  {ok, FD} = file:open(File, [read, write, binary]),
  {ok, #s{files=[#f{name=File, start=0, fd=FD, size=0}], n2o=dict:new()}}.

init_old(Path, Size, Files) ->
  Fs = lists:map(
	 fun(File) ->
	     {ok, FD} = file:open(File, [read, write, binary]),
	     {ok, #file_info{size=Size}} = file:read_file_info(File),
	     #f{name=File, start=start_offset(File), fd=FD, size=Size}
	 end, Files),
  {N2O, Min, Max} = traverse(Fs, dict:new(), undefined, undefined),
  {ok, #s{files=Fs, n2o=N2O}}.

traverse_log([F|Fs], D, Min, Max) ->
  case read_next(F) of
    {ok, {entry, N, Offset}} ->
      read_next([F|Fs], dict:store(N, Offset, D), Min, Max+1);
    {ok, {trunc_tail, N}} ->
      read_next([F|Fs], D, N, Max);
    {ok, {trunc_head, N}} ->
      read_next([F|Fs], D, Min, N);
    {error, eof}   when Fs =:= [] -> {D, Min, Max};
    {error, short} when Fs =:= [] -> {D, Min, Max}
  end.

read_next(#f{fd=FD}) ->
  case file:read(FD, 20+8) of
    {ok, <<Hash:20/binary, Size:64/integer>>} ->
      case file:read(FD, Size) of
	{ok, Bin} when size(Bin) =:= Size ->
	  Hash = crypto:sha(<<Size:64/integer, Bin/binary>>),
	  {ok, read_next_parse(Hash, Bin)};
	{ok, _} ->
	  {error, short};
	eof ->
	  {error, short}
      end;
    {ok, _} ->
      {error, short};
    {error, eof} ->
      {error, eof}
  end.

read_next_parse(<<?entry_log, N:64/integer, Term:/binary>>) ->
  {entry, N, Offset};
read_next_parse(<<?entry_trunc_head, N:64/integer>>) ->
  {trunc_head, N};
read_next_parse(<<?entry_trunc_tail, N:64/integer>>) ->
  {trunc_tail, N}.

%%%_ * Internals append ------------------------------------------------
do_append(N, Term, #f{size=Size}) ->
  TermBin   = erlang:term_to_binary(Term),
  EntryBin  = <<?entry_log, N:64/integer, TermBin/binary>>,
  write_log(EntryBin, F).

%%%_ * Internals truncate ----------------------------------------------
do_truncate(N) ->
  EntryBin  = <<?entry_trunc_head, N:64/integer>>,
  EntrySize = erlang:size(EntryBin),
  Hash      = crypto:sha(EntryBin),
  write_log(<<?entry_trunc_head, N:64/integer>>).


%%%_ * Internals misc --------------------------------------------------
maybe_switch_log([#f{size=Size}|_] = Files, MaxSize)
  when Size < MaxSize ->
  Files;
maybe_switch_log([F|_] = Files, _MaxSize) ->
  Start = F#f.start + F#f.size,
  File = log_filename(Path, Start),
  {ok, FD} = file:open(File, [read, write, binary]),
  [#f{name=File, start=Start, fd=FD, size=0} | Files].

write_log(Bin, F) ->
  Size = erlang:size(Bin),
  Hash = crypto:sha(Bin),
  ok = file:pwrite(F#f.fd, F#f.size, <<Hash/binary,
				       Size:64/integer,
				       Bin/binary>>),
  F#f{size = F#f.size + 20 + 8 + Size}.

log_filename(Path, Offset) ->
  File = lists:flatten(io_lib:format("~20..0B", [Offset])),
  filename:join(Path, File).

sort(Files) ->
  lists:sort(fun(A, B) -> start_offset(A) > start_offset(B) end, Files).

start_offset(File) ->
  erlang:list_to_integer(filename:basename(File)).

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

in_clean_env(F) ->
  {ok, Path} = application:get_env(eraft, log_path),
  case file:list_dir(Path) of
    {ok, Files} ->
      [ok = file:delete(filename:join(Path, File)) || File <- Files];
    {error, enoent} ->
      ok
  end,
  F().

start_stop_test() ->
  in_clean_env(
    fun() ->
	{ok, _}   = eraft_log:start_link(),
	{ok, 1}   = eraft_log:append(foo),
	{ok, 2}   = eraft_log:append(bar),
	{ok, foo} = eraft_log:read(1),
	{ok, bar} = eraft_log:read(2),
	ok        = eraft_log:stop(),
	{ok, _}   = eraft_log:start_link(),
	{ok, foo} = eraft_log:read(1),
	{ok, 3}   = eraft_log:append(baz)
    end).

-endif

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
