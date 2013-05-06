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
-record(f, { name  = throw(name)  :: list()
	   , start = throw(start) :: integer()
	   , fd    = throw(fd)    :: _
	   , size  = 0            :: integer()
	   }).

-record(s, { path  = throw(path)  :: list()
	   , size  = throw(size)  :: integer()
	   , files = throw(files) :: list()
	   , min   = 0            :: integer()
	   , max   = 0            :: integer()
	   , idx   = dict:new()   :: _
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
  gen_server:call(?MODULE, {trunc_head, N}).

%% @doc truncate_tail(N) -> ok | {error, _}.
%% remove entry N and everything before it
truncate_tail(N) ->
  gen_server:call(?MODULE, {trunc_tail, N}).

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
  [F0|Fs]         = maybe_switch_log(S#s.files, S#s.size),
  {F, Offset}     = append_term(S#s.max, Term, F0),
  {Idx, Min, Max} = update_idx_append(Offset, S#s.idx, S#s.min, S#s.max),
  {reply, {ok, N}, S#s{files=[F|Fs],
		       n2o=N2O
		       max_n=N
		      }};

handle_call({read, N}, _From, S) ->
  case dict:find(N, S#s.n2o) of
    {value, Offset} -> {reply, {ok, do_read(Offset, S#s.files)}, S};
    error           -> {reply, {error, notfound}, S}
  end.

handle_call({Trunc, N}, _From, S)
  when Trunc =:= trunc_tail;
       Trunc =:= trunc_head,
       (N < S#s.min orelse N > S#s.max ->
  {reply, {error, bounds}, S};

handle_call({Trunc, N}, _From, S)
  when Trunc =:= trunc_head;
       Trunc =:= trunc_tail ->
  [F0|Fs0]        = maybe_switch_log(S#s.files, S#s.size),
  F               = append_trunc(Type, N, F0),
  {Idx, Min, Max} = update_idx_trunc(Type, S#s.idx, S#s.min, S#s.max),
  Fs              = gc([F|Fs0], Idx, Min, Max),
  {reply, ok, S#s{files=Fs, idx=Idx, min=Min, max=Max}}.

handle_cast(Msg, S) ->
  {stop, {bad_cast, Msg}, S}.

handle_info(Msg, S) ->
  ?warning("~p", [Msg]),
  {noreply, S}.

%%%_ * Internals init --------------------------------------------------
init_new(Path, Size) ->
  File     = log_filename(Path, 0),
  {ok, FD} = file:open(File, [read, write, binary]),
  F        = #f{name=File, start=0, fd=FD},
  {ok, #s{path=Path, size=Size, files=[F]}}.

init_old(Path, Size, Files) ->
  Fs = lists:map(
	 fun(File) ->
	     {ok, FD} = file:open(File, [read, write, binary]),
	     {ok, #file_info{size=Size}} = file:read_file_info(File),
	     #f{name=File, start=start_offset(File), fd=FD, size=Size}
	 end, Files),
  {N2O, Min, Max} = traverse(Fs, dict:new(), 0, 0),
  {ok, #s{files=Fs, n2o=N2O}}.

traverse_log([F|Fs], D0, Min0, Max0) ->
  case read_next(F) of
    {ok, {entry, N, Offset}} ->
      {D, Min, Max} = update_idx_append(Offset, N, D0, Min0, Max0),
      read_next([F|Fs], D, Min, Max);
    {ok, {Trunc, N}}
      when Trunc =:= trunc_head;
	   Trunc =:= trunc_tail ->
      {D, Min, Max} = update_idx_trunc(Trunc, D0, Min0, Max0),
      read_next([F|Fs], D, Min, Max);
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
append_term(N, Term, #f{size=Size}) ->
  TermBin   = erlang:term_to_binary(Term),
  EntryBin  = <<?entry_log, N:64/integer, TermBin/binary>>,
  write_log(EntryBin, F).

append_trunc(Trunc, N, F) ->
  EntryBin  = <<a2m(Trunc), N:64/integer>>,
  write_log(EntryBin, F).

write_log(Bin, F) ->
  Size = erlang:size(Bin),
  Hash = crypto:sha(Bin),
  ok = file:pwrite(F#f.fd, F#f.size, <<Hash/binary,
				       Size:64/integer,
				       Bin/binary>>),
  F#f{size = F#f.size + 20 + 8 + Size}.

a2m(trunc_head) -> ?entry_trunc_head;
a2m(trunc_tail) -> ?entry_trunc_tail.

%%%_ * Internals gc ----------------------------------------------------
gc(Files, Idx, Min, Max) ->
  Files.

%%%_ * Internals index -------------------------------------------------
update_idx_append(Offset, Idx, Min, Max) ->
  {dict:store(Max+1, Offset, Idx), Min, Max+1}.

update_idx_trunc(Trunc, N, Idx, Min, Max) ->
  %% remove mappings from Idx.
  %% update min/max
  Idx.

%%%_ * Internals misc --------------------------------------------------
maybe_switch_log([#f{size=Size}|_] = Files, MaxSize)
  when Size < MaxSize ->
  Files;
maybe_switch_log([F|_] = Files, _MaxSize) ->
  Start = F#f.start + F#f.size,
  File = log_filename(Path, Start),
  {ok, FD} = file:open(File, [read, write, binary]),
  [#f{name=File, start=Start, fd=FD, size=0} | Files].


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
