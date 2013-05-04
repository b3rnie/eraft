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
	, truncate/1
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
	   , next  = 1 :: integer()
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

%% @doc truncate(N) -> ok | {error, _}.
truncate(N) ->
  gen_server:call(?MODULE, {truncate, N}).

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
  [F|Fs] = maybe_switch_log(S#s.files, S#s.size),
  F      = do_append(Term, S#s.next, F),
  {reply, {ok, S#s.next} S#s{files=[F|Fs], next=S#s.next+1}};

handle_call({read, N}, _From, S) ->
  {reply, ok, S};

handle_call({truncate, Offset}, _From, S) ->
  Fs0 = maybe_switch_log(S#s.files, S#s.size),
  case do_truncate(Offset, Fs0) of
    {ok, Fs} -> {reply, ok, S#s{files=Fs}};
    {error, _Rsn} = Err -> {reply, Err, S#s{files=Fs0}}
  end.

handle_cast(Msg, S) ->
  {stop, {bad_cast, Msg}, S}.

handle_info(Msg, S) ->
  ?warning("~p", [Msg]),
  {noreply, S}.

%%%_ * Internals -------------------------------------------------------
init_new(Path, Size) ->
  File = log_filename(Path, 0),
  {ok, FD} = file:open(File, [read, write, binary]),
  {ok, #s{files=[#f{name=File, start=0, fd=FD, size=0}]}}.

init_old(Path, Size, Files) ->
  Fs = lists:map(
	 fun(File) ->
	     {ok, FD} = file:open(File, [read, write, binary]),
	     {ok, #file_info{size=Size}} = file:read_file_info(File),
	     #f{name=File, start=start_offset(File), fd=FD, size=Size}
	 end, Files),
  Entries = traverse(Fs),
  {ok, #s{files=Fs}}.

traverse_log([F|Fs]) ->
  case read_next(F) of
    {ok, {entry, N, Offset}} ->
      %% update mapping
      read_next([F|Fs]);
    {ok, {trunc, N}} ->
      %% update mapping
      read_next([F|Fs]);
    {error, eof} ->
      read_next(Fs);
    {error, short} when Fs =:= [] ->
      read_next(Fs).
  end;
traverse_log([]) ->
  ok.

%% TODO: rewrite
read_next(#f{fd=FD}) ->
  case file:read(FD, 20+8) of
    {ok, <<Hash:20/binary, Size:64/integer>>} ->
      case file:read(FD, Size) of
	{ok, <<?entry_log, N:64/integer, Term:/binary>>} = Bin
	  when size(Bin) =:= Size ->
	  Hash = crypto:sha(<<Size:64/integer,
			      ?entry_log,
			      N:64/integer,
			      Term:/binary>>),
	  {ok, {entry, N, Offset}};
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

maybe_switch_log([#f{size=Size}|_] = Files, MaxSize)
  when Size < MaxSize ->
  Files;
maybe_switch_log([F|_] = Files, _MaxSize) ->
  Start = F#f.start + F#f.size,
  File = log_filename(Path, Start),
  {ok, FD} = file:open(File, [read, write, binary]),
  [#f{name=File, start=Start, fd=FD, size=0} | Files].

do_append(N, Term, #f{size=Size}) ->
  TermBin   = erlang:term_to_binary(Term),
  TermSize  = erlang:size(TermBin),
  EntryBin  = <<?entry_log, N:64/integer, TermBin/binary>>,
  EntrySize = 1 + 8 + TermSize,
  Hash      = crypto:sha(EntryBin),
  ok = file:pwrite(FD, Size, <<Hash/binary,
			       EntrySize:64/integer,
			       EntryBin/binary>>),
  F#f{size = 20 + 8 + EntrySize}.

do_truncate(Offset) ->
  %% lookup actual offset in log
  %%

log_filename(Path, Offset) ->
  File = lists:flatten(io_lib:format("~20..0B", [Offset])),
  filename:join(Path, File).

sort(Files) ->
  lists:sort(fun(A, B) -> start_offset(A) > start_offset(B) end, Files).

start_offset(File) ->
  erlang:list_to_integer(filename:basename(File)).

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
