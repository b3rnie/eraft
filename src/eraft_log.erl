%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc A log..
%%% @copyright Bjorn Jensen-Urstad 2013
%%%
%%% tail
%%% ------
%%% ^-----
%%%
%%% ------
%%%      ^
%%% head
%%% ------
%%% ^
%%%
%%% ------
%%% -----^
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(eraft_log).
-behaviour(gen_server).

%%%_* Exports ==========================================================
%% api
-export([ start_link/0
        , append/1
        , read/1
	, trunc_head/1
	, trunc_tail/1
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
	   , min   = 1            :: integer()
	   , next  = 1            :: integer()
	   , idx   = dict:new()   :: _
	   }).

%%%_ * API -------------------------------------------------------------
-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec append(any()) -> {ok, pos_integer()}.
append(Term) ->
  gen_server:call(?MODULE, {append, Term}).

-spec read(pos_integer()) -> maybe(_,_).
read(N)
  when erlang:is_integer(N) ->
  gen_server:call(?MODULE, {read, N}).

-spec trunc_head(pos_integer()) -> maybe(_, _).
trunc_head(N)
  when erlang:is_integer(N) ->
  gen_server:call(?MODULE, {trunc_head, N}).

-spec trunc_tail(pos_integer()) -> maybe(_, _).
trunc_tail(N)
  when erlang:is_integer(N) ->
  gen_server:call(?MODULE, {trunc_tail, N}).

%%%_ * gen_server callbacks --------------------------------------------
init([]) ->
  {ok, Path} = application:get_env(eraft, log_path),
  {ok, Size} = application:get_env(eraft, log_size),
  case filelib:wildcard(Path ++ "log_*.data") of
    {ok, []}    -> init_new(Path, Size);
    {ok, Files} -> F = fun(A, B) ->
                           file_offset(A) > file_offset(B)
                       end,
                   init_old(Path, Size, lists:sort(F, Files))
  end.

terminate(_Rsn, S) ->
  lists:foreach(fun(F) -> ok = file:close(F#.fd) end, S#s.files).

handle_call({append, Term}, _From, S) ->
  [F0|Fs]          = maybe_switch(S#s.files, S#s.size),
  {F, Offset}      = append_term(Term, F0, S#s.next),
  Idx              = update_idx(S#s.next, Offset, S#s.idx),
  {reply, {ok, S#s.next}, S#s{ files = [F|Fs]
			     , next  = S#s.next+1
                             , idx   = Idx
                             }};

handle_call({read, N}, _From, S) ->
  case dict:find(N, S#s.idx) of
    {value, Offset} -> {reply, {ok, do_read(Offset, S#s.files)}, S};
    error           -> {reply, {error, notfound}, S}
  end;

handle_call({Trunc, N}, _From, S)
  when (Trunc =:= trunc_tail orelse
        Trunc =:= trunc_head) andalso
       (N < S#s.min orelse
        N >= S#s.next) ->
  {reply, {error, bounds}, S};

handle_call({Trunc, N}, _From, S)
  when Trunc =:= trunc_head;
       Trunc =:= trunc_tail ->
  %% Update idx
  %% gc / truncate
  {Idx, Min, Next} = update_idx_trunc(Type, S#s.idx, S#s.min, S#s.next),
  Fs               = gc([F|Fs0], Idx, Min, Next),
  {reply, ok, S#s{files=Fs, idx=Idx, min=Min, max=Max}}.

handle_cast(Msg, S) ->
  {stop, {bad_cast, Msg}, S}.

handle_info(Msg, S) ->
  ?warning("~p", [Msg]),
  {noreply, S}.

%%%_ * Internals init --------------------------------------------------
init_new(Path, Size) ->
  File     = file_name(Path, 0),
  {ok, FD} = file:open(File, [read, write, binary]),
  F        = #f{name=File, start=0, fd=FD},
  {ok, #s{path=Path, size=Size, files=[F]}}.

init_old(Path, Size, Files) ->
  Fs0 = lists:map(
          fun(File) ->
              {ok, FD} = file:open(File, [read, write, binary]),
              {ok, #file_info{size=Size}} = file:read_file_info(File),
              #f{name=File, start=file_offset(File), fd=FD, size=Size}
          end, Files),
  {Fs, N20} = traverse_log(Fs, dict:new(), 0),
  {ok, #s{files=Fs, n2o=N2O}}.

traverse_log([F|Fs], {Acc, Idx0}, Pos) ->
  case read_pos(F, Pos) of
    {ok, {N, NextPos}} ->
      Idx = update_idx(N, Pos, Idx0),
      traverse_log([F|Fs], {Acc, Idx}, NextPos);
    {error, eof} ->
      traverse_log(Fs, {[F|Acc], Idx0}, 0);
    {error, short} when Fs =:= [] ->
      %% This can happen if we crash before everything
      %% was written
      %% TODO: write testcase for this!
      ok = file:truncate(F#f.fd),
      traverse_log(Fs, {[F#f{size=Pos}Acc], Idx}, 0}
  end;
traverse_log([], {Acc, Idx}, 0) ->
  {lists:reverse(Acc), Idx}.



%%%_ * Internals append ------------------------------------------------
append_term(Term, F, N) ->
  TermBin   = erlang:term_to_binary(Term),
  Size      = erlang:size(TermBin),
  EntryBin  = <<N:64/integer, Size:64/integer, TermBin/binary>>,
  Hash      = crypto:sha(EntryBin),
  ok = file:pwrite(F#f.fd, F#f.size, <<Hash/binary,
                                       EntryBin/binary>>),
  {F#f{size = F#f.size + 8 + 8 + Size + 20},
   F#f.start+F#f.size}.

%%%_ * Internals gc ----------------------------------------------------
gc(Files, Idx, Min, Max) ->
  Files.

%%%_ * Internals index -------------------------------------------------
update_idx(N, Pos, Idx) ->
  dict:store(N, Pos, Idx).

remove_range(From, To, Idx) ->
  lists:foldl(fun(N, D) ->
		  dict:erase(N, D)
	      end, Idx, lists:seq(From, To)).

%%%_ * Internals index -------------------------------------------------
do_read(Offset, Files) ->
  {[F], _} = lists:partition(fun(#f{start=Start, size=Size}) ->
                                 Offset >= Start,
                                 Offset < Start+Size
                             end, Files),
  {ok, {N, Bin, _Offset}} = read_pos(F, Offset-F#f.start),
  Bin.

read_pos(#f{fd=FD}, Pos) ->
  case file:pread(FD, Pos, 20+8+8) of
    {ok, <<Hash:20/binary, N:64/integer, Size:64/integer>>} ->
      case file:pread(FD, Size) of
	{ok, Bin} when erlang:size(Bin) =:= Size ->
	  Hash = crypto:sha(<<Size:64/integer, Bin/binary>>),
	  {ok, {N, Bin, Offset}};
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

%%%_ * Internals misc --------------------------------------------------
maybe_switch_log([#f{size=Size}|_] = Files, MaxSize)
  when Size < MaxSize ->
  Files;
maybe_switch_log([F|_] = Files, _MaxSize) ->
  Start = F#f.start + F#f.size,
  File = log_filename(Path, Start),
  {ok, FD} = file:open(File, [read, write, binary]),
  [#f{name=File, start=Start, fd=FD, size=0} | Files].

file_name(Path, Offset) ->
  File = lists:flatten(io_lib:format("log_~20..0B.data", [Offset])),
  filename:join(Path, File).

file_offset(File) ->
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
