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
%%%_* Macros ===========================================================

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(s, { path
	   , size
	   , files
	   , min_entry
	   , max_entry
	   }).

%%%_ * API -------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% append(Bin) -> {ok, integer()} | {error, _}.
append(Term) ->
  gen_server:call(?MODULE, {append, Term}).

%% read(Offset) -> {ok, binary()} | {error, _}.
read(Entry) ->
  gen_server:call(?MODULE, {read, Entry}).

truncate(Offset) ->
  gen_server:call(?MODULE, {truncate, Offset}).

%%%_ * gen_server callbacks --------------------------------------------
init([]) ->
  {ok, Path} = application:get_env(eraft, log_path),
  {ok, Size} = application:get_env(eraft, log_size),
  eraft_util:ensure_path(Path),
  case file:list_dir(Path) of
    {ok, []} -> init_new(Path, Size);
    {ok, Fs} -> init_old(Path, Size, sort(Fs))
  end.

terminate(_Rsn, S) ->
  lists:foreach(fun({_Offset, _Name, FD}) ->
		    ok = file:close(FD)
		end, S#s.files).

handle_call({append, Term}, _From, S) ->
  {} = do_append(Term),
  {reply, ok, S}.

handle_call({truncate, Offset}, _From, S) ->
  {reply, ok, S}.

handle_cast(Msg, S) ->
  {stop, {bad_case, Msg}, S}.

handle_info(Msg, S) ->
  ?warning("~p", [Msg]),
  {noreply, S}.

%%%_ * Internals -------------------------------------------------------
init_new(Path, Size) ->
  File = filename(Path, 0),
  {ok, Fd} = file:open(File, [read, write, binary]),
  {ok, #s{files=[{0, Name, Fd}]}}.

init_old(Path, Size, Files) ->
  ok.

traverse_log(Files) ->
  ok

do_append(Term, N, FD) ->
  TermBin  = erlang:term_to_binary(Term),
  TermSize = erlang:size(TermBin),
  Entry    = <<N:64/integer, TermSize:64/integer, TermBin/binary>>,
  Hash     = crypto:sha(Entry),
  Hdr      = <<16#DD, Hash/binary>>,
  ok = file:pwrite(FD, Offset, <<Hdr/binary, Entry/binary>>),

filename(Path, Offset) ->
  File = lists:flatten(io_lib:format("~20..0B", [Offset])),
  filename:join(Path, File).

sort(Files) ->
  lists:sort(fun(A, B) -> start_offset(A) < start_offset(B) end, Files).

start_offset(File) ->
  erlang:list_to_integer(filename:basename(File)).

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
