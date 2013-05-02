%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%% @copyright Bjorn Jensen-Urstad 2013
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(eraft_db).
-behaviour(gen_server).

%%%_* Exports ==========================================================
-export([
        ]).

%% gen_server
-export([ init/1
        , terminate/2
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , code_change/3
        ]).

%%%_* Macros ===========================================================
-define(ets_init_options, [set, public]).
-define(ets_load_options, [{verify, true}]).
-define(ets_dump_options, [{extended_info, {object_count, md5sum}}]).

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(s, { file
           , tref
           , dump_min
           , dump_max
           , dump_pid
           }).

%%%_ * API -------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%_ * gen_server callbacks --------------------------------------------
init([]) ->
  erlang:process_flag(trap_exit, true),
  {ok, Path} = application:get_env(eraft, path_db),
  {ok, Min}  = application:get_env(eraft, dump_min),
  {ok, Max}  = application:get_env(eraft, dump_max),
  case file:exists(File) of
    true  -> load_ets(File);
    false -> init_ets()
  end,
  {ok, #s{file=File, dump_min=Min, dump_max=Max,
          tref=next_snapshot(undefined)}}.

handle_call(_, _ _) ->
  {reply, ok, S}.

handle_cast(Msg, S) ->
  {stop, {bad_cast, Msg}, S}.

handle_info(dump, S) ->
  ?hence(S#s.dump_pid =:= undefined),
  Pid = erlang:spawn_link(fun() -> take_dump(Filename) end),
  {noreply, S#s{dump_pid=Pid}};

handle_info({'EXIT', Pid, normal}, S#s{dump_pid=Pid} = S) ->
  truncate_log(),
  {noreply, schedule_dump(S)};
handle_info({'EXIT', Pid, Rsn}, S#s{dump_pid=Pid} = S) ->
  {stop, Rsn, S}.

handle_info(Msg, S) ->
  ?warning("~p", [Msg]),
  {noreply, S}.

%%%_ * Internals -------------------------------------------------------
init_ets(File) ->
  ?MODULE = ets:new(?MODULE, ?ets_init_options).

load_ets(File) ->
  {ok, ?MODULE} = ets:file2tab(File, ?ets_load_options).

take_snapshot(File) ->
  ok = ets:tab2file(?MODULE, File, ?ets_dump_options),

schedule_snapshot(S) ->
  [{ok, cancel} = timer:cancel(S#s.tref) || S#s.tref =/= undefined],
  {ok, TRef} = timer:send_interval(
                 take_snapshot, eraft_util:random(S#s.dump_min,
						  S#s.dump_max)),
  S#s{tref=TRef}.


filename_db()      -> "db".
filename_db_tmp()  -> "db.tmp".
filename_log()     -> "log".
filename_log_tmp() -> "log.tmp".

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:

