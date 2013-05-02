%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc This is an attempt to make a minimal implementation
%%%      of the raft consensus algorithm
%%% @copyright Bjorn Jensen-Urstad 2013
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(eraft_server).
-behaviour(gen_server).

%%%_* Exports ==========================================================
-export([ start_link/0
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
-include_lib("stdlib2/prelude.hrl").

%%%_* Macros ===========================================================
%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(s, {
           %% common
             role  = follower :: leader | candidate | follower
           , term  = 1        :: integer()
           , id               :: node()
	   , tref             :: _
           , nodes            :: list()
           %% follower
	   , vote             :: undefined | integer()
           , leader           :: node()
           %% leader
           , request          :: any()
           , next_index       :: list()
           , call             :: any()
           }).

-record(rv, { term           :: integer()
            , id             :: node()
            , last_log_idx   :: integer()
            , last_log_term  :: integer()
            }).

-record(ae, { term           :: integer()
            , id             :: node()
            , prev_log_idx   :: integer()
            , prev_log_term  :: integer()
            , entries        :: list()
            , commit_idx     :: integer()
            }).

-record(resp, {uuid, id, data}).

%%%_ * API -------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

append(Msg) ->
  gen_server:call(?MODULE, {append, Msg}).

%%%_ * gen_server callbacks --------------------------------------------
init([]) ->
  {ok, ID}    = application:get_env(eraft, id),
  {ok, Nodes} = application:get_env(eraft, nodes),
  NextIndex   = [{Node, 1} || Node <- Nodes],
  S           = #s{id=ID, nodes=Nodes, next_index=NextIndex},
  {ok, reset_timeout(S)}.

handle_call({append, Msg}, From, #s{role=leader} = S) ->
  {reply, ok, S};

handle_call({append, Msg}, From, S) ->
  {reply, {error, {leader, S#s.leader}}, S};

handle_call(stop, _From, S) ->
  {stop, normal, ok, S}.

handle_cast(Msg, S) ->
  {stop, {bad_cast, Msg}, S}.

%% rv stale
handle_info(#rv{term=Term} = RV, #s{term=CurrentTerm} = S)
  when Term < CurrentTerm ->
  {noreply, S};

%% rv same/bigger term
handle_info(#rv{} = RV, #s{term=CurrentTerm} = S0) ->
  S1 = rv_maybe_stepdown(RV#rv.term, S0),
  S2 = maybe_change_term(RV#rv.term, S1),
  S3 = rv_maybe_grant(RV, S2),
  S  = reset_timeout(S3),
  {noreply, S}

%% ae stale
handle_info(#ae{term=Term} = AE, #s{term=CurrentTerm} = S)
  when Term < CurrentTerm ->
  ?debug("received stale request from ~p", [ID]),
  {noreply, S};

%% ae same/bigger term
handle_info(#ae{} = AE, S0) ->
  S1 = maybe_change_term(AE#ae.term, S0),
  S2 = ae_stepdown(S1),
  S3 = reset_timeout(S2),
  S  = ae_try_append(AE, S3),
  {noreply, S}.

%% response stale
handle_info(#resp{} = R, S)
  when R#resp.uuid =/= S#s.call_uuid ->
  {noreply, S};

handle_info(#resp{} = R, S) ->
  
  {noreply, S}.

%% heartbeat
handle_info(timeout, #s{role=leader}, S) ->
  AE = #ae{ term          = S#s.term
          , id            = S#s.id
          , prev_log_idx  = undefined
          , last_log_idx  = undefined
          , prev_log_term = undefined
          , entries       = []
          , commit_idx    = undefined},
  broadcast(AE, S#s.nodes),
  {noreply, S};

handle_info(timeout, #s{role=Role} = S)
  when Role =:= follower;
       Role =:= candidate ->
  S1 = S0#s{term=S0#s.term+1}, %increase current term
  S2 = S1#s{role=candidate},   %transition to candidate
  S3 = reset_timeout(S2),
  S  = election_start(S3),
  {noreply, S}.

%%%_ * Internals request vote ------------------------------------------
rv_maybe_stepdown(Term, #s{term=CurrentTerm})
  when Term > CurrentTerm,
       (S#s.role =:= candidate orelse
        S#s.role =:= leader) ->
  S#s{role=follower};
rv_maybe_stepdown(#rv{}, S) ->
  S.

rv_maybe_grant(#rv{id=ID}, S#s{vote=Vote})
  when Vote =/= undefined,
       Vote =/= ID ->
  ?debug("~p: already voted", [S#s.term]),
  S;
rv_maybe_grant(#rv{term=Term, id=ID}, S) ->
  case
    (RV#rv.last_log_term > last_log_term()) orelse
    (RV#rv.last_log_term =:= last_log_term() andalso
     RV#rv.last_log_idx >= last_log_index()) of
    true ->
      ?debug("~p: granting vote to ~p", [S#s.term, ID]),
      send(
      S#s{vote=ID, tref=reset_timeout(S#s.tref)};
    false ->
      ?debug("~p: denying vote to ~p", [S#s.term, ID]),
      S
  end.

%%%_ * Internals append entries ----------------------------------------
ae_stepdown(#s{role=leader} = S)    -> S#s{role=follower};
ae_stepdown(#s{role=candidate} = S) -> S#s{role=follower};
ae_stepdown(#s{role=follower} = S)  -> S.

ae_try_append(#ae{} = AE, S) ->
  case log_contains_entry(PrevLogIdx, PrevLogTerm) of
    false ->
      S
    true  ->
      delete_conflicting_entries(Entries),
      append_new_entries(Entries),
      S
  end.

%%%_ * Internals election ----------------------------------------------
election_start(S) ->
  RV = #rv{term=S#s.term,
           id=S#s.id,
           last_log_idx = undefined,
           last_log_term=undefinded},
  broadcast(RV, S#s.nodes),
  S.

%%%_ * Internals misc --------------------------------------------------
reset_timeout(S) ->
  [{ok, cancel} = timer:cancel(TRef) || S#s.tref =/= undefined],
  {ok, TRef} = timer:send_after(timeout, eraft_util:random(150, 300)),
  S#s{tref=TRef}.

maybe_change_term(Term, #s{term=CurrentTerm})
  when Term > CurrentTerm ->
  S#s{term=Term, vote=undefined};
maybe_change_term(_, S) ->
  S.

broadcast(Msg, Nodes) ->
  lists:foreach(fun(N) -> send(Msg, N) end, Nodes).

send(Msg, Node) ->
  {?MODULE, Node} ! Msg.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
