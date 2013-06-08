%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc This is an attempt to make a minimal implementation
%%%      of the raft consensus algorithm
%%% @copyright Bjorn Jensen-Urstad 2013
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(eraft_server).
-behaviour(gen_fsm).

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
-record(s,
        { %% common
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

-record(request_vote_req,
        { term           = throw('term')          :: integer()
        , id             = throw('id')            :: node()
        , last_log_idx   = throw('last_log_idx')  :: integer()
        , last_log_term  = throw('last_log_term') :: integer()
        }).

-record(request_vote_resp,
        { id             = throw('id')
        , term           = throw('term')
        , granted        = throw('granted')
        }).

-record(append_entries_req,
        { term           :: integer()
        , id             :: node()
        , prev_log_idx   :: integer()
        , prev_log_term  :: integer()
        , entries        :: list()
        , commit_idx     :: integer()
        }).

-record(append_entries_resp,
        { id            = throw('id')
        , term          = throw('term')
        , success       = throw('success')
        }).

%%%_ * API -------------------------------------------------------------
start_link() ->
  gen_fsm:start_link({local, ?MODULE}, ?MODULE, [], []).

append(Entry) ->
  gen_fsm:sync_send_event(?MODULE, {append, Entry}).

%%%_ * gen_fsm callbacks -----------------------------------------------
init([]) ->
  {ok, ID}    = application:get_env(eraft, id),
  {ok, Nodes} = application:get_env(eraft, nodes),
  NextIndex   = [{Node, 1} || Node <- Nodes],
  {ok, follower, #s{id=ID, nodes=Nodes, next_index=NextIndex, tref=reset_timeout(undefined)}}.

terminate(_Rsn, _Sn, _S) ->
  ok.

handle_event(stop, _Sn, S) ->
  {stop, normal, S};
handle_event(Msg, _Sn, S) ->
  {stop, {error, {bad_event, Msg}}, S}.

handle_sync_event(Msg, _From, _Sn, S) ->
  {stop, {error, {bad_event, Msg}}, S}.

handle_info(Msg, Sn, S) ->
  ?warning("~p", [Msg]),
  {next_state, Sn, S}.

%%%_ * gen_fsm states --------------------------------------------------

%% stale
follower(#request_vote_req{term=Term, id=ID}, #s{term=CurrentTerm} = S)
  when Term < CurrentTerm ->
  send(#request_vote_resp{id=S#s.id, granted=false, term=CurrentTerm}, ID),
  {next_state, follower, S};

%% resend vote
follower(#request_vote_req{term=Term, id=ID} = R, #s{term=Term, vote=ID} = S) ->
  ?hence(has_complete_log(R#request_vote_req.last_log_idx,
                          R#request_vore_req.last_log_term)),
  send(#request_vote_resp{id=S#s.id, granted=true, term=Term}, ID),
  {next_state, follower, S#s{tref=election_timeout(S#s.tref)}};

%% already voted
follower(#request_vote_req{term=Term, id=ID} = R, #s{term=Term, vote=Vote} = S)
  when Vote =/= undefined ->
  send(#request_vote_resp{id=S#s.id, granted=false, term=Term}, ID),
  {next_state, follower, S};

%% can vote
follower(#request_vote_req{term=Term, id=ID} = R, S) ->
  case has_complete_log(R#request_vote_req.last_log_idx,
                        R#request_vote_req.last_log_term) of
    true  ->
      send(#request_vote_resp{id=S#s.id, granted=false, term=Term}, ID),
      {next_state, follower, S#s{term=Term, vote=ID, tref=election_timeout(S#s.tref)}};
    false ->
      send(#request_vote_resp{id=S#s.id, granted=false, term=Term}, ID),
      {next_state, follower, S#s{term=Term, vote=undefined}}
  end;

%% stale
follower(#append_entries_req{term=Term, id=ID}, #s{term=CurrentTerm} = S)
  when Term < CurrentTerm ->
  send(#append_entries_resp{id=S#s.id, term=CurrentTerm, success=false}, ID),
  {next_state, follower, S};

%% can append
follower(#append_entries_req{term=Term, id=ID}, #s{term=CurrentTerm} = S) ->
  TRef = election_timeout(S#s.tref),
  Vote = case Term > CurrentTerm of
           true  -> undefined;
           false -> S#s.vote
         end,
  case has_entry() of
    true ->
      %% delete conflicting
      %% append
      send(#append_entries_resp{id=S#s.id, term=Term, success=true}, ID),
      {next_state, follower, S#s{term=Term, vote=Vote, tref=TRef}};
    false ->
      send(#append_entries_resp{id=S#s.id, term=Term, success=false}, ID),
      {next_state, follower, S#s{term=Term, vote=Vote, tref=TRef}}
  end;

%% stale
follower(#request_vote_resp{}, S) ->
  {next_state, follower, S};

%% stale
follower(#append_entries_resp{}, S) ->
  {next_state, follower, S}.

%% start election
follower({timeout, Ref, _Msg} #s{tref=Ref, other=Nodes} = S) ->
  Term = S#s.term+1,
  Req = #request_vote_req{id=S#s.id, term=Term,
                          last_log_idx=eraft_log:last_log_idx(),
                          last_log_term=eraft_log:last_log_term()},
  broadcast(Req, Nodes),
  {next_state, candidate, S#s{term=S#s.term+1, vote=S#s.id, tref=election_timeout(S#s.tref)}}.

%% stale
candidate(#request_vote_req{term=Term, id=ID}, #s{term=CurrentTerm} = S)
  when Term < CurrentTerm ->
  send(#request_vote_resp{id=S#s.id, term=CurrentTerm, granted=false}, ID),
  {next_state, candidate, S};

%% someone else tries to become leader aswell
candidate(#request_vote_req{term=Term, id=ID}, #s{term=Term, vote=Vote} = S) ->
  ?hence(S#s.id =:= S#s.vote),
  send(#request_vote_resp{id=S#s.id, term=Term, granted=false}, ID),
  {next_state, candidate, S};

%% stepdown
candidate(#request_vote_req{term=Term, id=ID}, S) ->
  case has_complete_log(R#request_vote_req.last_log_idx,
                        R#request_vote_req.last_log_term) of
    true ->
      send(#request_vote_resp{id=S#s.id, term=Term, granted=true}, ID),
      {next_state, follower, S#s{term=Term, vote=ID, tref=election_timeout(S#s.tref)}};
    false ->
      send(#request_vote_resp{id=S#s.id, term=Term, granted=false}, ID),
      {next_state, follower, S#s{term=Term, vote=undefined}}
  end;

%% stale
candidate(#append_entries{term=Term, id=ID}, #s{term=CurrentTerm} = S)
  when Term < CurrentTerm ->
  send(#append_entries_resp{id=S#s.id, term=CurrentTerm, success=false}, ID),
  {next_state, candidate, S};

candidate(#append_entries{term=Term}, #s{term=CurrentTerm} = S) ->
  TRef = election_timeout(S#s.tref),
  Vote =
    case Term > CurrentTerm of
      true  -> undefined;
      false -> S#s.vote
    end,
  case has_entry(R) of
    true ->
      %% delete conflicting
      %% append
      send(#append_entries_resp{id=S#s.id, term=Term, success=true}, ID),
      {next_state, follower, S#s{term=Term, vote=Vote, tref=TRef}};
    false ->
      send(#append_entries_resp{id=S#s.id, term=Term, success=false}, ID),
      {next_state, follower, S#s{term=Term, vote=Vote, tref=TRef}}
  end;

%% stale
candidate(#request_vote_resp{term=Term}, #s{term=CurrentTerm} = S)
  when Term < CurrentTerm ->
  {next_state, candidate, S}.

%% wait for majority
candidate(#request_vote_resp{}, S) ->
  {next_state, candidate, S};

%% stale
candidate(#append_entries_resp{}, S) ->
  {next_state, candidate, S};

%% start new election
candidate({timeout, Ref, _Msg}, #s{tref=Ref} = S) ->
  Term = S#s.term+1,
  Req = #request_vote_req{id=S#s.id, term=Term,
                          last_log_idx=eraft_log:last_log_idx(),
                          last_log_term=eraft_log:last_log_term()},
  broadcast(Req, S#s.nodes),
  %% TODO: broadcast request_vote
  {next_state, candidate, S#s{term=S#s.term+1, vote=S#s.id, tref=election_timeout(S#s.tref)}}.

%% stale
leader(#request_vote_req{term=Term, id=ID}, #s{term=CurrentTerm} = S)
  when Term < CurrentTerm ->
  send(#request_vote_resp{id=S#s.id, term=CurrentTerm, granted=false}, ID),
  {next_state, leader, S};

%% someone else tried to become leader
leader(#request_vote_req{term=Term, id=ID}, #s{term=Term} = S) ->
  ?hence(S#s.id =:= S#s.vote),
  send(#request_vote_resp{id=S#s.id, term=Term, granted=false}, ID),
  {next_state, leader, S}.

%% stepdown
leader(#request_vote_req{term=Term, id=ID} = R, S) ->
  case has_complete_log(R#request_vote_req.last_log_idx,
                        R#request_vote_req.last_log_term) of
    true ->
      send(#request_vote_resp{id=S#s.id, term=Term, granted=true}, ID),
      {next_state, follower, S#s{term=Term, vote=ID, tref=election_timeout(S#s.tref)}};
    false ->
      send(#request_vote_resp{id=S#s.id, term=Term, granted=false}, ID),
      {next_state, follower, S#s{term=Term, vote=undefined}}
  end;

leader(#append_entries_req{term=Term}, #s{term=CurrentTerm} = S) ->
  TRef = election_timeout(S#s.tref),
  Vote =
    case Term > CurrentTerm of
      true  -> undefined;
      false -> S#s.vote
    end,
  case has_entry(R) of
    true ->
      %% delete conflicting
      %% append
      send(#append_entries_resp{id=S#s.id, term=Term, success=true}, ID),
      {next_state, follower, S#s{term=Term, vote=Vote, tref=TRef}};
    false ->
      send(#append_entries_resp{id=S#s.id, term=Term, success=false}, ID),
      {next_state, follower, S#s{term=Term, vote=Vote, tref=TRef}}
  end;

%% stale
leader(#request_vote_resp{}, S) ->
  {next_state, leader, S};

leader(#append_entries_resp{}, S) ->
  {next_state, leader, S};

leader({timeout, Ref, _Msg}, #s{tref=Ref} = S) ->
  %% broadcast heartbeat
  Req = #append_entries_req{ term = S#s.term
                           , id   = S#s.id
                           },
  broadcast(Req, S#s.other),
  {next_state, leader, S#s{tref=heartbeat_timeout(S#s.tref)}}.

%%%_ * Internals -------------------------------------------------------
has_complete_log(LastLogIdx, LastLogTerm) ->
  LocalLastLogTerm = eraft_log:last_log_term(),
  LocalLastLogIdx  = eraft_log:last_log_idx(),
  (LastLogTerm > LocalLastLogTerm)
    orelse
      (LastLogTerm =:= LocalLastLogTerm andalso
       LastLogIdx >= LocalLastLogIdx).

has_entry(PrevLogIdx, PrevLogTerm) ->
  ok.

heartbeat_timeout(TRef) ->
  [gen_fsm:cancel_timer(TRef) || TRef =/= undefined],
  gen_fsm:start_timer(eraft_util:random(150, 300), meh).

election_timeout(TRef) ->
  [gen_fsm:cancel_timer(TRef) || TRef =/= undefined],
  gen_fsm:start_timer(eraft_util:random(500, 900), meh).

broadcast(Msg, Nodes) ->
  lists:foreach(fun(N) -> send(Msg, N) end, Nodes).

send(Msg, ID) ->
  gen_fsm:send_event(ID, Msg).

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
