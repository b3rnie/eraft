%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%% @copyright Bjorn Jensen-Urstad 2012
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(eraft_util).

%%%_* Exports ==========================================================
-export([ random/2
	, ensure_path/1
        , quorum/1
        ]).

%%%_ * API -------------------------------------------------------------
random(From, To) when From >=0, To >=0, From =< To ->
    From + random:uniform(To - From + 1) - 1.

ensure_path(Path) ->
  filelib:ensure_dir(filename:join([Path, "dummy"])).

quorum(N) ->
  erlang:trunc(N/2)+1.

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

random_test() ->
    random(2, 2),
    random(0, 10),
    true = lists:member(random(2, 3), [2, 3]).


%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
