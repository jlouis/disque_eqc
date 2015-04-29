-module(disque_eqc).
-compile(export_all).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-define(Q, <<"test_queue">>).
-record(state, {
	contents = [],
	gotten = []
}).

initial_state() -> #state{}.

%% Queue length
%% -----------------------------------------------------------------------
qlen() ->
    {ok, Len} = disque:qlen(whereis(disque_conn), ?Q),
    Len.
    
qlen_args(_S) ->
    [].
    
qlen_post(#state { contents = C }, [], Res) ->
    eq(Res, length(C)).

%% Adding jobs
%% -----------------------------------------------------------------------
addjob_opts() -> #{ timeout => 300 }.

addjob(Job, Opts) ->
    case disque:addjob(whereis(disque_conn), ?Q, Job, Opts) of
        {ok, JobID} -> JobID;
        {error, Err} -> {error, Err}
    end.
    
addjob_args(_S) ->
    [binary(), addjob_opts()].

addjob_next(#state { contents = C } = State, JobID, [Job, _]) ->
    State#state { contents = C ++ [{JobID, Job}] }.

addjob_post(_S, [_Job, _], {error, Err}) -> {error, Err};
addjob_post(_S, [_Job, _], JobID) when is_binary(JobID) -> true.

%% Getting jobs out of the queue
%% -----------------------------------------------------------------------
getjob(Count, Timeout) ->
    disque:getjob(whereis(disque_conn), [?Q], #{ count => Count, timeout => Timeout }).
    
getjob_args(#state { contents = []}) -> [1, 1];
getjob_args(#state { contents = Cs}) -> [choose(1, length(Cs)), 0].

getjob_next(#state { contents = [] } = State, _, _) ->
    State;
getjob_next(#state { contents = Cs, gotten = Gotten } = State, _, [Count, _Timeout]) ->
    {Taken, Rest} = lists:split(Count, Cs),
    State#state { contents = Rest, gotten = Gotten ++ Taken }.

getjob_post(#state { contents = [] }, [_Count, _Timeout], Res) ->
    eq(Res, {ok, undefined});
getjob_post(#state { contents = Cs }, [Count, _Timeout], Res) ->
    {Taken, _} = lists:split(Count, Cs),
    case Res of
        {ok, Jobs} -> eq(Jobs, [[?Q, ID, X] || {ID, X} <- Taken]);
        _Otherwise -> {error, Res}
    end.

%% Peeking the queue
%% -----------------------------------------------------------------------
qpeek(Count) ->
    disque:qpeek(whereis(disque_conn), ?Q, Count).
    
qpeek_args(_S) -> [int()].

qpeek_post(#state { contents = [] }, _, Res) ->
    eq(Res, {ok, []});
qpeek_post(_S, [0], Res) -> eq(Res, {ok, []});
qpeek_post(#state { contents = Cs }, [Count], Res) when Count > 0 ->
    {Taken, _} = lists:split(min(length(Cs), Count), Cs),
    eq(Res, {ok, [[ID, X] || {ID, X} <- Taken]});
qpeek_post(#state { contents = Cs }, [Count], Res) when Count < 0 ->
    {Taken, _} = lists:split(min(length(Cs), abs(Count)), lists:reverse(Cs)),
    eq(Res, {ok, [[ID, X] || {ID, X} <- Taken]}).
    
%% Acking jobs in the queue
%% -----------------------------------------------------------------------
ackjob(JobIDs) ->
    disque:ackjob(whereis(disque_conn), JobIDs).
    
ackjob_pre(#state { contents = C, gotten = G }) -> (C /= []) orelse (G /= []).

ackjob_args(#state { contents = C, gotten = G}) ->
    IDs = [ID || {ID, _} <- C] ++ [ID || {ID, _} <- G],
    [list(elements(IDs))].
    
ackjob_pre(#state { contents = C}, [JobIDs]) ->
    lists:all(fun(ID) -> lists:keymember(ID, 1, C) end, JobIDs).
    
ackjob_next(#state { contents = C, gotten = G } = State, _, [JobIDs]) ->
    F = fun(JobID, Cts) -> lists:keydelete(JobID, 1, Cts) end,
    NewC = lists:foldl(F, C, JobIDs),
    NewG = lists:foldl(F, G, JobIDs),
    State#state { contents = NewC, gotten = NewG }.

ackjob_post(_S, [JobIDs], Res) ->
    eq(Res, {ok, length(lists:usort(JobIDs))}).

%% Fastacking
%% -----------------------------------------------------------------------
fastack(JobIDs) ->
    disque:fastack(whereis(disque_conn), JobIDs).
    
fastack_pre(#state { contents = C, gotten = G }) -> (C /= []) orelse (G /= []).

fastack_args(#state { contents = C, gotten = G}) ->
    IDs = [ID || {ID, _} <- C] ++ [ID || {ID, _} <- G],
    [list(elements(IDs))].
    
fastack_pre(#state { contents = C}, [JobIDs]) ->
    lists:all(fun(ID) -> lists:keymember(ID, 1, C) end, JobIDs).
    
fastack_next(#state { contents = C, gotten = G } = State, _, [JobIDs]) ->
    F = fun(JobID, Cts) -> lists:keydelete(JobID, 1, Cts) end,
    NewC = lists:foldl(F, C, JobIDs),
    NewG = lists:foldl(F, G, JobIDs),
    State#state { contents = NewC, gotten = NewG }.

fastack_post(_S, [JobIDs], Res) ->
    eq(Res, {ok, length(lists:usort(JobIDs))}).

%% SETUP/CLEANUP
%% -----------------------------------------------------------------------
setup() ->
    {ok, Pid} = disque:start_link(),
    register(disque_conn, Pid),
    Pid.

empty_q(QName) ->
    {ok, Len} = disque:qlen(whereis(disque_conn), ?Q),
    empty_q(QName, Len).
    
empty_q(_QName, 0) -> ok;
empty_q(QName, Len) when Len > 0 ->
    case disque:getjob(whereis(disque_conn), [QName], #{ count => Len }) of
        {ok, []} -> ok;
        {ok, Jobs} ->
            {ok, _} = disque:ackjob(whereis(disque_conn), [ID || {ID, _} <- Jobs]),
            empty_q(QName)
    end.

%% PROPERTY SECTION
%% -----------------------------------------------------------------------
weight(_S, qlen) -> 30;
weight(_S, qpeek) -> 30;
weight(_S, addjob) -> 100;
weight(_S, ackjob) -> 100;
weight(#state { contents = [] }, getjob) -> 3;
weight(_S, _) -> 100.

prop_disque() ->
  ?SETUP(fun() -> Pid = setup(), fun() -> exit(Pid, kill) end end,
  ?FORALL(Attempts, ?SHRINK(1, [20]),
  ?FORALL(Cmds, commands(?MODULE),
    ?SOMETIMES(1, ?ALWAYS(Attempts, 
      begin
          empty_q(?Q),
          {H,S,R} = run_commands(?MODULE, Cmds),
          pretty_commands(?MODULE, Cmds, {H,S,R},
            aggregate(command_names(Cmds),
            measure(length, length(Cmds),
              R == ok)))
      end))))).

%% BUGS FOUND??
%% -----------------------------------------------------------------------

%% This bug manifests itself as a nondeterministic bug some times. It may be relevant
%% what came before, but I've seen the error happen with this set of messages at least.
bug1(Attempts) ->
  Cmds =
    [{set,{var,1},{call,disque_eqc,addjob,[<<>>,#{timeout => 300}]}},
     {set,{var,2},
          {call,disque_eqc,addjob,
                [<<209,100,0,105,182,14,6,2,62>>,#{timeout => 300}]}},
     {set,{var,3},
          {call,disque_eqc,addjob,[<<39,157,129,26,95,242,121>>,#{timeout => 300}]}},
     {set,{var,4},
          {call,disque_eqc,addjob,[<<232,223,179,218,130,58>>,#{timeout => 300}]}},
     {set,{var,5},{call,disque_eqc,addjob,[<<>>,#{timeout => 300}]}},
     {set,{var,6},{call,disque_eqc,addjob,[<<237,158,171,168,97>>,#{timeout => 300}]}},
     {set,{var,7},{call,disque_eqc,addjob,[<<"?~">>,#{timeout => 300}]}},
     {set,{var,8},
          {call,disque_eqc,addjob,[<<61,96,195,99,167,139>>,#{timeout => 300}]}},
     {set,{var,9},{call,disque_eqc,qlen,[]}}],
  eqc:check(prop_disque(), [Attempts, Cmds]).
 
