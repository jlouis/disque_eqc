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
weight(_S, addjob) -> 100;
weight(_S, ackjob) -> 100;
weight(#state { contents = [] }, getjob) -> 5;
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
bug1(Attempts) ->
  Cmds =
    [{set,{var,1},{call,disque_eqc,qlen,[]}},
     {set,{var,2},{call,disque_eqc,qlen,[]}},
     {set,{var,3},{call,disque_eqc,addjob,[<<212,88,200,14>>,#{timeout => 300}]}},
     {set,{var,4},{call,disque_eqc,ackjob,[[]]}},
     {set,{var,5},{call,disque_eqc,addjob,[<<" 3(R">>,#{timeout => 300}]}},
     {set,{var,6},{call,disque_eqc,addjob,[<<"3ªÝð">>,#{timeout => 300}]}},
     {set,{var,7},{call,disque_eqc,qlen,[]}},
     {set,{var,8},{call,disque_eqc,qlen,[]}},
     {set,{var,9},{call,disque_eqc,addjob,[<<"ü">>,#{timeout => 300}]}},
     {set,{var,10},{call,disque_eqc,qlen,[]}},
     {set,{var,11},{call,disque_eqc,qlen,[]}},
     {set,{var,12},{call,disque_eqc,qlen,[]}},
     {set,{var,13},{call,disque_eqc,addjob,[<<21>>,#{timeout => 300}]}},
     {set,{var,14},{call,disque_eqc,addjob,[<<"*">>,#{timeout => 300}]}},
     {set,{var,15},{call,disque_eqc,qlen,[]}},
     {set,{var,16},{call,disque_eqc,qlen,[]}},
     {set,{var,17},{call,disque_eqc,addjob,[<<"l±">>,#{timeout => 300}]}},
     {set,{var,18},{call,disque_eqc,qlen,[]}},
     {set,{var,19},{call,disque_eqc,addjob,[<<134,141>>,#{timeout => 300}]}},
     {set,{var,20},{call,disque_eqc,ackjob,[[]]}},
     {set,{var,21},{call,disque_eqc,qlen,[]}},
     {set,{var,22},{call,disque_eqc,qlen,[]}},
     {set,{var,23},{call,disque_eqc,addjob,[<<>>,#{timeout => 300}]}},
     {set,{var,24},{call,disque_eqc,addjob,[<<"mí">>,#{timeout => 300}]}},
     {set,{var,25},{call,disque_eqc,qlen,[]}},
     {set,{var,26},{call,disque_eqc,qlen,[]}},
     {set,{var,27},{call,disque_eqc,addjob,[<<222,181,150>>,#{timeout => 300}]}},
     {set,{var,28},{call,disque_eqc,addjob,[<<2>>,#{timeout => 300}]}},
     {set,{var,29},{call,disque_eqc,addjob,[<<"ÔTëû">>,#{timeout => 300}]}},
     {set,{var,30},{call,disque_eqc,qlen,[]}},
     {set,{var,31},{call,disque_eqc,addjob,[<<94,22,29,127>>,#{timeout => 300}]}},
     {set,{var,32},{call,disque_eqc,addjob,[<<"¡2">>,#{timeout => 300}]}}],
  eqc:check(prop_disque(), [Attempts, Cmds]).

bug2(Attempts) ->
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
 
