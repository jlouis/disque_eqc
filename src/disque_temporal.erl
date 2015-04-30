-module(disque_temporal).
-compile(export_all).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eqc/include/eqc_temporal.hrl").

-define(Q, <<"test_queue">>).
-record(state, {
	contents = [],
	gotten = []
}).

initial_state() -> #state{}.

%% Generate a random connection
conn() -> oneof([disque_conn_1, disque_conn_2]).

%% Queue length
%% -----------------------------------------------------------------------
qlen(C) ->
    {ok, Len} = disque:qlen(C, ?Q),
    Len.
    
qlen_args(_S) -> [conn()].
    
qlen_post(#state { contents = C }, [_], Res) ->
    eq(Res, length(C)).

%% Adding jobs
%% -----------------------------------------------------------------------
addjob_opts() -> #{ timeout => 300 }.

addjob(C, Job, Opts) ->
    case disque:addjob(C, ?Q, Job, Opts) of
        {ok, JobID} ->
          tracer:record({addjob, JobID}),
          JobID;
        {error, Err} -> {error, Err}
    end.
    
addjob_args(_S) ->
    [conn(), binary(), addjob_opts()].

addjob_next(#state { contents = C } = State, JobID, [_, Job, _]) ->
    State#state { contents = C ++ [{JobID, Job}] }.

addjob_post(_S, [_, _Job, _], {error, Err}) -> {error, Err};
addjob_post(_S, [_, _Job, _], JobID) when is_binary(JobID) -> true.

%% Getting jobs out of the queue
%% -----------------------------------------------------------------------
% getjob(Count, Timeout) ->
%     disque:getjob(whereis(disque_conn), [?Q], #{ count => Count, timeout => Timeout }).
%     
% getjob_args(#state { contents = []}) -> [1, 1];
% getjob_args(#state { contents = Cs}) -> [choose(1, length(Cs)), 0].
% 
% getjob_next(#state { contents = [] } = State, _, _) ->
%     State;
% getjob_next(#state { contents = Cs, gotten = Gotten } = State, _, [Count, _Timeout]) ->
%     {Taken, Rest} = lists:split(Count, Cs),
%     State#state { contents = Rest, gotten = Gotten ++ Taken }.
% 
% getjob_post(#state { contents = [] }, [_Count, _Timeout], Res) ->
%     eq(Res, {ok, undefined});
% getjob_post(#state { contents = Cs }, [Count, _Timeout], Res) ->
%     {Taken, _} = lists:split(Count, Cs),
%     case Res of
%         {ok, Jobs} -> eq(Jobs, [[?Q, ID, X] || {ID, X} <- Taken]);
%         _Otherwise -> {error, Res}
%     end.

%% Peeking the queue
%% -----------------------------------------------------------------------
qpeek(C, Count) ->
    disque:qpeek(C, ?Q, Count).
    
qpeek_args(_S) -> [conn(), int()].

qpeek_post(#state { contents = [] }, _, Res) ->
    eq(Res, {ok, []});
qpeek_post(_S, [_, 0], Res) -> eq(Res, {ok, []});
qpeek_post(#state { contents = Cs }, [_, Count], Res) when Count > 0 ->
    {Taken, _} = lists:split(min(length(Cs), Count), Cs),
    eq(Res, {ok, [[ID, X] || {ID, X} <- Taken]});
qpeek_post(#state { contents = Cs }, [_, Count], Res) when Count < 0 ->
    {Taken, _} = lists:split(min(length(Cs), abs(Count)), lists:reverse(Cs)),
    eq(Res, {ok, [[ID, X] || {ID, X} <- Taken]}).
    
%% Acking jobs in the queue
%% -----------------------------------------------------------------------
ackjob(C, JobIDs) ->
    R = disque:ackjob(C, JobIDs),
    [tracer:record({ackjob, JobID}) || JobID <- JobIDs],
    R.
    
ackjob_pre(#state { contents = C, gotten = G }) -> (C /= []) orelse (G /= []).

ackjob_args(#state { contents = C, gotten = G}) ->
    IDs = [ID || {ID, _} <- C] ++ [ID || {ID, _} <- G],
    [conn(), list(elements(IDs))].
    
ackjob_pre(#state { contents = C}, [_, JobIDs]) ->
    lists:all(fun(ID) -> lists:keymember(ID, 1, C) end, JobIDs).
    
ackjob_next(#state { contents = C, gotten = G } = State, _, [_, JobIDs]) ->
    F = fun(JobID, Cts) -> lists:keydelete(JobID, 1, Cts) end,
    NewC = lists:foldl(F, C, JobIDs),
    NewG = lists:foldl(F, G, JobIDs),
    State#state { contents = NewC, gotten = NewG }.

ackjob_post(_S, [_, JobIDs], Res) ->
    eq(Res, {ok, length(lists:usort(JobIDs))}).

%% Fastacking
%% -----------------------------------------------------------------------
% fastack(JobIDs) ->
%     disque:fastack(whereis(disque_conn), JobIDs).
%     
% fastack_pre(#state { contents = C, gotten = G }) -> (C /= []) orelse (G /= []).
% 
% fastack_args(#state { contents = C, gotten = G}) ->
%     IDs = [ID || {ID, _} <- C] ++ [ID || {ID, _} <- G],
%     [list(elements(IDs))].
%     
% fastack_pre(#state { contents = C}, [JobIDs]) ->
%     lists:all(fun(ID) -> lists:keymember(ID, 1, C) end, JobIDs).
%     
% fastack_next(#state { contents = C, gotten = G } = State, _, [JobIDs]) ->
%     F = fun(JobID, Cts) -> lists:keydelete(JobID, 1, Cts) end,
%     NewC = lists:foldl(F, C, JobIDs),
%     NewG = lists:foldl(F, G, JobIDs),
%     State#state { contents = NewC, gotten = NewG }.
% 
% fastack_post(_S, [JobIDs], Res) ->
%     eq(Res, {ok, length(lists:usort(JobIDs))}).

%% SETUP/CLEANUP
%% -----------------------------------------------------------------------
setup() ->
    exec:start([]),
    {ok, _, Server1} = exec:run("/home/jlouis/Store/P/disque/src/disque-server --port 7711", []),
    timer:sleep(400),
    {ok, Pid1} = disque:start_link("127.0.0.1", 7711),
    register(disque_conn_1, Pid1),
    unlink(Pid1),
    {ok, Pid2} = disque:start_link("127.0.0.1", 7711),
    register(disque_conn_2, Pid2),
    unlink(Pid2),
    {ok, TracerPid} = tracer:start_link(),
    unlink(TracerPid),
    #{
      disque_conn_1 => Pid1,
      disque_conn_2 => Pid2,
      tracer => TracerPid,
      disque_server_1 => Server1
    }.

teardown(#{
	disque_conn_1 := Pid1,
	disque_conn_2 := Pid2,
	tracer := TracerPid,
	disque_server_1 := Server1 }) ->
    true = exit(Pid1, kill),
    true = exit(Pid2, kill),
    true = exit(TracerPid, kill),
    ok = exec:stop(Server1),
    ok.

clean_state(C, #state { contents = Cs, gotten = Gs }) ->
    Elems = [ID || {ID, _} <- Cs ++ Gs],
    Len = length(Elems),
    {ok, Len} = disque:ackjob(C, Elems),
    [tracer:record({ackjob, E}) || E <- Elems],
    ok.

empty_q(C, QName) ->
    {ok, Len} = disque:qlen(C, ?Q),
    empty_q(C, QName, Len).
    
empty_q(_C, _QName, 0) -> ok;
empty_q(C, QName, Len) when Len > 0 ->
    case disque:getjob(C, [QName], #{ count => Len }) of
        {ok, []} -> ok;
        {ok, Jobs} ->
            {ok, Len} = disque:ackjob(C, job_ids(Jobs)),
            empty_q(C, QName)
    end.

job_ids([]) -> [];
job_ids([[?Q, ID, _Job] | Js]) -> [ID | job_ids(Js)].

%% PROPERTY SECTION
%% -----------------------------------------------------------------------
weight(_S, qlen) -> 30;
weight(_S, qpeek) -> 30;
weight(_S, addjob) -> 100;
weight(_S, ackjob) -> 100;
weight(#state { contents = [] }, getjob) -> 3;
weight(_S, _) -> 100.

prop_disque() ->
  ?SETUP(fun() -> State = setup(), fun() -> teardown(State) end end,
  ?FORALL(Attempts, ?SHRINK(1, [20]),
  ?FORALL(Cleaner, conn(),
  ?FORALL(Cmds, commands(?MODULE),
    ?SOMETIMES(1, ?ALWAYS(Attempts, 
      begin
          empty_q(disque_conn_1, ?Q),
          empty_q(disque_conn_2, ?Q),
          ok = tracer:start_recording(),
          {H,S,R} = run_commands(?MODULE, Cmds),
          ok = clean_state(Cleaner, S),
          Trace = lists:sort(tracer:stop_recording()),
          %% FinalValue = lists:max([T || {T, _E} <- Trace ++ [{0, dummy}]]),
          Events = eqc_temporal:from_timed_list(Trace),
          LiveJobs = stateful(
              fun({addjob, Job}) -> [{job, Job}] end,
              fun({job, Job}, {ackjob, Job}) -> [] end,
              Events),
          LivePastEnd = all_future(LiveJobs),
          %% io:format("~nTrace:~n~p~n~p~n~p~n", [Trace, LiveJobs, LivePastEnd]),
          pretty_commands(?MODULE, Cmds, {H,S,R},
            aggregate(command_names(Cmds),
            measure(length, length(Cmds),
              conjunction([
                {postcondition, R == ok},
                {temporal, eqc_temporal:is_false(LivePastEnd)}
              ]))))
      end)))))).

%% BUGS FOUND
%% -----------------------------------------------------------------------

%% None at the moment.