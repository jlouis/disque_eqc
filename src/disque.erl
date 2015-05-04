-module(disque).

-export([start_link/0, start_link/2]).

-export([
	ackjob/2,
	addjob/3, addjob/4,
	cluster/2,
	fastack/2,
	getjob/2, getjob/3,
	qlen/2,
	qpeek/3,
	show/2
]).

start_link() -> start_link("127.0.0.1", 7711).
    
start_link(Host, Port) ->
    eredis:start_link(Host, Port).

%% CLUSTER OPERATIONS
%% -----------------------------------------------------------------------

cluster(Pid, {meet, Host, Port}) ->
    eredis:q(Pid, ["CLUSTER", "MEET", Host, Port]).

%% ADDJOB
%% -----------------------------------------------------------------------

ackjob(Pid, Jobs) ->
    case eredis:q(Pid, ["ACKJOB" | Jobs]) of
        {ok, NBin} -> {ok, binary_to_integer(NBin)};
        Otherwise -> Otherwise
    end.

%% FASTACK
%% -----------------------------------------------------------------------
fastack(Pid, Jobs) ->
   case eredis:q(Pid, ["FASTACK" | Jobs]) of
       {ok, NBin} -> {ok, binary_to_integer(NBin)};
       Otherwise -> Otherwise
   end.

%% ADDJOB
%% -----------------------------------------------------------------------

addjob(Pid, Q, J) ->
    addjob(Pid, Q, J, #{ timeout => 5000 }).
    
addjob(Pid, Q, J, #{ timeout := Timeout } = Opts) ->
    eredis:q(Pid, ["ADDJOB", Q, J, Timeout | addjob_opts(Opts)]).
    
addjob_opts(Opts) -> addjob_opts_replicate(Opts).

addjob_opts_replicate(#{ replicate := Replicate } = Opts) ->
    ["REPLICATE", Replicate | addjob_opts_delay(Opts)];
addjob_opts_replicate(Opts) ->
    addjob_opts_delay(Opts).

addjob_opts_delay(#{ delay := Delay } = Opts) ->
    ["DELAY", Delay | addjob_opts_retry(Opts)];
addjob_opts_delay(Opts) ->
   addjob_opts_retry(Opts).

addjob_opts_retry(#{ retry := Retry } = Opts) ->
    ["RETRY", Retry | addjob_opts_ttl(Opts)];
addjob_opts_retry(Opts) ->
    addjob_opts_ttl(Opts).

addjob_opts_ttl(#{ ttl := TTL } = Opts) ->
    ["TTL", TTL | addjob_opts_maxlen(Opts)];
addjob_opts_ttl(Opts) ->
    addjob_opts_maxlen(Opts).

addjob_opts_maxlen(#{ maxlen := Count } = Opts) ->
    ["MAXLEN", Count | addjob_opts_async(Opts)];
addjob_opts_maxlen(Opts) ->
    addjob_opts_async(Opts).

addjob_opts_async(#{ async := true }) -> ["ASYNC"];
addjob_opts_async(#{ async := false }) -> [];
addjob_opts_async(_) -> [].

%% GETJOB
%% -----------------------------------------------------------------------

getjob(P, Qs) when is_pid(P), is_list(Qs) -> getjob(P, Qs, #{}).

getjob(P, Qs, Opts) ->
    eredis:q(P, ["GETJOB" | getjob_opts(Opts, ["FROM" | Qs])]).

getjob_opts(Opts, Tail) -> getjob_opts_timeout(Opts, Tail).

getjob_opts_timeout(#{ timeout := Timeout } = Opts, Tail) ->
    ["TIMEOUT", Timeout | getjob_opts_count(Opts, Tail)];
getjob_opts_timeout(Opts, Tail) ->
    getjob_opts_count(Opts, Tail).
    
getjob_opts_count(#{ count := Count }, Tail) ->
    ["COUNT", Count | Tail];
getjob_opts_count(_, Tail) -> Tail.

%% QLEN
%% -----------------------------------------------------------------------

qlen(Pid, Q) when is_pid(Pid); is_atom(Pid) ->
    case eredis:q(Pid, ["QLEN", Q])of
      {ok, LenBin} -> {ok, binary_to_integer(LenBin)};
      Otherwise -> Otherwise
    end.

qpeek(Pid, Q, Count) when is_pid(Pid) orelse is_atom(Pid), is_integer(Count) ->
    eredis:q(Pid, ["QPEEK", Q, Count]).

%% SHOW
%% -----------------------------------------------------------------------

show(L, ID) when
	is_pid(L) orelse is_atom(L),
	is_binary(ID) ->
    case eredis:q(L, ["SHOW", ID]) of
       {ok, Pairs} -> {ok, show_map(Pairs)};
       {error, _} = Err -> Err
    end.

show_map(Pairs) ->
    maps:from_list(show_map_pair(Pairs)).
    
show_map_pair([]) -> [];
show_map_pair([<<"id">>, ID | Next]) -> [{id, ID} | show_map_pair(Next)];
show_map_pair([<<"queue">>, Q | Next]) -> [{queue, Q} | show_map_pair(Next)];
show_map_pair([<<"state">>, S | Next]) -> [{state, S} | show_map_pair(Next)];
show_map_pair([<<"repl">>, R | Next]) -> [{replication_factor, binary_to_integer(R)} | show_map_pair(Next)];
show_map_pair([<<"ttl">>, TTL | Next]) -> [{ttl, binary_to_integer(TTL)} | show_map_pair(Next)];
show_map_pair([<<"ctime">>, CT | Next]) ->
	[{ctime, binary_to_integer(CT)} | show_map_pair(Next)];
show_map_pair([<<"delay">>, D | Next]) -> [{delay, binary_to_integer(D)} | show_map_pair(Next)];
show_map_pair([<<"retry">>, R | Next]) -> [{retry, binary_to_integer(R)} | show_map_pair(Next)];
show_map_pair([<<"nodes-delivered">>, Ns | Next]) -> [{nodes_delivery, Ns} | show_map_pair(Next)];
show_map_pair([<<"nodes-confirmed">>, Ns | Next]) -> [{nodes_confirmed, Ns} | show_map_pair(Next)];
show_map_pair([<<"next-requeue-within">>, T | Next]) -> [{next_requeue_within, binary_to_integer(T)} | show_map_pair(Next)];
show_map_pair([<<"next-awake-within">>, T | Next]) -> [{next_awake_within, binary_to_integer(T)} | show_map_pair(Next)];
show_map_pair([<<"body">>, B | Next]) -> [{body, B} | show_map_pair(Next)].



