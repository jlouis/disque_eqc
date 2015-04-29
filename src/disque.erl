-module(disque).

-export([start_link/0]).

-export([
	ackjob/2,
	addjob/3, addjob/4,
	fastack/2,
	getjob/2, getjob/3,
	qlen/2,
	qpeek/3
]).

start_link() ->
    eredis:start_link("127.0.0.1", 7711).

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

qlen(Pid, Q) when is_pid(Pid) ->
    case eredis:q(Pid, ["QLEN", Q])of
      {ok, LenBin} -> {ok, binary_to_integer(LenBin)};
      Otherwise -> Otherwise
    end.

qpeek(Pid, Q, Count) when is_pid(Pid), is_integer(Count) ->
    eredis:q(Pid, ["QPEEK", Q, Count]).
