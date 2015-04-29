-module(tracer).
-behaviour(gen_server).

-export([start_link/0]).
-export([start_recording/0, record/1, stop_recording/0, stop_recording/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    start_time,
    events
}).

%% API
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_recording() ->
    gen_server:call(?MODULE, start_recording).
    
stop_recording() ->
    stop_recording(micro_seconds).
    
stop_recording(Resolution) ->
    gen_server:call(?MODULE, {stop_recording, Resolution}).

record(Event) ->
    TS = erlang:monotonic_time(),
    gen_server:cast(?MODULE, {event, {TS, Event}}).

%% CALLBACKS
init([]) ->
    #state { start_time = erlang:monotonic_time(), events = [] }.

handle_call(start_recording, _From, State) ->
    TS = erlang:monotonic_time(),
    {reply, ok, State#state { start_time = TS, events = [] }};
handle_call({stop_recording, Resolution}, _From, #state { start_time = Start, events = Es} = State) ->
    F = fun({TS, Event}) -> {erlang:convert_time_unit(TS - Start, native, Resolution), Event} end,
    Output = lists:map(F, Es),
    {reply, Output, State}.
    
handle_cast({event, E}, #state { events = Es } = State) ->
    {noreply, State#state { events = [E | Es] }}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
    
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
