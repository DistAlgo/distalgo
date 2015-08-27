%% Copyright (c) 2010-2015 Bo Lin
%% Copyright (c) 2010-2015 Yanhong Annie Liu
%% Copyright (c) 2010-2015 Stony Brook University
%% Copyright (c) 2010-2015 The Research Foundation of SUNY

%% Permission is hereby granted, free of charge, to any person
%% obtaining a copy of this software and associated documentation files
%% (the "Software"), to deal in the Software without restriction,
%% including without limitation the rights to use, copy, modify, merge,
%% publish, distribute, sublicense, and/or sell copies of the Software,
%% and to permit persons to whom the Software is furnished to do so,
%% subject to the following conditions:

%% The above copyright notice and this permission notice shall be
%% included in all copies or substantial portions of the Software.

%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
%% MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
%% NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
%% LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
%% OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
%% WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

-module(lamutex).
-export([run/2, start/1, start/0, run_processes/2]).
-export([site1/2, site/2, comm/5, broadcast_tracker/2]).

%% ------------------------------
%% Command line entry point
%% ------------------------------
start() ->
    run(10, 10).

start([NPeers]) ->
    run(list_to_integer(NPeers), 5);
start([NPeers, RTime]) ->
    run(list_to_integer(NPeers), list_to_integer(RTime)).

%% ------------------------------
%% Program entry point
%% ------------------------------
run(NumPeers, RunTime)
  when is_integer(NumPeers), is_integer(RunTime) ->
    io:format("Starting Comm processes...\n"),
    Comms = create_comm_processes(NumPeers),
    %% Register the broadcast process so Comms would know where to send
    %% broadcast requests:
    register(tracker, spawn(lamutex, broadcast_tracker, [Comms, self()])),
    io:format("Starting Site processes... ~w~n \n", [length(Comms)]),
    Sites = create_site_processes(Comms, RunTime),
    statistics(runtime),
    {WallclockTime, _Result} = timer:tc(lamutex, run_processes, [Sites, NumPeers]),
    {_, CpuTime} = statistics(runtime),
    dump_perf_data(WallclockTime, NumPeers, CpuTime),
    halt(0).

run_processes(Sites, NumPeers) ->
    do_broadcast(Sites, start),
    wait_all_done(NumPeers).

wait_all_done(0) ->
    ok;
wait_all_done(NumLeft)
  when NumLeft > 0 ->
    receive
        {finished, _Id} ->
            wait_all_done(NumLeft-1)
    end.

get_memory_stats() ->
    {ok, StatsFile} = file:open(filename:join(['/proc', os:getpid(), 'status']),
                                [read]),
    {ok, Matcher} = re:compile("^VmHWM:[^0-9]*\([0-9]+\) kB"),
    case first_matching_line(Matcher, StatsFile) of
        {vmhwm, Value} -> Value;
        notfound -> "0"
    end.

first_matching_line(Matcher, Device) ->
    case io:get_line(Device, "") of
        eof ->
            file:close(Device),
            notfound;
        Line ->
            case re:run(Line, Matcher, [{capture, all_but_first, list}]) of
                {match, [Cap]} ->
                    file:close(Device),
                    {vmhwm, Cap};
                nomatch ->
                    first_matching_line(Matcher, Device)
            end
    end.

dump_perf_data(WallclockTime, NumPeers, CpuTime) ->
    io:format("###OUTPUT: {\"Total_memory\": ~s, "
              "\"Wallclock_time\": ~f, \"Total_processes\": ~w, "
              "\"Total_process_time\": ~f, \"Total_user_time\": ~f}~n",
              [get_memory_stats(), WallclockTime/1000000,
               NumPeers, CpuTime/1000, CpuTime/1000]).

%% --------------------
%% This helper process handles all broadcast requests. It's a small hackery to
%% get around the circular dependency of letting every peer process know about
%% all other peers before they are actually created. Maybe there's a better
%% way to do this...
%% --------------------
broadcast_tracker(Targets, ParentId) ->
    receive
        {broadcast, Message} ->
            do_broadcast(Targets, Message),
            broadcast_tracker(Targets, ParentId);
        {finished, Time} ->
            ParentId ! {finished, Time},
            broadcast_tracker(Targets, ParentId)
    end.
do_broadcast([], _) ->
    ok;
do_broadcast([To|Rest], Message) ->
    To ! Message,
    do_broadcast(Rest, Message).


%% --------------------
%% Create all the "Comm" processes.
%% --------------------
create_comm_processes(N) ->
    [spawn(lamutex, comm, [Id, N, [], 0, ignore]) || Id <- lists:seq(1, N)].

%% --------------------
%% Create all the "Site" processes.
%% --------------------
create_site_processes(CommList, Rounds) ->
    [spawn(lamutex, site1, [Id, Rounds]) || Id <- CommList].

%% --------------------
%% Site entry point.
%% --------------------
site1(CommId, Rounds) ->
    io:format("Site ~w starting...\n", [CommId]),
    receive
        start -> ok
    end,
    site(CommId, Rounds),
    io:format("Process ~w done. \n", [CommId]),
    tracker ! {finished, CommId}.

%% --------------------
%% The "Site" process is now extremely simple and straight forward: 1. do
%% non-critical stuff; 2. try enter critical section by notifying our "Comm"
%% and waiting for the "goahead" signal; 3. do critical work; 4. leave CS. It
%% maintains no data apart from the corresponding "Comm" PID that handles CS
%% requests on its behalf.
%% --------------------
site(_CommId, 0) ->
    ok;
site(CommId, Rounds) ->
    %% Non CS:
    enter_critical_section(CommId),
    %% In CS:
    io:format("In critical section: ~w \n", [CommId]),
    %procrastinate(),
    %% Leave CS:
    io:format("Leaving critical section: ~w \n", [CommId]),
    leave_critical_section(CommId),
    site(CommId, Rounds-1).

%% --------------------
%% All data structures pertaining to critical section management is now moved
%% into the "Comm" process.
%% --------------------
comm(SiteId, NPeers, ReqQueue, Clock, AckSet) ->
    %% First check whether our Site is ready to enter CS:
    Result = check_can_enter_cs(self(), ReqQueue, AckSet, NPeers),
    if
        Result ->
            SiteId ! goahead,
            comm(SiteId, NPeers, ReqQueue, Clock, ignore);
        true ->
            ok
    end,
    %% Then handle the next message:
    receive 
        {request, From, Clk} ->
            From ! {ack, self(), Clock},        % Send ack reply
            comm(SiteId, NPeers,
                 lists:keystore(From, 2, ReqQueue, {Clk, From}), 
                 max(Clock, Clk) + 1,
                 AckSet);

        {ack, From, _Clk} ->
            %% 'ignore' is used as a minor optimiazation: If our Site is not
            %% trying to enter CS, then we can safely ignore all ack messages:
            if
                AckSet == ignore ->
                    comm(SiteId, NPeers, ReqQueue, Clock, AckSet);
                true ->
                    comm(SiteId, NPeers, ReqQueue, Clock,
                         sets:add_element(From, AckSet))
            end;

        {release, From, _Clk} ->
            comm(SiteId, NPeers,
                 lists:keydelete(From, 2, ReqQueue), Clock, AckSet);

        %% These messages are used to communicate with our Site:
        {enter, Id} -> % Our own site wishes to enter
            broadcast({request, self(), Clock}),
            comm(Id, NPeers, ReqQueue, Clock, sets:new());
        {release, Id} when Id == SiteId-> % Our own site is done
            broadcast({release, self(), Clock}),
            comm(Id, NPeers, ReqQueue, Clock, ignore)
    end.

%% ==================================================
%% Helpers
%% ==================================================

%% Just send the message to the broadcast tracker process:
broadcast(Message) ->
    tracker ! {broadcast, Message}.

%% CS condition check. If successful send goahead back to site:
check_can_enter_cs(CommId, ReqQueue, AckSet, NumPeers) ->
    AckSet /= ignore andalso
        sets:size(AckSet) == NumPeers andalso
        length(ReqQueue) > 0 andalso
        element(2, lists:min(ReqQueue)) == CommId.

%% Time waster:
procrastinate() ->
    receive
    after random:uniform(3) * 1000 ->
            ok
    end.

enter_critical_section(CommId) ->
    CommId ! {enter, self()},
    receive
        goahead ->
            ok
    end.

leave_critical_section(CommId) ->
    CommId ! {release, self()}.
