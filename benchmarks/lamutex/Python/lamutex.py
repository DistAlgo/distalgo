#!/usr/bin/env python3
import sys
import time
import json
import queue
import random
import resource
import threading
import multiprocessing
from heapq import heappush, heappop, heapify

BROADCAST = -1
LOG = -2

# ==============================
# The 'Site' class represents a peer in the network. Each 'Site' is run in a
# separate process
# ==============================
class Site(multiprocessing.Process):
    # ====================
    # Each 'Site' will have one 'Comm' thread, its sole purpose is to reply all
    # incoming requests:
    # ====================
    class Comm(threading.Thread):
        def __init__(self, pid, net, queue):
            threading.Thread.__init__(self)
            self.peerid = pid   # self's unique process id
            self.queue = queue  # Queue used to communicate with our main Site
                                # process
            self.net = net      # Interface to the "network" (which is a
                                # dictionary containing an 'in' queue and an
                                # 'out' queue.)
            self.daemon = True

        # Simple algorithm for Comm: Take a message from net 'in' and put it
        # into the queue for Site. If the message is a request we reply it:
        def run(self):
            while True:
                mtype, source, clock = receive(self.net)
                self.queue.put((mtype, source, clock))
                if (mtype == "request"):
                    send(self.net, source, ("ack", self.peerid, clock))

    def __init__(self, pid, net, iterations, peers):
        multiprocessing.Process.__init__(self)
        self.peerid = pid       # Our unique peer id
        self.net = net          # Interface to the "network" (which is a dict
                                # containing an in queue and an out queue.)
        self.peers = peers      # List of all processes, including self
        self.iterations = iterations      # Number of iterations to run
        self.thread_queue = queue.Queue() # Queue used to communicate with Comm
        self.acks = set()                 # The set of replies we've received
        self.reqs = []                    # Request priority queue, in clock order
        self.clock = 0                    # Our logical clock
        self.comm = Site.Comm(pid, net, self.thread_queue)
        self.running = False

    def run(self):
        self.comm.start()       # Start the "Comm" thread
        while not self.running:
            self.handle_message(True)

        rudata_start = resource.getrusage(resource.RUSAGE_SELF)
        count = 0
        while(self.running and count < self.iterations):
            # Non CS:
            self.label()        # labels mark the points where we can "break"
            self.work()
            self.label()
            self.enter_critical_section()
            print("Process %d(clock:%d) has entered critical section."%
                  (self.peerid,self.clock))
            self.label()
            self.work()
            self.label()
            print("Process %d is leaving critical section."%self.peerid)
            self.leave_critical_section()
            count += 1

        rudata_end = resource.getrusage(resource.RUSAGE_SELF)
        utime = rudata_end.ru_utime - rudata_start.ru_utime
        stime = rudata_end.ru_stime - rudata_start.ru_stime
        send(self.net, LOG, (utime, stime, rudata_end.ru_maxrss))
        while self.running:
            self.handle_message(True)

    # This simulates the controlled "label" mechanism. Currently we simply
    # handle one message on one label call:
    def label(self):
        self.handle_message(False)

    # Handles one message. 'block' indicates whether to blocking waiting for
    # next message to come in if the queue is currently empty:
    def handle_message(self, block):
        try:
            (mtype, source, clock) = self.thread_queue.get(block)
            if mtype == "request":
                # Put request on queue and update clock
                heappush(self.reqs, (clock, source))
                self.clock = max(self.clock, clock) + 1
            elif mtype == "ack":
                self.acks.add(source)
            elif mtype == "release":
                # Remove the sender from our request queue
                for (clk, peer) in self.reqs:
                    if peer == source:
                        self.reqs.remove((clk, peer))
                        # Note that we have to re-heapify after the remove:
                        heapify(self.reqs)
            elif mtype == "terminate":
                self.running = False
            elif mtype == "start":
                self.running = True
            else:
                raise RuntimeError("Unknown message type " + mtype)
        except queue.Empty:
            pass

    # We keep handling messages until the wait condition becomes true:
    def enter_critical_section(self):
        self.acks = set()
        send(self.net, BROADCAST, ("request", self.peerid, self.clock))
        while (len(self.reqs) == 0 or
               self.reqs[0][1] != self.peerid or
               len(self.acks) != len(self.peers)):
            self.handle_message(True)

    def leave_critical_section(self):
        send(self.net, BROADCAST, ("release", self.peerid, self.clock))

    # Simulate work, waste some random amount of time:
    def work(self):
#        time.sleep(random.randint(1, 5))
        return


# ==============================
# The "Simulator" class takes care of message routing between the peers:
# ==============================
class Simulator(threading.Thread):
    def __init__(self, sites, netq):
        threading.Thread.__init__(self)
        self.sites = sites
        self.netq = netq    # This is the shared 'out' queue of all the
                            # processes

        self.perf_usrtime = 0
        self.perf_systime = 0
        self.perf_memory = 0

        self.completed = 0

    def run(self):
        t1 = time.perf_counter()
        for s in self.sites:
            s.net['in'].put(('start', -1, -1))

        while self.completed < len(self.sites):
            message = self.netq.get(True)
            self.dispatch_message(message)

        t2 = time.perf_counter()
        for s in self.sites:
            s.net['in'].put(('terminate', -1, -1))

        jsondata = {'Wallclock_time': t2 - t1,
                    'Total_processes': self.completed,
                    'Total_process_time': self.perf_usrtime + self.perf_systime,
                    'Total_user_time': self.perf_usrtime,
                    'Total_memory': self.perf_memory}
        jsonoutput = json.dumps(jsondata)
        print("###OUTPUT: " + jsonoutput)

        for site in self.sites:
            site.join()

    def dispatch_message(self, message):
        (to, m) = message
        if to == BROADCAST:
             for site in self.sites:
                 site.net['in'].put(m)
        elif to == LOG:
            self.completed += 1

            utime, stime, maxrss = m
            self.perf_usrtime += utime
            self.perf_systime += stime
            self.perf_memory += maxrss

        else:
            self.sites[to].net['in'].put(m)

# Create nproc "Sites":
def create_sites(nproc, q, iterations):
    return [Site(id, {'out':q, 'in':multiprocessing.Queue()},
                 iterations, range(0, nproc))
            for id in range(0, nproc)]

# ==============================
# Main program entry point:
# ==============================
def main(nproc, iterations):
    queue = multiprocessing.Queue()
    print("Creating sites...")
    sites = create_sites(nproc, queue, iterations)
    print("Creating simulator...")
    sim = Simulator(sites, queue)
    print("Starting %d sites...\n"%nproc)
    for site in sites:
        site.start()
    print("Starting simulator...")
    sim.start()
    sim.join()

# Wrapper functions for message passing:
def send(net, to, message):
    net['out'].put((to, message))

def receive(net):
    return net['in'].get()

if __name__ == "__main__":
    nprocs = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    iterations = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    main(nprocs, iterations)
