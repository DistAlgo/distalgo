import sys
config(channel is fifo, clock is lamport)

class P(process):
    def setup(s:set, nrequests:int):  # s is set of all other processes
        self.q = set()

    def mutex(task):
        -- request
        c = logical_clock()
        send(('request', c, self), to= s)
        q.add(('request', c, self))
        await(each(('request', c2, p) in q,
                   has= (c2, p)==(c, self) or (c, self) < (c2, p)) and
              each(p in s, has= some(received(('ack', c2, _p)), has= c2 > c)))
        -- critical_section
        task()
        -- release
        q.remove(('request', c, self))
        send(('release', logical_clock(), self), to= s)

    def receive(msg= ('request', c2, p)):
        q.add(('request', c2, p))
        send(('ack', logical_clock(), self), to= p)

    def receive(msg= ('release', _, p)):
#        q.remove(('request', _, p))  # pattern matching needed for _
#        q.remove(anyof(setof(('request', c, p), ('request', c, _p) in q)))
        for x in setof(('request', c, p), ('request', c, _p) in q):
            q.remove(x)
            break
#        for ('request', c, _p) in q: q.remove('request', c, p); break
#        for (tag, c, p2) in q:
#            if tag == 'request' and p2 == p:
#                q.remove((tag, c, p2)); break

    def run():
        def task():
            output('in cs')
        for i in range(nrequests):
            mutex(task)

        send(('done', self), to= parent())
        await(received(('done',), from_=parent()))
        output('terminating')

def main():
    nprocs = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    nrequests = int(sys.argv[2]) if len(sys.argv) > 2 else 1

    ps = new(P, num=nprocs)
    for p in ps: setup(p, (ps-{p}, nrequests))
    start(ps)
    await(each(p in ps, has=received(('done', p))))
    send(('done',), to=ps)

# This is an executable specification of the algorithm described in
# Lamport, L. (1978). "Time, clocks, and the ordering of events in a
# distributed system".  Communications of the ACM, 21(7):558-565.

# This code includes setup and termination for serving a given number of
# requests per process.

# All labels are not needed,
# leaving 14 or 15 lines total for the algorithm body and message handlers.
