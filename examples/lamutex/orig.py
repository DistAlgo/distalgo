import dpy
PatternExpr_2 = dpy.pat.PatternElement(dpy.pat.TupleVar, [dpy.pat.PatternElement(dpy.pat.ConstantVar, 'Reply'), dpy.pat.PatternElement(dpy.pat.FreeVar, 'c3')])
PatternExpr_3 = dpy.pat.PatternElement(dpy.pat.BoundVar, 'p3')
PatternExpr_4 = dpy.pat.PatternElement(dpy.pat.TupleVar, [dpy.pat.PatternElement(dpy.pat.ConstantVar, 'Request'), dpy.pat.PatternElement(dpy.pat.FreeVar, 'reqts')])
PatternExpr_5 = dpy.pat.PatternElement(dpy.pat.FreeVar, 'source')
PatternExpr_6 = dpy.pat.PatternElement(dpy.pat.TupleVar, [dpy.pat.PatternElement(dpy.pat.ConstantVar, 'Release'), dpy.pat.PatternElement(dpy.pat.FreeVar, 'time')])
PatternExpr_7 = dpy.pat.PatternElement(dpy.pat.FreeVar, 'source')
PatternExpr_9 = dpy.pat.PatternElement(dpy.pat.TupleVar, [dpy.pat.PatternElement(dpy.pat.ConstantVar, 'Done')])
PatternExpr_10 = dpy.pat.PatternElement(dpy.pat.BoundVar, 'p')
PatternExpr_8 = dpy.pat.PatternElement(dpy.pat.FreeVar, 'p')
PatternExpr_0 = dpy.pat.PatternElement(dpy.pat.TupleVar, [dpy.pat.PatternElement(dpy.pat.FreeVar, 'c2'), dpy.pat.PatternElement(dpy.pat.FreeVar, 'p2')])
PatternExpr_1 = dpy.pat.PatternElement(dpy.pat.FreeVar, 'p3')


class P(dpy.DistProcess):

    def __init__(self, parent, initq, channel, log):
        super().__init__(parent, initq, channel, log)
        self._events = [dpy.pat.EventPattern(dpy.pat.ReceivedEvent, 'ReceivedEvent_0', PatternExpr_2, sources=[PatternExpr_3], destinations=None, timestamps=None, record_history=True, handlers=[]), dpy.pat.EventPattern(dpy.pat.ReceivedEvent, 'ReceivedEvent_1', PatternExpr_4, sources=[PatternExpr_5], destinations=None, timestamps=None, record_history=False, handlers=[self.event_handler_0]), dpy.pat.EventPattern(dpy.pat.ReceivedEvent, 'ReceivedEvent_2', PatternExpr_6, sources=[PatternExpr_7], destinations=None, timestamps=None, record_history=False, handlers=[self.event_handler_1]), dpy.pat.EventPattern(dpy.pat.ReceivedEvent, 'ReceivedEvent_3', PatternExpr_9, sources=[PatternExpr_10], destinations=None, timestamps=None, record_history=True, handlers=[])]
        self.ReceivedEvent_0 = []
        self.ReceivedEvent_3 = []

    def setup(self, ps, n, timemax):
        self.timemax = timemax
        self.n = n
        self.ps = ps
        self.q = set()

    def main(self):

        def anounce():
            self.output('In cs!')
        for i in range(self.n):
            self.cs(anounce)
        self._send(('Done',), self.ps)
        p = None

        def UniversalOpExpr_3():
            nonlocal p
            for (p,) in PatternExpr_8.filter(self.ps, ('p',)):
                if (not set(self._events[3].filter(self.ReceivedEvent_3, p=p))):
                    return False
            return True
        while True:
            super()._label('_st_label_33', block=True)
            if UniversalOpExpr_3():
                break
        self.output('Terminating...')

    def cs(self, task):
        'To enter cs, enque and send request to all, then await replies from all\n        '
        super()._label('start', block=False)
        reqc = self.logical_clock()
        self.q.add((reqc, self._id))
        self._send(('Request', reqc), self.ps)
        super()._label('sync', block=False)
        p2 = c2 = None

        def UniversalOpExpr_0():
            nonlocal p2, c2
            for (p2, c2) in PatternExpr_0.filter(self.q, ('p2', 'c2')):
                if (not ((reqc, self._id) <= (c2, p2))):
                    return False
            return True
        p3 = None

        def UniversalOpExpr_1():
            nonlocal p3
            for (p3,) in PatternExpr_1.filter(self.ps, ('p3',)):
                c3 = None

                def ExistentialOpExpr_2():
                    nonlocal c3
                    for (c3,) in self._events[0].filter(self.ReceivedEvent_0, ('c3',), p3=p3):
                        if (c3 > reqc):
                            return True
                    return False
                if (not ExistentialOpExpr_2()):
                    return False
            return True
        self._timer_start()
        while True:
            super()._label('sync', block=True, timeout=self.timemax)
            if (UniversalOpExpr_0() and UniversalOpExpr_1()):
                self.output('witness: c2:%r p2:%r p3:%r' % ((c2, p2, p3)))
                task()
                super()._label('release', block=False)
                self.q.remove((reqc, self._id))
                self._send(('Release', reqc), self.ps)
                self.output('release cs')
                break
            if self._timer_expired:
                super()._label('release', block=False)
                self.q.remove((reqc, self._id))
                self._send(('Release', reqc), self.ps)
                self.output('timed out!')
                break
        self._timer_end()

    def event_handler_0(self, reqts, source):
        'When receiving requests from others, enque and reply\n        '
        self.q.add((reqts, source))
        self._send(('Reply', 
        self.logical_clock()), source)
    event_handler_0._labels = None
    event_handler_0._notlabels = None

    def event_handler_1(self, time, source):
        'When receiving release from others, deque'
        if ((time, source) in self.q):
            self.q.remove((time, source))
    event_handler_1._labels = None
    event_handler_1._notlabels = None
import sys

def main():
    nprocs = int(sys.argv[1]) if (len(sys.argv) > 1) else 10
    nrounds = int(sys.argv[2]) if (len(sys.argv) > 2) else 1
    timeout = int(sys.argv[3]) if (len(sys.argv) > 3) else 10
    dpy.api.use_channel('tcp')
    ps = dpy.api.createprocs(P, nprocs)
    for p in ps:
        dpy.api.setupprocs({p}, [ps - ({p}), nrounds, timeout])
    dpy.api.startprocs(ps)
if (__name__ == '__main__'):
    dpy.init(config)
