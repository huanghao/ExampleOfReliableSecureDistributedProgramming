import logging
import itertools
import bisect
import pickle
from collections import defaultdict

from .basic import implements, uses, trigger, start_timer, Store

log = logging.getLogger(__name__)


@implements('FairLossPointToPointLinks')
class BasicLink:
    def __init__(self, name, upper, udp):
        self.name = name
        self.upper = upper
        self.sendto = udp.register(name, self)
        trigger(self, 'Init')

    def upon_Init(self):
        pass

    def upon_Send(self, p, m):
        self.sendto(m, p)

    def upon_Deliver(self, q, m):
        trigger(self.upper, 'Deliver', q, m)


@implements('StubbornPointToPointLinks')
@uses('FairLossPointToPointLinks')
class RetransmitForever:
    """
    Algorithm: 2.1
    """
    DELTA = 10

    def __init__(self, name, upper, udp):
        self.name = name
        self.upper = upper
        self.fll = BasicLink(self.name+'.fll', self, udp)
        trigger(self, 'Init')

    def upon_Init(self):
        self.sent = set()
        start_timer(self.DELTA, self.upon_Timeout)

    def upon_Timeout(self):
        for p, m in self.sent:
            trigger(self.fll, 'Send', p, pickle.loads(m))
        start_timer(self.DELTA, self.upon_Timeout)

    def upon_Send(self, p, m):
        trigger(self.fll, 'Send', p, m)
        self.sent.add((p, pickle.dumps(m)))

    def upon_Deliver(self, q, m):
        trigger(self.upper, 'Deliver', q, m)


@implements('PerfectPointToPointLinks')
@uses('StubbornPointToPointLinks')
class EliminateDuplicates:
    """
    Algorithm 2.2
    """
    def __init__(self, name, upper, udp):
        self.name = name
        self.upper = upper
        self.sl = RetransmitForever(self.name+'.sl', self, udp)
        trigger(self, 'Init')

    def upon_Init(self):
        self.delivered = set()

    def upon_Send(self, p, m):
        trigger(self.sl, 'Send', p, m)

    def upon_Deliver(self, q, m):
        h = hash(pickle.dumps(m))
        if h not in self.delivered:
            self.delivered.add(h)
            trigger(self.upper, 'Deliver', q, m)


@implements('LoggedPerfectPointToPointLinks')
@uses('StubbornPointToPointLinks')
class LogDelivered:
    """
    Algorithm 2.3
    """
    def __init__(self, upper, udp):
        self.upper = upper
        self.sl = RetransmitForever(self, udp)
        self.store = Store(self.pid)
        if self.store.exists():
            trigger(self, 'Recovery')
        else:
            trigger(self, 'Init')

    def upon_Init(self):
        self.delivered = set()
        self.store.store(self.delivered)

    def upon_Recovery(self):
        self.delivered = self.store.retrieve()

    def upon_Send(self, p, m):
        trigger(self.sl, 'Send', p, m)

    def upon_Deliver(self, q, m):
        log.debug('m: %s, delievered: %s', m, self.delivered)
        if m not in self.delivered:
            self.delivered.add(m)
            self.store.store(self.delivered)
            trigger(self.upper, 'Deliver', q, m)


@implements('')
class SequenceNumber:
    """
    Ex2.3: implements FIFO-order perfect point-to-point links
    """
    def __init__(self, upper, addr):
        self.set_upper_layer(upper)
        self.pl = EliminateDuplicates(self, addr)
        self.seq = itertools.count(0)
        self.next = defaultdict(int)
        self.buffer = defaultdict(list)

    def upon_Send(self, msg, peer):
        trigger(self.pl, 'Send', peer, {
            'seq': next(self.seq),
            'payload': msg,
            })

    def upon_Deliver(self, q, m):
        bisect.insort(self.buffer[q], (m['seq'], m))
        rm = 0
        for seq, msg in self.buffer[q]:
            if seq != self.next[q]:
                break
            self.next[q] += 1
            trigger(self.upper, 'Deliver', self.name, q, msg)
            rm += 1
        self.buffer[q] = self.buffer[q][rm:]
