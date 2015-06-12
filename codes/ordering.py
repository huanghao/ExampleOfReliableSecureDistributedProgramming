import itertools
import logging
from collections import defaultdict, OrderedDict

log = logging.getLogger(__name__)

from .basic import implements, uses, trigger, mhash, ABC
from .broadcast import LazyReliableBroadcast


@implements('FIFOReliableBroadcast')
@uses('ReliableBroadcast', LazyReliableBroadcast, 'rb')
class BroadcastWithSequenceNumber(ABC):
    """
    Algorithm 3.12

    FIFO delivery: if some process broadcasts message m1 before it broadcasts
    message m2, then no correct process delivers m2 unless it has already
    delivered m1.
    """
    def upon_Init(self):
        self.lsn = itertools.count(0)
        self.pending = defaultdict(dict)
        self.next = defaultdict(int)

    def upon_Broadcast(self, m):
        trigger(self.rb, 'Broadcast', {
            'sn': next(self.lsn),
            'origin': self.addr,
            'data': m,
            })

    def upon_Deliver(self, q, m):
        origin = m['origin']
        p = self.pending[origin]
        p[m['sn']] = m['data']
        while self.next[origin] in p:
            data = p.pop(self.next[origin])
            trigger(self.upper, 'Deliver', origin, data)
            self.next[origin] += 1


@implements('CausalOrderReliableBroadcast')
@uses('ReliableBroadcast', LazyReliableBroadcast, 'rb')
class NoWaitingCausalBroadcast(ABC):
    """
    Algorithm 3.13

    Causal delivery: for any message m1 that potentially caused a message m2,
    i.e., m1 -> m2, no process delivers m2 unless it has already delivered m1.
    """
    def upon_Init(self):
        self.delivered = set()
        self.past = OrderedDict()

    def upon_Broadcast(self, m):
        trigger(self.rb, 'Broadcast', {
            'past': self.past,
            'data': m,
            })
        self.past[(self.addr, mhash(m))] = m

    def upon_Deliver(self, q, m):
        mpast, data = m['past'], m['data']
        mh = mhash(data)
        if mh in self.delivered:
            return

        msg = []
        for (s, nh), n in mpast.items():
            if nh not in self.delivered:
                self.delivered.add(nh)
                msg.append((s, n))
                if (s, nh) not in self.past:
                    self.past[(s, nh)] = n
        for s, n in msg:
            trigger(self.upper, 'Deliver', s, n)

        self.delivered.add(mh)
        if (q, mh) not in self.past:
            self.past[(q, mh)] = data
        trigger(self.upper, 'Deliver', q, data)


@implements('CausalOrderReliableBroadcast')
@uses('ReliableBroadcast', LazyReliableBroadcast, 'rb')
class WaitingCausalBroadcast(ABC):
    """
    algo 3.15
    using vector clock
    """
    def upon_Init(self):
        members = sorted(list(self.peers) + [self.addr])
        self.rank = lambda p: members.index(p)
        self.v = [0] * len(members)
        self.lsn = itertools.count(0)
        self.pending = []

    def upon_Broadcast(self, m):
        w = self.v[:]
        w[self.rank(self.addr)] = next(self.lsn)
        trigger(self.rb, 'Broadcast', {
            'clock': tuple(w),
            'data': m,
            })

    def upon_Deliver(self, q, m):
        self.pending.append((m['clock'], m['data'], q))
        while 1:
            msg = []
            for i in range(len(self.pending)-1, -1, -1):
                w, data, q = self.pending[i]
                if w <= tuple(self.v):
                    msg.append(self.pending.pop(i))
                    self.v[self.rank(q)] += 1
            if not msg:
                break
            # in async en, deliver may call send msg lead to
            # breaking the previous for loop
            for w, data, q in msg:
                trigger(self.upper, 'Deliver', q, data)
