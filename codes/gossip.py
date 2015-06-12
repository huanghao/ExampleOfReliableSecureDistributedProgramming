import random
import pickle
import logging
import itertools
from collections import defaultdict

from .basic import implements, uses, trigger, start_timer, ABC
from .links import BasicLink

log = logging.getLogger(__name__)


@implements('ProbabilisticBroadcast')
@uses('FairLossPointToPointLinks', BasicLink, 'fll')
class EagerProbabilisticBroadcast(ABC):
    """
    Algo 3.9
    request Broadcast, indication Deliver

    - probabilistic validity: there is a positive value e such that when
      a correct process broadcasts a message m, the probability that every
      correct process eventually delivers m is at least 1-e
    """
    R = 2  # rounds
    K = 3  # fanout

    def upon_Init(self):
        self.delivered = set()

    def upon_Broadcast(self, m):
        self.delivered.add(pickle.dumps(m))
        trigger(self.upper, 'Deliver', self.addr, m)
        self.gossip({
            'origin': self.addr,
            'payload': m,
            'rounds': self.R,
            })

    def upon_Deliver(self, q, m):
        origin, payload, rounds = m['origin'], m['payload'], m['rounds']
        h = pickle.dumps(payload)
        if h not in self.delivered:
            self.delivered.add(h)
            trigger(self.upper, 'Deliver', origin, payload)
        if rounds > 1:
            m['rounds'] -= 1
            self.gossip(m)

    def gossip(self, m):
        for p in random.sample(self.peers, self.K):
            trigger(self.fll, 'Send', p, m)


@implements('ProbabilisticBroadcast')
@uses('FairLossPointToPointLinks', BasicLink, 'fll')
@uses('ProbabilisticBroadcast', EagerProbabilisticBroadcast, 'upb')
# an unreliable implementation
class LazyProbabilisticBroadcast(ABC):
    """
    algo 3.10 lazy probabilistic broadcast
    part 1, data dissemintion
    part 2, recovery

    in order to achieve reliable delivery with high probability. rely on
    epidemic push-style broadcast only in a first phase, until many processes
    are infected, and to switch to a pulling mechanism in a second phase
    afterward. Gossiping util, say, half of the processes are infected is
    efficient. The pulling phase serves a backup to inform the processes
    that missed the message in the first phase. The second phase uses again
    gossip, but only to disseminate messages about which processes have missed
    a message in the first phase.

    Practical algorithms based on this principle make a significant effort
    to optimize the number of processes that store copies of each broadcast
    message.
    """
    ALPHA = .5
    # ratio of message to store in order to support pulling phase.
    # garbage collection of the stored message copies is omitted
    # in the pseudo code for simplicity.

    DELTA = 5
    # with small probability, recovery will fail. In this case,
    # after a timeout with delay has expired, a process simply
    # jumps ahead and skips the missed messages, such that
    # subsequent messages from the same sender can be delivered.

    R = 2  # gossip rounds
    K = 3  # gossip fanout

    def upon_Init(self):
        self.next = defaultdict(int)
        self.lsn = itertools.count(0)
        self.pending = defaultdict(dict)
        self.stored = defaultdict(dict)

    def gossip(self, m):
        for p in random.sample(self.peers, self.K):
            trigger(self.fll, 'Send', p, m)

    def upon_Broadcast(self, m):
        trigger(self.upb, 'Broadcast', {
            'typ': 'message',
            'origin': self.addr,
            'payload': m,
            'sn': next(self.lsn),
            })

    def upon_Deliver(self, q, m):
        if m['typ'] == 'request':
            self.recovery(q, m)
        else:
            self.dissemination(q, m)

    def deliver_pending(self, origin):
        sn = self.next[origin]
        while sn in self.pending[origin]:
            m = self.pending[origin].pop(sn)
            trigger(self.upper, 'Deliver', m['origin'], m['payload'])
            sn += 1
        self.next[origin] = sn

    def dissemination(self, q, m):
        origin, payload, sn = m['origin'], m['payload'], m['sn']
        if random.random() < self.ALPHA:
            self.stored[(origin, sn)] = m

        if sn == self.next[origin]:
            self.next[origin] += 1
            if sn in self.pending[origin]:
                self.pending[origin].pop(sn)
            trigger(self.upper, 'Deliver', origin, payload)
            self.deliver_pending(origin)
        elif sn > self.next[origin]:
            self.pending[origin][sn] = m
            for missing in range(self.next[origin], sn):
                if missing not in self.pending[origin]:
                    self.gossip({
                        'typ': 'request',
                        'origin': origin,
                        'sn': missing,
                        'dest': self.addr,
                        'rounds': self.R,
                        })
            start_timer(self.DELTA, self.upon_Timout, origin, sn)

    def recovery(self, q, m):
        origin, sn, dest, rounds = m['origin'], m['sn'], m['dest'], m['rounds']
        if (origin, sn) in self.stored:
            trigger(self.fll, 'Send', dest, self.stored[(origin, sn)])
        elif rounds > 1:
            m['rounds'] -= 1
            self.gossip(m)

    def upon_Timout(self, origin, sn):
        if sn > self.next[origin]:
            self.next[origin] = sn
            log.info("%s skip %s'ssn to %s",
                     self.addr, origin, self.next[origin])
            self.deliver_pending(origin)
