from collections import defaultdict
import pickle
import uuid
import copy
import logging

from .basic import implements, uses, trigger, ABC
from .links import EliminateDuplicates
from .failure_detector import ExcludeOnTimeout

log = logging.getLogger(__name__)


@implements('BestEffortBroadcast')
@uses('PerfectPointToPointLinks', EliminateDuplicates, 'pl')
class BasicBroadcast(ABC):
    """
    algo 3.1
    validity: if a correct process broadcasts a message m,
    then every correct process eventually delivers m.
    """
    def upon_Init(self):
        pass

    def upon_Broadcast(self, m):
        for p in self.peers:
            msg = {
                'mid': uuid.uuid4(),
                'data': m,
            }
            trigger(self.pl, 'Send', p, msg)
        trigger(self, 'Deliver', self.addr, msg)

    def upon_Deliver(self, q, m):
        trigger(self.upper, 'Deliver', q, m['data'])


@implements('ReliableBroadcast')
@uses('BestEffortBroadcast', BasicBroadcast, 'beb')
@uses('PerfectFailureDetector', ExcludeOnTimeout, 'p')
class LazyReliableBroadcast(ABC):
    """
    rely on the completeness property of the failure detector to ensure the
    broadcast agreement. If the failure detector does not ensure completeness
    then the processes might omit to relay messages that they should be
    relaying (e.g., messages broadcast by processes that crashed), and hence
    might violate agreement.
    """
    def upon_Init(self):
        self.correct = set(self.peers)
        self.correct.add(self.addr)
        self.from_ = defaultdict(set)

    def upon_Broadcast(self, m):
        trigger(self.beb, 'Broadcast', {
            'origin': self.addr,
            'data': m,
            })

    def upon_Deliver(self, q, m):
        origin, data = m['origin'], m['data']
        sdata = pickle.dumps(data)
        if sdata not in self.from_[origin]:
            trigger(self.upper, 'Deliver', origin, data)
            self.from_[origin].add(sdata)
            if origin not in self.correct:
                trigger(self.beb, 'Broadcast', m)

    def upon_Crash(self, p):
        self.correct.remove(p)
        for sdata in copy.copy(self.from_[p]):
            trigger(self.beb, 'Broadcast', {
                'origin': p,
                'data': pickle.loads(sdata),
                })


@implements('ReliableBroadcast')
@uses('BestEffortBroadcast', BasicBroadcast, 'beb')
class EagerReliableBroadcast(ABC):
    """
    algo 3.3 eager reliable broadcast

    A process that broadcast a message might deliver it and then crash, before
    the best-effort broadcast abstraction can even deliver the message to any
    other process. This scenario may occur in both reliable broadcast algos (
    eager and lazy). There are cases where such behavior causes problems
    because even a process that delivers a message and later crashes may bring
    the application into a inconsistent state.
    """
    def upon_Init(self):
        self.delivered = set()

    def upon_Broadcast(self, msg):
        self.beb.trigger('Broadcast', msg=msg)

    def upon_Deliver(self, q, m):
        if m not in self.delivered:
            self.delivered.add(m)
            trigger(self.upper, 'Deliver', q, m)
            trigger(self.beb, 'Broadcast', m)


@implements('UniformReliableBroadcast')
@uses('BestEffortBroadcast', BasicBroadcast, 'beb')
@uses('PerfectFailureDetector', ExcludeOnTimeout, 'p')
class AllAckUniformReliableBroadcast(ABC):
    """
    algo 3.4: All-Ack Uniform Reliable Broadcast

    uniform agreement: if a message m is delivered by some process
    (whether correct or faulty), then m is eventually delivered by every
    correct process.

    It's not correct if the failure detector is not prefect. Uniform
    agreement would be violated if accuracy is not satisfied and validity
    would be violated if completeness is not satisfied.
    """
    def upon_Init(self):
        self.delivered = set()
        self.pending = set()
        self.correct = set(self.peers)
        self.correct.add(self.addr)
        self.ack = defaultdict(set)

    def upon_Broadcast(self, msg):
        self.pending.add((self, msg))
        trigger(self.beb, 'Broadcast', {
            'origin': self.addr,
            'payload': msg,
            })

    def upon_Deliver(self, q, m):
        origin = m['origin']
        payload = m['payload']
        self.ack[payload].add(q)
        self.check_deliver()
        if (origin, payload) not in self.pending:
            self.pending.add((origin, payload))
            trigger(self.beb, 'Broadcast', m)

    def upon_Crash(self, peer):
        self.correct.remove(peer)

    def can_deliver(self, msg):
        return self.correct.issubset(self.ack[msg])

    def check_deliver(self):
        for origin, payload in self.pending:
            if payload not in self.delivered and self.can_deliver(payload):
                self.delivered.add(payload)
                self.upperlayer.trigger('Deliver', msg=payload)


@implements('UniformReliableBroadcast')
@uses('BestEffortBroadcast', BasicBroadcast, 'beb')
class MajorityAckUniformReliableBroadcast(ABC):
    """
    algo 3.5: Majority-Ack Uniform Reliable Broadcast
    """
    def upon_Init(self):
        self.delivered = set()
        self.pending = set()
        self.correct = set(self.peers)
        self.ack = defaultdict(set)
        self.M = len(self.members) / 2

    def upon_Broadcast(self, msg):
        newmsg = dict(origin=self.addr, payload=msg, through=self.addr)
        self.pending.add(newmsg)
        trigger(self.beb, 'Broadcast', newmsg)

    def upon_Deliver(self, msg, peer):
        origin = msg['origin']
        payload = msg['payload']
        self.ack[payload].add(peer)
        self.check_deliver()
        if msg not in self.pending:
            self.pending.add(msg)
            newmsg = dict(origin=origin, payload=payload, through=self.addr)
            trigger(self.beb, 'Broadcast', newmsg)

    def can_deliver(self, msg):
        return len(self.ack[msg]) > self.M

    def check_deliver(self):
        for msg in self.pending:
            origin = msg['origin']
            payload = msg['payload']
            if payload not in self.delivered and self.can_deliver(payload):
                self.delivered.add(payload)
                self.upperlayer.trigger('Deliver', msg=payload, peer=origin)
