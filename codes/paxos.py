"""
Pseudo code from course 6.824

--- Paxos Proposer ---
proposer(v):
while not decided:
    choose n, unique and higher than any n seen so far
    send prepare(n) to all servers including self
    if prepare_ok(n, na, va) from majority:
        v' = va with highest na; choose own v otherwise
        send accept(n, v') to all
        if accept_ok(n) from majority:
            send decided(v') to all

--- Paxos Acceptor ---
acceptor state on each node (persistent):
    np     --- highest prepare seen
    na, va --- highest accept seen

acceptor's prepare(n) handler:
    if n > np
        np = n
        reply prepare_ok(n, na, va)
    else
        reply prepare_reject

acceptor's accept(n, v) handler:
    if n >= np
        np = n
        na = n
        va = v
        reply accept_ok(n)
   else
        reply accept_reject
"""
import logging
from collections import defaultdict

from .basic import implements, uses, trigger, ABC

log = logging.getLogger(__name__)


@implements('Consensus')
@uses('BestEffortBroadcast', 'beb')
@uses('FairLossPointToPointLinks', 'fll')
class Synod(ABC):
    """
    Only proposer knows which value has been chosen
    If other servers want to know, must execute Paxos with their own proposal
    """
    def upon_Init(self):
        # for proposer
        self.max_round = 0
        self.proposals = {}
        self.promises = defaultdict(set)
        self.accepted = defaultdict(set)
        self.chosen = False
        # for acceptor
        self.min_proposal = None
        self.accepted_proposal = None
        self.accepted_value = None

    def nextn(self):
        self.max_round += 1
        return (self.max_round, self.addr)

    def highest(self, promises):
        n, v = None, None
        for peer, (accn, accv) in promises:
            if n is None or accn > n:
                n = accn
                v = accv
        return v

    def upon_Propose(self, v):
        if self.chosen:
            return
        n = self.nextn()
        self.proposals[n] = v
        log.info('%s propose n:%s, v:%s', self.addr, n, v)
        trigger(self.beb, 'Broadcast', {
            'typ': 'prepare',
            'n': n,
            })

    def upon_Deliver(self, q, m):
        n = m.get('n')
        if m['typ'] == 'promise':  # proposer
            if n[0] > self.max_round:
                self.max_round = n[0]
            p = self.promises[n]
            p.add((q, m['accepted']))
            if len(p) > self.N / 2:
                v = self.highest(p)
                if v:
                    self.proposals[n] = v
                else:
                    # FIXME: KeyError raised here, I didn't find the bug
                    v = self.proposals[n]
                trigger(self.beb, 'Broadcast', {
                    'typ': 'accept',
                    'n': n,
                    'v': v,
                    })
        elif m['typ'] == 'accepted':  # proposer
            p = self.accepted[n]
            p.add(q)
            if len(p) > self.N / 2:
                trigger(self.beb, 'Broadcast', {
                    'typ': 'decided',
                    'v': self.proposals[n],
                    })
        elif m['typ'] == 'prepare':  # acceptor
            if self.min_proposal is None or n > self.min_proposal:
                self.min_proposal = n
            trigger(self.fll, 'Send', q, {
                'typ': 'promise',
                'n': self.min_proposal,
                'accepted': (self.accepted_proposal, self.accepted_value),
                })
        elif m['typ'] == 'accept':  # acceptor
            if self.min_proposal is None or n >= self.min_proposal:
                self.min_proposal = n
                self.accepted_proposal = n
                self.accepted_value = m['v']
                trigger(self.fll, 'Send', q, {
                    'typ': 'accepted',
                    'n': n,
                    })
        elif m['typ'] == 'decided' and not self.chosen:
            self.chosen = True
            trigger(self.upper, 'Decide', m['v'])



class Proposer:
    pass


class Acceptor:
    pass


@implements('ReplicatedStateMachine')
@uses('BestEffortBroadcast', 'beb')
@uses('PerfectPointToPointLinks', 'pl')
class MultiPaxos(ABC):
    """
    http://www.youtube.com/watch?v=JEpsBg0AO6o

    - which log entry to use for a given client request ?
    - performance optimizations:
      use leader to reduce proposer conflicts
      eliminate most prepare requests
    - ensuring full replication
    - client protocol
    - configuration changes
    """
    def upon_Init(self):
        self.logs = []
        self.proposals = {}

    def upon_Execute(self, cmd):
        pos = self.guess_pos()
        proposer = Proposer(self, pos, cmd)
        proposer.start()
        
