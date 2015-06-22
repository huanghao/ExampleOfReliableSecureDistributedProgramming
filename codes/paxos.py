import logging
from collections import defaultdict

from .basic import implements, uses, trigger, ABC

log = logging.getLogger(__name__)


@implements('Consensus')
@uses('BestEffortBroadcast', 'beb')
@uses('FairLossPointToPointLinks', 'fll')
class Synod(ABC):
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
        n = m['n']
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
                    v = self.proposals[n]
                trigger(self.beb, 'Broadcast', {
                    'typ': 'accept',
                    'n': n,
                    'v': v,
                    })
        elif m['typ'] == 'accepted':  # proposer
            p = self.accepted[n]
            p.add(q)
            if len(p) > self.N / 2 and not self.chosen:
                self.chosen = True
                trigger(self.upper, 'Decide', self.proposals[n])
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
                self.accepted_proposal = n
                self.accepted_value = m['v']
                trigger(self.fll, 'Send', q, {
                    'typ': 'accepted',
                    'n': n,
                    })
