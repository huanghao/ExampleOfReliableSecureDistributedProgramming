"""
regular, uniform, and randomized consensus with crash-stop processes, logged
consensus with crash-recovery processes, and Byzantine and Byzantine randomized
consensus with arbi- trary-fault process abstractions.

Module:
  Name: Consensus, instance c.

Events:
  Request: <c, Propose | v>: Proposes value v for consensus.
  Indication: <c, Decide | v>: Outputs a decided value v fof consensus.

Properties:
  C1: Termination: Every correct process eventually decides some value.
  C2: Validity: If a process decides v, then v was proposed by some process.
  C3: Integrity: No process decides twice.
  C4: Agreement: No two correct processes decide differently.
"""
from collections import defaultdict
import logging

from .basic import implements, uses, trigger, ABC

log = logging.getLogger(__name__)


@implements('Consensus')
@uses('BestEffortBroadcast', 'beb')
@uses('PerfectFailureDetector', 'p')
class FloodingConsensus(ABC):
    """
    algo 5.1

    A process floods the system with all proposals it has seen in previous
    rounds. When a process receives a proposal set from another process, it
    merges this set with its own.

    A process decides when it has reached a round during which it has gathered
    all proposals that will ever possibly be seen by any correct process. At
    the end of this round, the process decides a specific value in its proposal
    set.

    A round terminates at a process p when p has received a message from every
    process that has not been detected to have crashed by p in that round.

    In every round, O(N^2) messages are exchanged and O(N^2) DECIDED messages
    are also exchanged after a process has decided. For each additional round
    where a process crashes, another O(N^2) message exchanges occur. In the
    worst case, the algorithm uses O(N^3) messages.
    """
    def upon_Init(self):
        self.correct = set(self.members)
        self.round = 1
        self.decision = None
        self.proposals = defaultdict(set)
        self.receivedfrom = defaultdict(set)
        self.receivedfrom[0] = set(self.members)

    def upon_Crash(self, p):
        log.info("%s Crashed", p)
        self.correct.remove(p)

    def upon_Propose(self, v):
        self.proposals[1].add(v)
        log.info("%s propose %s", self.addr, self.proposals[1])
        trigger(self.beb, 'Broadcast', {
            'typ': 'proposal',
            'round': 1,
            'proposals': self.proposals[1],
            })

    def upon_Deliver(self, q, m):
        if m['typ'] == 'proposal':
            r, ps = m['round'], m['proposals']
            self.receivedfrom[r].add(q)
            self.proposals[r] |= ps
            self.try_to_decide()
        elif m['typ'] == 'decided':
            if self.decision is None and q in self.correct:
                self.decide(m['decision'])

    def try_to_decide(self):
        if (self.decision is not None or
                not self.correct.issubset(self.receivedfrom[self.round])):
            return

        if (self.receivedfrom[self.round] == self.receivedfrom[self.round-1]):
            self.decide(min(self.proposals[self.round]))
        else:
            self.round += 1
            log.info("%s propose %s", self.addr, self.proposals[self.round-1])
            trigger(self.beb, 'Broadcast', {
                'typ': 'proposal',
                'round': self.round,
                'proposals': self.proposals[self.round-1],
                })

    def decide(self, v):
        log.info('%s made decision %s', self.addr, v)
        self.decision = v
        trigger(self.beb, 'Broadcast', {
            'typ': 'decided',
            'decision': self.decision,
            })
        trigger(self.upper, 'Decide', self.decision)


@implements("Consensus")
@uses('BestEffortBroadcast', 'beb')
@uses('PerfectFailureDetector', 'p')
class HierarchicalConsensus(ABC):
    """
    Algorithm 5.2

    Performance: this algorithm requires N communication steps to terminate and
    exchanges O(N) messages in each round.
    """
    def upon_Init(self):
        self.detectedranks = set()
        self.round = 1
        self.proposal = None
        self.proposer = 0
        self.delivered = False
        self.broadcast = False

    def rank(self, p):
        return sorted(self.members).index(p)

    def upon_Crash(self, p):
        log.info("%s crashed", p)
        self.detectedranks.add(self.rank(p))
        if self.round in self.detectedranks or self.delivered[self.round]:
            self.round += 1

    def upon_Propose(self, v):
        if not self.proposal:
            self.proposal = v

    def try_to_decide(self):
        if (self.round == self.rank(self.addr) and
                self.proposal and not self.broadcast):
            self.broadcast = True
            trigger(self.beb, 'Broadcast', {
                'typ': 'decided',
                'proposal': self.proposal,
                })
            trigger(self.upper, 'Decide', self.proposal)

    def upon_Deliver(self, q, m):
        assert 'decided' == m['typ']
        v = m['proposal']
        r = self.rank(q)
        if r < self.rank(self.addr) and r > self.proposer:
            self.proposal = v
            self.proposer = r
        self.delivered[r] = True


@implements('UniformConsensus')
@uses('BestEffortBroadcast', 'beb')
@uses('PerfectFailureDetector', 'p')
class FloodingUniformConsensus(ABC):
    """
    Algorithm 5.3

    Uniform agreement: No two processes decide differently, whether they are
    correct or not.

    None of the consensus algorithms we presented so far ensure uniform
    argreement. Roughly speaking, this is because some of the processes decide
    too early, without making sure that their decision has been seen by enough
    processes. Should such an early deciding process crash, other processes
    might have no choice but to decide a different value.

    This algorithm always runs for N rounds and every process decides only in
    round N. Intuitively, this permits that one process crashes in every round.
    """
    def onup_Init(self):
        self.correct = set(self.member)
        self.round = 1
        self.decision = None
        self.proposalset = set()
        self.receivedfrom = set()

    def upon_Crash(self, p):
        log.info('%s crashed', p)
        self.correct.remove(p)
        self.check()

    def upon_Propose(self, v):
        self.proposalset.add(v)
        trigger(self.beb, 'Broadcast', {
            'typ': 'proposal',
            'round': 1,
            'proposals': self.proposalset,
            })

    def upon_Deliver(self, q, m):
        if m['typ'] == 'proposal':
            r, ps = m['round'], m['proposals']
            if r == self.round:
                self.receivedfrom.add(q)
                self.proposalset |= ps
                self.check()

    def check(self):
        if self.correct.issubset(self.receivedfrom) and self.decision is None:
            if self.round == len(self.members):
                self.decision = min(self.proposalset)
                trigger(self.upper, 'Decide', self.decision)
            else:
                self.round += 1
                self.receivedfrom = set()
                trigger(self.beb, 'Broadcast', {
                    'typ': 'proposal',
                    'round': self.round,
                    'proposals': self.proposalset,
                    })


@implements('UniformConsensus')
@uses('PerfectPointToPointLinks', 'pl')
@uses('BestEffortBroadcast', 'beb')
@uses('ReliableBroadcast', 'rb')
@uses('PerfectFailureDetector', 'p')
class HierarchicalUniformConsensus(ABC):
    """
    Algorithm 5.4

    Every process maintaines a single proposal value that it broadcasts in the
    round corresponding to its rank. When it receives a proposal from a more
    importantly ranked process, it adopts the value.

    In every round of the algorithm, the process whose rank corresponds to the
    number of the round is the leader. A round consists of two communication
    steps: within the same round, the leader broadcast a PROPOSAL message to
    all processes, trying to impose its value, and then expects to obtain an
    ACK from all corret processes. Processes that receive a proposal from the
    leader of the round adopt this proposal as their own and send an ACK back
    to the leader of the round. If the leader succeeds in collecting an ACK
    from all processes expect those that P has detected to have crashed, the
    leader can decide. It disseminates the decided value using a reliable
    broadcast communication abstraction.

    if the leader of a round fails, the correct processes detect this and
    proceed to the next round. The leader of the next round is the process
    immediately below the current leader in hierarchy;the new leader broadcases
    its proposal only if it has not already delivered the decision through the
    reliable broadcast abstraction.

    Note that if the leader failes after disseminating the decision, the
    reliable broadcast abstraction ensures that if any process decides and
    stops taking any leadership action then all correct processes will also
    decide.

    Performance: if there are no failures, the algorithm terminates in three
    communication steps: two steps for the first round and one step for the
    reliable broadcast. The algorithm exchanges O(N) messages. Each failure
    of a leader adds two additional communication steps and O(N) additional
    messages.
    """
    def upon_Init(self):
        self.detectedranks = set()
        self.ackransk = set()
        self.round = 1
        self.proposal = None
        self.decision = None
        self.proposed = set()

    def rank(self, p):
        return sorted(self.members).index(p)

    def upon_Crash(self, p):
        log.info('%s crashed', p)
        self.detectedranks.add(self.rank(p))
        self.round_up()
        self.try_to_decide()

    def try_to_propose(self):
        if (self.round == self.rank(self.addr) and
                self.proposal and self.decision is None):
            trigger(self.beb, 'Broadcast', {
                'typ': 'proposal',
                'proposal': self.proposal,
                })

    def round_up(self):
        if self.round in self.detectedranks:
            if self.proposed[self.round]:
                self.proposal = self.proposed[self.round]
            self.round += 1

    def try_to_decide(self):
        if len(self.detectedranks | self.self.ackranks) == len(self.members):
            trigger(self.rb, 'Broadcast', {
                'typ': 'decided',
                'decision': self.proposal,
                })

    def upon_Propose(self, v):
        if not self.proposal:
            self.proposal = v
            self.try_to_propose()

    def upon_Deliver(self, q, m):
        r = self.rank(q)
        if m['typ'] == 'proposal':
            self.proposed[r] = m['proposal']
            if r >= self.round:
                trigger(self.pl, 'Send', q, {'typ': 'ack'})
            self.round_up()
        elif m['typ'] == 'ack':
            self.ackranks.add(r)
            self.try_to_decide()
        elif m['typ'] == 'decided':
            self.decision = m['proposal']
            trigger(self.upper, 'Decide', self.decision)


@implements('EpochChange')
@uses('PerfectPointToPointLinks', 'pl')
@uses('BestEffortBroadcast', 'beb')
@uses('EventualLeaderDetector', 'o')
class LeaderBasedEpochChange(ABC):
    """
    Algorithm 5.5: Leader-based Epoch-change

    Indication: <StartEpoch | ts, l>: starts the epoch identified by timestamp
    ts with leader l.

    - Monotonicity: If a correct process starts an epoch(ts,l) and later starts
    and epoch (ts', l'), then ts' > ts.
    - Consistency: If a correct process tarts and epoch (ts, l) and another
    correct process starts an epoch (ts', l') with ts = ts', then l = l'.
    - Eventual leadership: there is a time after which every correct process
    has started some epoch and starts no further epoch, such that the last
    epoch started at every correct process is epoch (ts, l) and process l is
    correct.

    When initialized, it's assumed that a default epoch with ts 0 and a leader
    l0 is active at all correct processes.
    """
    def upon_Init(self):
        self.trusted = None
        self.lastts = 0
        self.ts = self.rank(self.addr)

    def rank(self, p):
        return sorted(self.members).index(p)

    def new_epoch(self):
        self.ts += self.N
        trigger(self.beb, 'Broadcast', {
            'typ': 'newepoch',
            'ts': self.ts,
            })

    def upon_Trust(self, p):
        self.trusted = p
        if p == self.addr:
            self.new_epoch()

    def upon_Deliver(self, q, m):
        if m['typ'] == 'newepoch':
            newts = m['ts']
            if q == self.trusted and newts > self.lastts:
                self.lastts = newts
                trigger(self.upper, 'StartEpoch', newts, q)
            else:
                trigger(self.pl, 'Send', q, {'typ': 'nack'})
        elif m['typ'] == 'nack':
            if self.trusted == self.addr:
                self.new_epoch()


@implements('EpochConsensus')
@uses('PerfectPointToPointLinks', 'pl')
@uses('BestEffortBroadcast', 'beb')
class ReadWriteEpochChange(ABC):
    """
    Algorithm 5.6

    Events:
    - Request <ep, Propose | v>: Proposes value a for epoch consensus. Executed
    only by the leader l.
    - Request <ep, Abort>: Aborts epoch consensus.
    - Indication <ep, Decide | v>: Outputs a decided value v of epoch consensus
    - Indication <ep, Aborted | state>: Singals that epoch consensus has
    completed the abort and outputs internal state.
    """
    def upon_Init(self, state):
        self.valts, self.val = state
        self.tmpval = None
        self.states = {}
        self.accepted = 0

    def upon_Propose(self, v):  # only leader l
        self.tmpval = v
        trigger(self.beb, 'Broadcast', {'typ': 'read'})

    def upon_Deliver(self, q, m):
        if m['typ'] == 'read':
            trigger(self.pl, 'Send', q, {
                'typ': 'state',
                'ts': self.valts,
                'val': self.val,
                })
        elif m['typ'] == 'state':  # only leader l
            self.states[q] = (m['ts'], m['val'])
            self.check_to_write()
        elif m['typ'] == 'write':
            self.valts, self.val = m['ts'], m['val']
            trigger(self.pl, 'Send', q, {'typ': 'accept'})
        elif m['typ'] == 'accept':  # only leader l
            self.accepted += 1
            if self.accepted > self.N / 2:
                self.accepted = 0
                trigger(self.beb, 'Broadcast', {
                    'typ': 'decided',
                    'val': self.tmpval,
                    })
        elif m['typ'] == 'decided':
            trigger(self.upper, 'Decide', m['val'])

    def check_to_write(self):  # only leader l
        if len(self.states) > self.N / 2:
            ts, v = max(self.states.values())
            if v:
                self.tmpval = v
            self.states = {}
            trigger(self.beb, 'Broadcast', {
                'typ': 'write',
                'ts': self.ts,
                'val': self.tmpval,
                })

    def upon_Abort(self):
        trigger(self.upper, 'Aborted', self.valts, self.val)
        # halt  # stop operating when aborted


@implements('UniformConsensus')
@uses('EpochChange', 'ec')
@uses('EpochConsensus', 'y')  # multiple instances
class LeaderDrivenConsensus(ABC):
    """
    Algorithm 5.7
    """
    def upon_Init(self):
        pass
