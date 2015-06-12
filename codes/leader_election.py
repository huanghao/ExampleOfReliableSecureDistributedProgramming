import uuid
import hashlib
import logging

from .basic import Store, trigger, start_timer, implements, uses
from .links import BasicLink
from .failure_detector import ExcludeOnTimeout, IncreasingTimeout

log = logging.getLogger(__name__)


@implements('LeaderElection')
@uses('PerfectFailureDetector', ExcludeOnTimeout, 'p')
class MonarchicalLeaderElection:
    """
    Algo 2.6: Monarchical Leader Election
    """
    def __init__(self, upperlayer, addr, peers):
        self.set_upper_layer(upperlayer)
        self.set_members(addr, peers)
        self.p = ExcludeOnTimeout(self, addr, peers)
        self.suspected = set()
        self.leader = None
        self.elect()

    def elect(self):
        leader = max(self.members - self.suspected)
        if leader != self.leader:
            self.leader = leader
            self.upperlayer.trigger('Leader', leader=leader)

    def upon_Crash(self, peer):
        self.suspected.add(peer)
        self.elect()

    def upon_Restore(self, peer):
        self.suspected.remove(peer)
        self.elect()


@implements('EventualLeaderElection')
@uses('EventuallyPerfectFailureDetector', IncreasingTimeout, 'p')
class MonarchicalEventualLeaderElection:
    """
    Algo 2.8: monarchical eventual leader election
    """
    def __init__(self, upperlayer, addr, peers):
        self.set_upper_layer(upperlayer)
        self.set_members(addr, peers)
        self.p = IncreasingTimeout(self, addr, peers)
        self.suspected = set()
        self.leader = None
        self.elect()

    def elect(self):
        leader = max(self.members - self.suspected)
        if leader != self.leader:
            self.leader = leader
            self.upperlayer.trigger('Trust', leader=leader)

    def upon_Suspect(self, peer):
        self.suspected.add(peer)
        self.elect()

    def upon_Restore(self, peer):
        self.suspected.remove(peer)
        self.elect()


@implements('EventualLeaderElection')
@uses('FairLossPointToPointLinks', BasicLink, 'fll')
class ElectLowerEpoch:
    """
    Algorithm 2.9: Elect Lower Epoch
    """
    DELAY = 0.5

    def __init__(self, upper, addr, peers):
        self.set_upper_layer(upper)
        self.set_members(addr, peers)
        self.fll = FairLossPointToPointLinks.create(self, addr)
        sid = hashlib.md5(str(addr).encode()).hexdigest()
        self.store = Store(sid)
        if self.store.exists():
            self.trigger('Recovery')
        else:
            self.trigger('Init')

    def upon_Init(self):
        self.epoch = 0
        self.store.store(self.epoch)
        self.candidates = {}
        self.trigger('Recovery')

    def upon_Recovery(self):
        self.leader = max(self.members)
        self.upperlayer.trigger('Trust', leader=self.leader)
        self.delay = self.DELAY
        self.epoch = self.store.retrieve()
        self.epoch += 1
        self.store.store(self.epoch)
        log.info('with epoch %s', self.epoch)
        self.pulse()

    def upon_HeartbeatTimeout(self):
        leader, epoch = self.select(self.candidates.items())
        if leader != self.leader:
            self.leader = leader
            self.delay += self.DELAY
            log.info('delay increased to %s', self.delay)
            self.upperlayer.trigger('Trust', leader=leader)
        self.pulse()

    def select(self, candidates):
        """
        deterministic to select peer with
        the minimal epoch and the largest rank
        """
        if not candidates:
            return None, None
        _, min_e = min(candidates, key=lambda i: i[1])
        min_p = [(p, e) for p, e in candidates if e == min_e]
        return max(min_p)

    def pulse(self):
        for p in self.members:
            trigger(self.fll, 'Send', p, {
                'msgid': uuid.uuid4(),
                'typ': 'Heartbeat',
                'epoch': self.epoch,
                })
        self.candidates = {}
        start_timer(self.delay, self.upon_HeartbeatTimeout)

    def upon_Deliver(self, msg, peer):
        epoch = msg['epoch']
        if (peer in self.candidates and
                self.candidates[peer] < epoch):
            self.candidates.pop(peer)
        self.candidates[peer] = epoch
