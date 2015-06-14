import uuid
import logging

from .basic import trigger, implements, uses, start_timer, ABC

log = logging.getLogger(__name__)


@implements('PerfectFailureDetector')
@uses('PerfectPointToPointLinks', 'pl')
class ExcludeOnTimeout(ABC):
    """
    Algorithm 2.5: Exclude on Timeout
    Request: Send
    Indication: Crash
    """
    TIMEOUT = 10

    def upon_Init(self):
        self.alive = set(self.peers)
        self.detected = set()
        start_timer(self.TIMEOUT, self.upon_Timeout)

    def upon_Timeout(self):
        for p in self.peers:
            if p not in self.alive and p not in self.detected:
                self.detected.add(p)
                trigger(self.upper, 'Crash', p)
            trigger(self.pl, 'Send', p, {
                'mid': uuid.uuid4(),
                'typ': 'heartbeatrequest',
            })
        self.alive = set()
        start_timer(self.TIMEOUT, self.upon_Timeout)

    def upon_Deliver(self, q, m):
        if m['typ'] == 'heartbeatrequest':
            trigger(self.pl, 'Send', q, {
                'mid': uuid.uuid4(),
                'typ': 'heartbeatreply',
            })
        elif m['typ'] == 'heartbeatreply':
            self.alive.add(q)


@implements('EventuallyPerfectFailureDetector')
@uses('PerfectPointToPointLinks', 'pl')
class IncreasingTimeout(ABC):
    """
    Algorithm 2.7: Increasing Timeout
    Request: Send
    Indication: Suspect, Restore
    """
    DELAY = 4

    def upon_Init(self):
        self.alive = set(self.peers)
        self.suspected = set()
        self.delay = self.DELAY
        start_timer(self.delay, self.upon_Timeout)

    def upon_Timeout(self):
        if self.alive & self.suspected:
            self.delay += self.DELAY
            log.info('%s inc delay to %s', self.addr[1], self.delay)
        for p in self.peers:
            if p not in self.alive and p not in self.suspected:
                self.suspected.add(p)
                trigger(self.upper, 'Suspect', p)
            elif p in self.alive and p in self.suspected:
                self.suspected.remove(p)
                trigger(self.upper, 'Restore', p)
            trigger(self.pl, 'Send', p, {'heartbeat': uuid.uuid4()})
        self.alive = set()
        start_timer(self.delay, self.upon_Timeout)

    def upon_Deliver(self, q, m):
        # log.debug('%s <- %s (%s)', self.addr[1], q[1], m['heartbeat'])
        self.alive.add(q)
