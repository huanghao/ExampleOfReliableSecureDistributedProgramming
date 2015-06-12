"""
Some of the presented register abstractions and algorithms restrict the set of
processes that may write to and read from a register. The simplest case is a
register with one writer and one reader, which is called a (1, 1) register; the
writer is a specific process known in advance, and so is the reader. We will
also consider registers with one specific writer and N readers, which means
that any process can read from the register. It is called a (1, N ) register.
Finally, a register to which every process may write to and read from is called
an (N,N)register. Sometimes a (1,1) register is also called a single-writer,
single-reader register, a (1, N ) register is called a single-writer,
multi-reader register, and an (N,N) register is called a multi-writer,
multi-reader register.

- A safe register may return an arbitrary value when a write is concurrently
  ongoing.
- A regular register, in contrast, ensures a minimal guarantee in the face of
  concurrent or failed operations, and may only return the previous value or
  the newly written value.
- An atomic register is even stronger and provides a strict form of consistency
  even in the face of concurrency and failures.
"""
from .basic import implements, uses, trigger, ABC
from .links import EliminateDuplicates
from .broadcast import BasicBroadcast
from .failure_detector import ExcludeOnTimeout


@implements('OneNRegularRegister')
@uses('BestEffortBroadcast', BasicBroadcast, 'beb')
@uses('PerfectPointToPointLinks', EliminateDuplicates, 'pl')
@uses('PerfectFailureDetector', ExcludeOnTimeout, 'p')
class ReadOneWriteAll(ABC):
    """
    Algorithm 4.1: the reader reads one value and the writer writes all values

    Request: Read, Write
    Indication: ReadReturn, WriteReturn

    Termination: if a correct process invokes an operation, then the operation
        eventually completes.
    Validity: A read that is not concurrent with a write returns the last value
        written; a read that is concurrent with a write returns the last value
        written or the value concurrently written.
    """
    def upon_Init(self):
        self.val = None
        self.correct = set(self.peers) + {self.addr}
        self.writeset = set()

    def upon_Crash(self, p):
        self.correct.remove(p)
        self.check()

    def upon_Read(self):
        trigger(self.upper, 'ReadReturn', self.val)

    def upon_Write(self, v):
        trigger(self.beb, 'Broadcast', {
            'typ': 'write',
            'val': v,
            })

    def check(self):
        if self.correct.issubset(self.writeset):
            self.writeset = set()
            trigger(self.upper, 'WriteReturn')

    def upon_Deliver(self, q, m):
        if m['typ'] == 'write':
            self.val = m['val']
            trigger(self.pl, 'Send', q, {'typ': 'ack'})
        elif m['typ'] == 'ack':
            self.writeset.add(q)
            self.check()


@implements('OneNRegularRegister')
@uses('BestEffortBroadcast', BasicBroadcast, 'beb')
@uses('PerfectPointToPointLinks', EliminateDuplicates, 'pl')
class MajorityVotingRegularRegister(ABC):
    """
    Algorithm 4.2

    if the failure detector is not perfect, the "Read-One Write-All" algorithm
    (Algorithm 4.1) may violate the validity property of the register. 4.2
    implements a regular register in the fail-silent model, it does not rely on
    any failure detection abstraction. Instead this algorithm assumes that a
    majority of the processes is correct.
    """
    def upon_Init(self):
        self.N = (len(self.peers) + 1)
        self.ts = 0
        self.val = None
        self.wts = 0  # write-timestamp
        self.acks = 0
        self.rid = 0  # read-request identifier
        self.readlist = {}

    def upon_Write(self, v):
        self.wts += 1
        self.acks = 0
        trigger(self.beb, 'Broadcast', {
            'typ': 'write',
            'ts': self.wts,
            'val': self.val,
            })

    def upon_Read(self):
        self.rid += 1
        self.readlist = {}
        trigger(self.beb, 'Broadcast', {
            'typ': 'read',
            'rid': self.rid,
            })

    def upon_Deliver(self, q, m):
        if m['typ'] == 'write':
            if m['ts'] > self.ts:
                self.ts = m['ts']
                self.val = m['val']
            trigger(self.pl, 'Send', q, {
                'typ': 'ack',
                'ts': m['ts'],
                })
        elif m['typ'] == 'ack':
            if m['ts'] == self.wts:
                self.acks += 1
                if self.acks > (self.N / 2):
                    self.acks = 0
                    trigger(self.upper, 'WriteReturn')
        elif m['typ'] == 'read':
            trigger(self.pl, 'Send', q, {
                'typ': 'value',
                'rid': m['rid'],
                'ts': self.ts,
                'val': self.val,
                })
        elif m['typ'] == 'value' and m['rid'] == self.rid:
            self.readlist[q] = (m['ts'], m['val'])
            if len(self.readlist) > (self.N / 2):
                # choosing the largest timestamp ensures that the value written
                # last is returned
                _, v = max(self.readlist)
                self.readlist = {}
                trigger(self.upper, 'ReadReturn', v)


@implements('OneOneAtomicRegister')
@uses('OneNRegularRegister', MajorityVotingRegularRegister, 'onrr')
class ONRRtoOOAR(ABC):
    """
    Algorithm 4.3: From (1, N) Regular to (1, 1) Atomic Registers
    """
    def upon_Init(self):
        self.ts, self.val = 0, None
        self.wts = 0

    def upon_Write(self, v):
        self.wts += 1
        trigger(self.onrr, 'Write', (self.wts, self.val))

    def upon_WriteReturn(self):
        trigger(self.upper, 'WriteReturn')

    def upon_Read(self):
        trigger(self.upper, 'Read')

    def upon_ReadReturn(self, v):
        ts, val = v
        if ts > self.ts:
            self.ts, self.val = ts, val
        trigger(self.upper, 'ReadReturn', self.val)


@implements('OneNAtomicRegister')
@uses('OneOneAtomicRegister', ONRRtoOOAR, 'ooar')
class OOARtoONAR(ABC):
    """
    Algorithm 4.4: From (1, 1) Atomic to (1, N) Atomic Registers

    Ordering: if a read returns a value v and a subsequent read returns a value
    w, then the write of w does not precede the write of v.
    """
    def upon_Init(self):
        self.ts = 0
        self.acks = 0
        self.writing = False
        self.readval = None
        self.readlist = ...
        ...

    def upon_Write(self, v):
        self.ts += 1
        self.writing = True
        for q in self.members:
            trigger(self.ooar.q.self, 'Write', (self.ts, v))

    def upon_Read(self):
        for r in self.members:
            trigger(self.ooar.self.r, 'Read')
