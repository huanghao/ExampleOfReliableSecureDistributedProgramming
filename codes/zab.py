"""
"""
import logging

from .basic import implements, uses, trigger, ABC

log = logging.getLogger(__name__)


@implements('ZKAtomicBroadcast')
@uses('CausalOrderReliableBroadcast', 'crb')
class Zab(ABC):
    """
    ZK makes the following requirements on the broadcast protocol:
    - Reliable delivery
    - Total order
    - Causal order

    For correctness, ZK additionally requires
    - Prefix property: if m is the last message delivered for a leader L, any
    message proposed before m by L must also be delivered
    """
    def upon_Init(self):
        trigger(self.crb, 'Broadcast', 'xx')
