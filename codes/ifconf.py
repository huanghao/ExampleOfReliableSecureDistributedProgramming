from .links import BasicLink, RetransmitWithACK, EliminateDuplicates
from .broadcast import (
    BasicBroadcast, LazyReliableBroadcast,
    MajorityAckUniformReliableBroadcast)
from .gossip import LazyProbabilisticBroadcast
from .failure_detector import ExcludeOnTimeout, IncreasingTimeout
from .leader_election import (
    MonarchicalLeaderElection, MonarchicalEventualLeaderElection)
from .consensus import FloodingConsensus


mapping = {
    'FairLossPointToPointLinks': BasicLink,
    'StubbornPointToPointLinks': RetransmitWithACK,
    'PerfectPointToPointLinks': EliminateDuplicates,

    'BestEffortBroadcast': BasicBroadcast,
    'ReliableBroadcast': LazyReliableBroadcast,
    'UniformReliableBroadcast': MajorityAckUniformReliableBroadcast,

    'ProbabilisticBroadcast': LazyProbabilisticBroadcast,

    'PerfectFailureDetector': ExcludeOnTimeout,
    'EventuallyPerfectFailureDetector': IncreasingTimeout,

    'LeaderElection': MonarchicalLeaderElection,
    'EventualLeaderDetector': MonarchicalEventualLeaderElection,

    'Consensus': FloodingConsensus,
    }


def get_implementation(ifname):
    try:
        return mapping[ifname]
    except KeyError:
        import sys
        print('ERR:Undefined interface name: %s' % ifname, file=sys.stderr)
        sys.exit(1)
