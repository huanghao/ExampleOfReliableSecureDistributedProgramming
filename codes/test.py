from .basic import IFS
from .links import *
from .failure_detector import *
from .leader_election import *
from .broadcast import *
from .gossip import *
from .ordering import *
from .register import *
from .consensus import *


for ifname in sorted(IFS.keys(), key=lambda i: ''.join(reversed(i))):
    classes = IFS[ifname]
    print(ifname)
    for cls in classes:
        print('-', cls)
        if hasattr(cls, '_uses'):
            for i in cls._uses:
                print('  +', i)
    print()
