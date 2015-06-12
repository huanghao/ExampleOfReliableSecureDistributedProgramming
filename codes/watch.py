from __future__ import print_function
import sys
from collections import defaultdict

msg2proc = defaultdict(set)
proc2msg = defaultdict(list)

for line in sys.stdin:
    if 'heartbeat' in line:
        continue
    print(line, end='')
    parts = line.rstrip().split(None, 4)
    if ' Recv ' in line:
        before, msg = line.rstrip().split(' Recv ', 1)
        pid = before.split()[-1]
        msg2proc[msg].add(pid)
        proc2msg[pid].append(msg)
        for k in sorted(msg2proc.keys()):
            v = msg2proc[k]
            print(k, '\t', len(v), '\t', '|'.join(v))
        for k in sorted(proc2msg.keys()):
            v = proc2msg[k]
            print(k, '\t', len(v), '\t', '|'.join(v))
        print('-'*40)
    sys.stdout.flush()
