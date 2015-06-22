import asyncio
import logging
import argparse
import random

from .basic import UDPProtocol, trigger
from .consensus import LeaderBasedEpochChange
from .failure_detector import IncreasingTimeout
from .paxos import Synod

log = logging.getLogger(__name__)


class Proc:
    def __init__(self, addr, peers):
        self.pid = str(addr[1])
        self.set_members(addr, peers)
        self.create_transport(UDPProtocol, addr)

    def set_members(self, addr, peers):
        self.addr = addr
        self.peers = set(peers) - {addr}
        self.members = {addr} | self.peers

    def create_transport(self, protocol, addr):
        loop = asyncio.get_event_loop()
        listen = loop.create_datagram_endpoint(protocol, local_addr=addr)
        self.transport, self.protocol = loop.run_until_complete(listen)
        self.protocol.addr = addr
        log.info('listen at %s', addr)


class Admin:
    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        log.warn('connection %s lost: %s', self, exc)

    def datagram_received(self, data, peer):
        # select serveral procs to propose random values
        for pid in random.sample(range(7), int(data)):
            proc = self.procs[pid]
            v = chr(65+random.randint(0, 25))
            trigger(proc.con, 'Propose', v)


class Test(Proc):
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        cls = IncreasingTimeout
        cls = LeaderBasedEpochChange
        cls = Synod
        self.con = cls(
            'con', self, self.protocol,
            self.addr, self.peers)

    def upon_StartEpoch(self, ts, leader):
        log.info('%s start epoch at ts %s', leader, ts)

    def upon_Decide(self, v):
        log.info('Decision: %s, %s', v, self.addr)

    def upon_Deliver(self, q, m):
        log.info('%d: Recv %s', self.addr[1], m)

    def upon_Crash(self, p):
        log.info('%s crashed', p)

    def upon_Suspect(self, p):
        log.info('%s suspect %s', self.addr[1], p[1])

    def upon_Restore(self, p):
        log.info('%s restore %s', self.addr[1], p[1])


def parse_args():
    def str2level(slevel):
        return {
            'debug': logging.DEBUG,
            'info': logging.INFO,
            'warn': logging.WARN,
            'error': logging.ERROR,
            }[slevel.lower()]
    p = argparse.ArgumentParser()
    p.add_argument('-l', '--level', default=logging.INFO, type=str2level)
    p.add_argument('-v', '--verbose', action='count')
    p.add_argument('--log-format', default='%(message)s')
    p.add_argument('--host', default='127.0.0.1')
    p.add_argument('-i', '--host-id', type=int, default=0)
    p.add_argument('-n', '--member-count', type=int, default=3)
    p.add_argument('--port-start', type=int, default=5000)
    p.add_argument('-a', '--all-in-one', action='store_true')
    p.add_argument('-A', '--admin', action='store_true')
    p.add_argument('--admin-port', type=int, default=4000)
    args = p.parse_args()
    args.members = [(args.host, args.port_start + i)
                    for i in range(args.member_count)]
    if args.verbose:
        args.level = logging.DEBUG
        args.log_format = '%(asctime)s[%(levelname)s]' \
                          '[%(name)s:%(lineno)s] %(message)s'
    return args


def main():
    args = parse_args()
    logging.basicConfig(level=args.level, format=args.log_format)
    logging.getLogger('asyncio').setLevel(logging.WARN)

    loop = asyncio.get_event_loop()
    if args.admin:
        addr = (args.host, args.admin_port)
        listen = loop.create_datagram_endpoint(Admin, addr)
        _, admin = loop.run_until_complete(listen)
        admin.procs = {}
        log.info('admin at %s', addr)

    for i, addr in enumerate(args.members):
        if i == args.host_id or args.all_in_one:
            proc = Test(addr, args.members)
            if args.admin:
                admin.procs[i] = proc

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    loop.close()


if __name__ == '__main__':
    main()
