import os
import logging
import asyncio
import pickle
import hashlib
import random
from collections import defaultdict

log = logging.getLogger(__name__)

IFS = defaultdict(list)


def implements(ifname):
    def decorator(cls):
        IFS[ifname].append(cls)
        return cls
    return decorator


def uses(ifname, attr):
    def decorator(cls):
        if ifname not in IFS:
            raise ValueError("Undefined interface name: %s" % ifname)
        if hasattr(cls, '_uses'):
            cls._uses.append((ifname, attr))
        else:
            cls._uses = [(ifname, attr)]
        return cls
    return decorator


class ABC:
    def __init__(self, name, upper, udp, addr, peers, init=True):
        self.name, self.upper = name, upper
        self.addr, self.peers = addr, peers
        self.members = set(peers) | {addr}
        self.N = len(self.members)
        if init:
            trigger(self, 'Init')

        from .ifconf import get_implementation
        for ifname, attr in self._uses:
            concrete = get_implementation(ifname)
            that = concrete('%s.%s' % (name, attr), self, udp, addr, peers)
            setattr(self, attr, that)


def trigger(obj, event, *attrs):
    m = getattr(obj, 'upon_' + event, None)
    if not m:
        log.warn('Unknown event %s to %s', event, obj.__class__.__name__)
        return
    # put event to a queue instead of calling it right now
    # need this hack, since some modules have cycle dependency
    # in their Init event
    import asyncio
    loop = asyncio.get_event_loop()
    loop.call_soon(m, *attrs)


def start_timer(delay, callback, *args):
    loop = asyncio.get_event_loop()
    loop.call_later(delay, callback, *args)


def mhash(m):
    return hashlib.md5(pickle.dumps(m)).hexdigest()


class UDPProtocol:
    DELAY = 2

    def connection_made(self, transport):
        self.transport = transport
        self.handlers = {}

    def connection_lost(self, exc):
        log.warn('connection %s lost: %s', self, exc)

    def datagram_received(self, data, peer):
        try:
            name, msg = pickle.loads(data)
            handler = self.handlers[name]
        except:
            log.warn('bad msg from %s: %s', peer, data)
        else:
            loop = asyncio.get_event_loop()
            loop.call_later(
                random.random() * self.DELAY,
                trigger, handler, 'Deliver', peer, msg)

    def register(self, name, handler):
        self.handlers[name] = handler

        def sendto(msg, peer):
            data = pickle.dumps((name, msg))
            loop = asyncio.get_event_loop()

            def _send():
                log.debug('%s --> %s: %s', self.addr, peer, msg)
                self.transport.sendto(data, peer)
            return loop.call_later(random.random() * self.DELAY, _send)
        return sendto


class Store:
    def __init__(self, storeid):
        self.filename = os.path.abspath('__store.%s' % storeid)

    def exists(self):
        return os.path.exists(self.filename)

    def store(self, data):
        with open(self.filename, 'wb') as file:
            pickle.dump(data, file)

    def retrieve(self):
        with open(self.filename, 'rb') as file:
            obj = pickle.load(file)
        return obj
