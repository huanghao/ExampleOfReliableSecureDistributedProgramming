import os
import logging
import asyncio
import pickle
import hashlib
import random

log = logging.getLogger(__name__)


def implements(interface):
    def decorator(cls):
        return cls
    return decorator


def uses(*ifs):
    def decorator(cls):
        return cls
    return decorator


def trigger(obj, event, *attrs):
    m = getattr(obj, 'upon_' + event, None)
    if m:
        return m(*attrs)
    log.warn('Unknown event %s to %s', event, obj.__class__.__name__)


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