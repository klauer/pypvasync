import time
import asyncio
import logging
import atexit
import queue
import threading
from functools import partial

from . import ca
from .callback_registry import CallbackRegistry

logger = logging.getLogger(__name__)


class CAContextHandler:
    def __init__(self, ctx):
        self._sub_lock = threading.RLock()

        self._running = True
        self._ctx = ctx
        self._loop = asyncio.get_event_loop()
        self._tasks = None
        self._connect_queue = queue.Queue()
        self._monitor_queue = queue.Queue()
        self._queues = dict(connection=self._connect_queue,
                            monitor=self._monitor_queue)

        sigs = list(self._queues.keys())
        self._cbreg = CallbackRegistry(allowed_sigs=sigs,
                                       ignore_exceptions=True)
        self.pv_names = {}

    def add_event(self, type_, info):
        self._queues[type_].put(info, block=False)

    def subscribe(self, sig, func, chid, *, oneshot=False):
        with self._sub_lock:
            # subscribe to connection, but it's already connected
            # so run the callback now
            if sig == 'connection' and ca.is_connected(chid):
                chid = ca.channel_id_to_int(chid)
                self._loop.call_soon_threadsafe(partial(func,
                                                        pvname=self.pv_names[chid],
                                                        # ca.name(chid),
                                                        chid=chid,
                                                        connected=True))
                if oneshot:
                    return

            cid = self._cbreg.subscribe(sig=sig, chid=chid, func=func,
                                        oneshot=oneshot)
            return cid

    def unsubscribe(self, cid):
        with self._sub_lock:
            self._cbreg.unsubscribe(cid)

    def _queue_loop(self, q):
        loop = self._loop
        ca.attach_context(self._ctx)
        while self._running:
            try:
                yield q.get(block=True, timeout=0.1)
            except queue.Empty:
                pass

    @asyncio.coroutine
    def connect_channel(self, chid, timeout=1.0):
        loop = self._loop
        fut = asyncio.Future()

        def connection_update(chid=None, pvname=None,
                              connected=None):
            print('connection update', chid, pvname, connected, fut)
            if fut.done():
                logger.debug('Connect_channel but future already done')
                return

            if connected:
                loop.call_soon_threadsafe(fut.set_result, True)
            else:
                loop.call_soon_threadsafe(fut.set_exception,
                                          asyncio.TimeoutError('Disconnected'))

        self.subscribe(sig='connection', chid=chid, func=connection_update,
                       oneshot=True)
        yield from fut

    def _connect_queue_loop(self):
        loop = self._loop
        for info in self._queue_loop(self._connect_queue):
            with self._sub_lock:
                print('*** connect queue:', info)
                chid = ca.channel_id_to_int(info.pop('chid'))
                pvname = ca.name(chid)
                self.pv_names[chid] = pvname
                loop.call_soon_threadsafe(partial(self._process_cb,
                                                  'connection', chid, **info))

    def _process_cb(self, sig, chid, **info):
        with self._sub_lock:
            print('*** cb processing', sig, chid, info)
            self._cbreg.process(sig, chid, **info)

    def _monitor_queue_loop(self):
        loop = self._loop
        for item in self._queue_loop(self._monitor_queue):
            with self._sub_lock:
                print('*** monitor queue item', item)
                # loop.call_soon_threadsafe(future.set_exception, ex)

    def _poll_thread(self):
        '''Poll context ctx in an executor thread'''
        ca.attach_context(self._ctx)
        try:
            while self._running:
                ca.pend_event(1.e-5)
                ca.pend_io(1.0)
                time.sleep(0.01)
        finally:
            print('context detach')
            ca.detach_context()
            print('done')

    def start(self):
        loop = self._loop
        self._tasks = [loop.run_in_executor(None, self._poll_thread),
                       loop.run_in_executor(None, self._connect_queue_loop),
                       ]

    def stop(self):
        self._running = False
        if self._tasks:
            for task in self._tasks:
                self._loop.run_until_complete(task)
            del self._tasks[:]

    def __del__(self):
        self.stop()


class CAContexts:
    def __init__(self):
        if hasattr(CAContexts, 'instance'):
            raise RuntimeError('CAContexts is a singleton')

        CAContexts.instance = self

        self.contexts = {}
        self.add_context()
        atexit.register(self.stop)

    def add_context(self, ctx=None):
        if ctx is None:
            ctx = ca.current_context()

        ctx_id = int(ctx)
        if ctx_id in self.contexts:
            return self.contexts[ctx_id]

        handler = CAContextHandler(ctx=ctx)
        self.contexts[ctx_id] = handler
        handler.start()
        return handler

    def __getitem__(self, ctx):
        return self.add_context(ctx)

    def stop(self):
        print('stopping')
        for ctx_id, context in list(self.contexts.items()):
            context.stop()
            del self.contexts[ctx_id]

        ca.clear_cache()
        ca.detach_context()

    def add_event(self, ctx, event_type, info):
        ctx_id = int(ctx)
        self.contexts[ctx_id].add_event(event_type, info)


def get_contexts():
    '''The global context handler'''
    global _cm
    return _cm


def get_current_context():
    return _cm[None]


_cm = CAContexts()
