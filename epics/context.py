import time
import asyncio
import logging
import atexit
import queue
import threading
import ctypes
import copy
from functools import partial

from . import ca
from . import dbr
from . import utils
from . import cast
from . import errors
from .callback_registry import CallbackRegistry

logger = logging.getLogger(__name__)
loop = asyncio.get_event_loop()


class CAContextHandler:
    # Default mask for subscriptions (means update on value changes exceeding
    # MDEL, and on alarm level changes.) Other option is dbr.DBE_LOG for
    # archive changes (ie exceeding ADEL)
    default_mask = dbr.DBE_VALUE | dbr.DBE_ALARM

    def __init__(self, ctx):
        self._sub_lock = threading.RLock()

        self._running = True
        self._ctx = ctx
        self._loop = asyncio.get_event_loop()
        self._tasks = None
        self._event_queue = queue.Queue()

        sigs = ['connection', 'monitor']
        self._cbreg = CallbackRegistry(allowed_sigs=sigs,
                                       ignore_exceptions=True)
        self.channel_to_pv = {}
        self.pv_to_channel = {}
        self.ch_monitors = {}
        self.evid = {}

    def add_event(self, type_, info):
        self._event_queue.put((type_, info), block=False)

    def create_channel(self, pvname):
        try:
            return self.pv_to_channel[pvname]
        except KeyError:
            pass

        pvname = pvname.encode('ascii')
        chid = dbr.chid_t()
        ret = ca.libca.ca_create_channel(pvname,
                                         _on_connection_event.ca_callback,
                                         0, 0, ctypes.byref(chid))
        ca.PySEVCHK('create_channel', ret)
        return ca.channel_id_to_int(chid)

    def subscribe(self, sig, func, chid, *, oneshot=False):
        with self._sub_lock:
            cid = self._cbreg.subscribe(sig=sig, chid=chid, func=func,
                                        oneshot=oneshot)
            return cid

    def subscribe_connect(self, func, chid, *, oneshot=False):
        with self._sub_lock:
            # subscribe to connection, but it's already connected
            # so run the callback now
            if ca.is_connected(chid):
                chid = ca.channel_id_to_int(chid)
                callback = partial(func, pvname=self.channel_to_pv[chid],
                                   chid=chid, connected=True)
                self._loop.call_soon_threadsafe(callback)
                if oneshot:
                    return

            return self.subscribe(sig='connection', chid=chid, func=func,
                                  oneshot=oneshot)

    def subscribe_monitor(self, func, chid, *, use_ctrl=False, use_time=True,
                          mask=None):
        with self._sub_lock:
            mask |= self.default_mask
            ftype = dbr.promote_type(ca.field_type(chid), use_ctrl=use_ctrl,
                                     use_time=use_time)

            mon_key = (mask, ftype)
            if mon_key in self.ch_monitors:
                evid = self.ch_monitors[mon_key]
            else:
                evid = ctypes.c_void_p()
                ca_callback = _on_monitor_event.ca_callback
                ret = ca.libca.ca_create_subscription(ftype, 0, chid, mask,
                                                      ca_callback, None,
                                                      ctypes.byref(evid))
                ca.PySEVCHK('create_subscription', ret)

                self.ch_monitors[mon_key] = evid

        return self.subscribe(sig='monitor', chid=chid, func=func,
                              oneshot=False)

    def unsubscribe(self, cid):
        with self._sub_lock:
            self._cbreg.unsubscribe(cid)

    def _queue_loop(self, q):
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

    def _event_queue_loop(self):
        loop = self._loop
        for event_type, info in self._queue_loop(self._event_queue):
            with self._sub_lock:
                chid = info.pop('chid')
                pvname = ca.name(chid)
                self.channel_to_pv[chid] = pvname
                loop.call_soon_threadsafe(partial(self._process_cb,
                                                  event_type, chid, **info))

                # TODO need to further differentiate monitor callbacks based on
                # use_time, use_ctrl, etc.
                # may be possible to send more info than requested, but never
                # less

    def _process_cb(self, sig, chid, **info):
        with self._sub_lock:
            print('*** cb processing', sig, chid, info)
            self._cbreg.process(sig, chid, **info)

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
                       loop.run_in_executor(None, self._event_queue_loop),
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
        for ctx_id, context in list(self.contexts.items()):
            context.stop()
            del self.contexts[ctx_id]

        # old finalize ca had multiple flush and wait...
        ca.flush_io()
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


def _make_callback(func, args):
    """ make callback function"""
    if utils.PY64_WINDOWS:
        # note that ctypes.POINTER is needed for 64-bit Python on Windows
        args = ctypes.POINTER(args)

        @wraps(func)
        def wrapped(args):
            # for 64-bit python on Windows!
            return func(args.contents)
        return ctypes.CFUNCTYPE(None, args)(wrapped)
    else:
        return ctypes.CFUNCTYPE(None, args)(func)


def ca_callback_event(fcn):
    fcn.ca_callback = _make_callback(fcn, dbr.event_handler_args)
    return fcn


def ca_connection_callback(fcn):
    fcn.ca_callback = _make_callback(fcn, dbr.connection_args)
    return fcn


@ca_connection_callback
def _on_connection_event(args):
    global _cm
    info = dict(chid=int(args.chid),
                connected=(args.op == dbr.OP_CONN_UP)
                )
    _cm.add_event(ca.current_context(), 'connection', info)


@ca_callback_event
def _on_monitor_event(args):
    global _cm
    info = cast.cast_monitor_args(args)
    _cm.add_event(ca.current_context(), 'monitor', info)


@ca_callback_event
def _on_get_event(args):
    """get_callback event: simply store data contents which will need
    conversion to python data with _unpack()."""
    future = args.usr
    future.ca_callback_done()

    # print("GET EVENT: chid, user ", args.chid, future, hash(future))
    # print("           type, count ", args.type, args.count)
    # print("           status ", args.status, dbr.ECA_NORMAL)

    if future.done():
        print('getevent: hmm, future already done', future, id(future))
        return

    if future.cancelled():
        print('future was cancelled', future)
        return

    if args.status != dbr.ECA_NORMAL:
        # TODO look up in appdev manual
        ex = errors.CASeverityException('get', str(args.status))
        loop.call_soon_threadsafe(future.set_exception, ex)
    else:
        loop.call_soon_threadsafe(future.set_result,
                                  copy.deepcopy(dbr.cast_args(args)))

    # TODO
    # ctypes.pythonapi.Py_DecRef(args.usr)


@ca_callback_event
def _on_put_event(args, **kwds):
    """set put-has-completed for this channel"""
    future = args.usr
    future.ca_callback_done()

    if future.done():
        print('putevent: hmm, future already done', future, id(future))
    elif not future.cancelled():
        print('finishing put event')
        loop.call_soon_threadsafe(future.set_result, True)

    # ctypes.pythonapi.Py_DecRef(args.usr)
