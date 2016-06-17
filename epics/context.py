import time
import asyncio
import logging
import atexit
import queue
import functools
import threading
import ctypes
import copy
from functools import partial

from . import ca
from . import dbr
from . import utils
from . import cast
from . import errors
from .callback_registry import (ChannelCallbackRegistry, ChannelCallbackBase,
                                _locked as _cb_locked)

logger = logging.getLogger(__name__)
loop = asyncio.get_event_loop()


class ConnectionCallback(ChannelCallbackBase):
    sig = 'connection'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # store the previously run callback status here:
        self.status = None

    def __ge__(self, other):
        if self.chid != other.chid:
            raise TypeError('Cannot compare subscriptions from different '
                            'channels')

        return True

    @_cb_locked
    def add_callback(self, cbid, func, *, oneshot=False):
        # since callbacks are locked on the context throughout this, it's only
        # necessary to check if it's connected prior to adding the callback. if
        # a connection callback comes in somewhere during the process of adding
        # the callback, it will be queued and eventually run after this method
        # releases the lock
        if self.status is not None and self.status['connected']:
            loop.call_soon_threadsafe(partial(func, chid=self.chid,
                                              connected=True,
                                              pvname=self.pvname))
            if oneshot:
                return

        super().add_callback(cbid, func, oneshot=oneshot)

    @_cb_locked
    def process(self, **kwargs):
        self.status = kwargs
        super().process(**kwargs)


class MonitorCallback(ChannelCallbackBase):
    '''Callback type 'monitor'

    Parameters
    ----------
    mask : int
        channel mask from dbr.SubscriptionType
        default is (DBE_VALUE | DBE_ALARM)
    ftype : int, optional
        Field type to request, maybe promoted from the native type
    '''
    # a monitor can be reused if:
    #   amask = available_mask / atype = available_type
    #   rmask = requested_mask / rtype = requested_type
    #   (amask & rmask) == rmask
    #   rtype == atype or rtype is native_type(atype)
    default_mask = (dbr.SubscriptionType.DBE_VALUE |
                    dbr.SubscriptionType.DBE_ALARM)
    sig = 'monitor'

    def __init__(self, registry, chid, *, mask=default_mask, ftype=None):
        super().__init__(registry=registry, chid=chid)

        if ftype is None:
            ftype = ca.field_type(chid)

        if mask is None:
            mask = self.default_mask
        elif mask <= 0:
            raise ValueError('Invalid subscription mask')

        self.mask = int(mask)
        self.ftype = int(ftype)
        self.native_type = dbr.native_type(self.ftype)
        self._hash_tuple = (self.chid, self.mask, self.ftype)

        # monitor information for when it's created:
        # python object referencing the callback id
        self.py_handler_id = None
        # event id returned from ca_create_subscription
        self.evid = None

    def create(self):
        logger.debug('Creating a subscription on %s (ftype=%s mask=%s)',
                     self.pvname, dbr.ChType(self.ftype).name, self.mask)
        self.evid = ctypes.c_void_p()
        ca_callback = _on_monitor_event.ca_callback
        self.py_handler_id = ctypes.py_object(self.handler_id)
        ret = ca.libca.ca_create_subscription(self.ftype, 0, self.chid,
                                              self.mask, ca_callback,
                                              self.py_handler_id,
                                              ctypes.byref(self.evid))
        ca.PySEVCHK('create_subscription', ret)

    def destroy(self):
        logger.debug('Clearing subscription on %s (ftype=%s mask=%s) evid=%s',
                     self.pvname, dbr.ChType(self.ftype).name, self.mask,
                     self.evid)
        super().destroy()
        import gc
        gc.collect()

        if self.py_handler_id is not None:
            pass
            # print('referrers:', )
            # for i, ref in enumerate(gc.get_referrers(self.py_handler_id)):
            #     info = str(ref)
            #     if hasattr(ref, 'f_code'):
            #         info = '[frame] {}'.format(ref.f_code.co_name)
            #     print(i, '\t', info)

        if self.evid is not None:
            ret = ca.clear_subscription(self.evid)
            ca.PySEVCHK('clear_subscription', ret)

            self.py_handler_id = None
            self.evid = None

    def __repr__(self):
        return ('{0.__class__.__name__}(chid={0.chid}, mask={0.mask:04b}, '
                'ftype={0.ftype})'.format(self))

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __hash__(self):
        return hash(self._hash_tuple)

    def __lt__(self, other):
        return not (self >= other)

    def __le__(self, other):
        return (self == other) or (self < other)

    def __ge__(self, other):
        if self.chid != other.chid:
            raise TypeError('Cannot compare subscriptions from different '
                            'channels')

        has_req_mask = (other.mask & self.mask) == other.mask
        type_ok = ((self.ftype == other.ftype) or
                   (self.native_type == other.ftype))
        return has_req_mask and type_ok


def _in_context(func):
    '''Ensure function is executed in the correct CA context'''
    @functools.wraps(func)
    def inner(self, *args, **kwargs):
        if ca.current_context() != self._ctx:
            ca.attach_context(self._ctx)
        return func(self, *args, **kwargs)

    return inner


class CAContextHandler:
    # Default mask for subscriptions (means update on value changes exceeding
    # MDEL, and on alarm level changes.) Other option is DBE_LOG for archive
    # changes (ie exceeding ADEL)
    default_mask = (dbr.SubscriptionType.DBE_VALUE |
                    dbr.SubscriptionType.DBE_ALARM)

    def __init__(self, ctx):
        self._sub_lock = threading.RLock()

        self._running = True
        self._ctx = ctx
        self._loop = asyncio.get_event_loop()
        self._tasks = None
        self._event_queue = queue.Queue()

        callback_classes = {'connection': ConnectionCallback,
                            'monitor': MonitorCallback
                            }
        self._cbreg = ChannelCallbackRegistry(self, callback_classes)
        self.channel_to_pv = {}
        self.pv_to_channel = {}
        self.ch_monitors = {}
        self.evid = {}

    def add_event(self, type_, info):
        self._event_queue.put((type_, info), block=False)

    def create_channel(self, pvname, *, callback=None):
        try:
            return self.pv_to_channel[pvname]
        except KeyError:
            pass

        chid = dbr.chid_t()
        ret = ca.libca.ca_create_channel(pvname.encode('ascii'),
                                         _on_connection_event.ca_callback,
                                         0, 0, ctypes.byref(chid))

        ca.PySEVCHK('create_channel', ret)

        chid = ca.channel_id_to_int(chid)

        self.channel_to_pv[chid] = pvname
        self.pv_to_channel[pvname] = chid

        if callback is not None:
            self.subscribe(sig='connection', chid=chid, func=callback,
                           oneshot=True)
        return chid

    def clear_channel(self, pvname):
        with self._sub_lock:
            chid = self.pv_to_channel.pop(pvname)
            del self.channel_to_pv[chid]

            # TODO: investigate segfault
            # ca.clear_channel(chid)

            try:
                handlers = list(self._cbreg.subscriptions_by_chid(chid))
            except KeyError:
                logger.debug('No handlers associated with chid')
            else:
                for sig, handler in handlers:
                    handler.destroy()

    def subscribe(self, sig, func, chid, *, oneshot=False, **kwargs):
        return self._cbreg.subscribe(sig=sig, chid=chid, func=func,
                                     oneshot=oneshot, **kwargs)

    def unsubscribe(self, cid):
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

        def connection_update(chid=None, pvname=None, connected=None):
            # print('connection update', chid, pvname, connected, fut)
            if fut.done():
                logger.debug('connect_channel but future already done')
                return

            if connected:
                loop.call_soon_threadsafe(fut.set_result, True)
            else:
                loop.call_soon_threadsafe(fut.set_exception,
                                          asyncio.TimeoutError('Disconnected'))

        self.subscribe(sig='connection', chid=chid, func=connection_update,
                       oneshot=True)

        yield from asyncio.wait_for(fut, timeout=timeout)

    def _event_queue_loop(self):
        loop = self._loop
        for event_type, info in self._queue_loop(self._event_queue):
            with self._sub_lock:
                chid = info.pop('chid')
                pvname = self.channel_to_pv[chid]
                loop.call_soon_threadsafe(partial(self._cbreg.process,
                                                  event_type, chid,
                                                  pvname=pvname,
                                                  **info))

    @_in_context
    def _poll_thread(self):
        '''Poll context ctx in an executor thread'''
        try:
            logger.debug('Event poll thread starting', self)
            while self._running:
                ca.pend_event(1.e-5)
                ca.pend_io(1.0)
        finally:
            ca.flush_io()
            ca.detach_context()
            logger.debug('%s event poll thread exiting', self)

    def start(self):
        loop = self._loop
        self._tasks = [loop.run_in_executor(None, self._poll_thread),
                       loop.run_in_executor(None, self._event_queue_loop),
                       ]

    @_in_context
    def stop(self):
        self._running = False

        if self._tasks:
            for task in self._tasks:
                logger.debug('Stopping task %s', task)
                self._loop.run_until_complete(task)
            del self._tasks[:]

        for chid, pvname in list(self.channel_to_pv.items()):
            logger.debug('Destroying channel %s (%d)', pvname, chid)
            self.clear_channel(pvname)

        ca.flush_io()
        ca.detach_context()

    def __del__(self):
        self.stop()


class CAContexts:
    def __init__(self):
        if hasattr(CAContexts, 'instance'):
            raise RuntimeError('CAContexts is a singleton')

        CAContexts.instance = self

        self.running = True
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
        self.running = False

        for ctx_id, context in list(self.contexts.items()):
            context.stop()
            del self.contexts[ctx_id]

        # # old finalize ca had multiple flush and wait...
        # ca.flush_io()
        # ca.detach_context()

    def add_event(self, ctx, event_type, info):
        if not self.running:
            return

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
    """Make a ctypes callback function

    Works a bit differently on Windows Python x64
    """
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
    '''Decorator which creates a ctypes callback function for events'''
    @functools.wraps(fcn)
    def wrapped(args):
        try:
            return fcn(args)
        except Exception as ex:
            logger.error('Exception in libca callback',
                         exc_info=ex)

    fcn.ca_callback = _make_callback(wrapped, dbr.EventHandlerArgs)
    return fcn


def ca_connection_callback(fcn):
    '''Decorator which creates a ctypes callback function for connections'''
    fcn.ca_callback = _make_callback(fcn, dbr.ConnectionArgs)
    return fcn


@ca_connection_callback
def _on_connection_event(args):
    global _cm
    _cm.add_event(ca.current_context(), 'connection',
                  args.to_dict())


@ca_callback_event
def _on_monitor_event(args):
    global _cm

    ctx = ca.current_context()
    args = cast.cast_monitor_args(args)
    _cm.add_event(ctx, 'monitor', args)


@ca_callback_event
def _on_get_event(args):
    """get_callback event: simply store data contents which will need
    conversion to python data with _unpack()."""
    future = args.usr
    future.ca_callback_done()

    # print("GET EVENT: chid, user ", args.chid, future, hash(future))
    # print("           type, count ", args.type, args.count)
    # print("           status ", args.status, dbr.ECA.NORMAL)

    if future.done() or future.cancelled():
        pass
    elif args.status != dbr.ECA.NORMAL:
        # TODO look up in appdev manual
        ex = errors.CASeverityException('get', str(args.status))
        loop.call_soon_threadsafe(future.set_exception, ex)
    else:
        data = copy.deepcopy(cast.cast_args(args))
        loop.call_soon_threadsafe(future.set_result, data)

    # TODO
    # ctypes.pythonapi.Py_DecRef(args.usr)


@ca_callback_event
def _on_put_event(args, **kwds):
    """set put-has-completed for this channel"""
    future = args.usr
    future.ca_callback_done()

    if future.done():
        pass
    elif not future.cancelled():
        loop.call_soon_threadsafe(future.set_result, True)

    # ctypes.pythonapi.Py_DecRef(args.usr)
