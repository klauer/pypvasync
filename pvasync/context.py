import asyncio
import logging
import queue
import functools
import threading
import ctypes
import copy
from functools import partial

import caproto

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
        return
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
        # import gc
        # gc.collect()

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


class VcProtocol(asyncio.Protocol):
    transport = None

    def __init__(self, avc):
        self.avc = avc
        self.vc = avc._vc

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        print("Received:", data)
        try:
            self.vc.recv(data)
            cmd = None
            while cmd is not caproto.NEED_DATA:
                if cmd is not None:
                    self.avc._received_ca_command(cmd)

                cmd = self.vc.next_command()
        except Exception as ex:
            print('receive fail', ex)
            self.avc._ca_failure(ex)
            raise

    def connection_lost(self, exc):
        # The socket has been closed, stop the event loop
        loop.stop()


class AsyncClientChannel(caproto.ClientChannel):
    pass
    # def state_changed(self, role, old_state, new_state, command=None):
    #     if role is caproto.SERVER:
    #         return

    #     print('state changed', role, old_state, new_state,
    #           'command was', command)
    #     if new_state is caproto.CONNECTED:
    #         print('connected! data type is', self.native_data_type,
    #               'count', self.native_data_count)


class AsyncVirtualCircuit:
    # Default mask for subscriptions (means update on value changes exceeding
    # MDEL, and on alarm level changes.) Other option is DBE_LOG for archive
    # changes (ie exceeding ADEL)
    default_mask = (dbr.SubscriptionType.DBE_VALUE |
                    dbr.SubscriptionType.DBE_ALARM)

    def __init__(self, hub, host, port, priority=None, loop=None):
        self._sock = None
        self._sub_lock = threading.RLock()
        self._connected_event = asyncio.Event()
        self._hub = hub
        self._host = host
        self._port = port
        self._priority = priority

        self._running = True
        if loop is None:
            loop = asyncio.get_event_loop()
        self._loop = loop
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

        self._vc = caproto.VirtualCircuit(hub=hub,
                                          address=(self._host, self._port),
                                          priority=self._priority)
        self._read_queue = asyncio.Queue()
        self._write_queue = asyncio.Queue()
        self._write_event = threading.Event()

        # TODO need to handle unsolicited version requests better
        # self._write_request(
        #     caproto.VersionRequest(version=self._hub.protocol_version,
        #                            priority=self._priority))
        self._write_request(caproto.HostNameRequest(caproto.OUR_HOSTNAME[:40]))
        self._write_request(caproto.ClientNameRequest(caproto.OUR_USERNAME))

        self._futures = [
            asyncio.run_coroutine_threadsafe(
                self._connection_context(host, port), self._loop),
            ]

    def _write_request(self, command):
        with self._sub_lock:
            if not isinstance(command, (tuple, list)):
                command = [command]

            self._write_queue.put_nowait(tuple(command))

    def _received_ca_command(self, event):
        if not self._connected_event.is_set():
            if self._vc._state.states[caproto.CLIENT] is caproto.CONNECTED:
                self._connected_event.set()

        print('received ca event', event)
        self._read_queue.put_nowait(event)

    def _ca_failure(self, ex):
        print('!! caproto state failure', ex)

    async def _connection_context(self, host, port):
        loop = asyncio.get_event_loop()
        proto, sock = await loop.create_connection(lambda: VcProtocol(self),
                                                   host, port)

        self._proto = proto
        self._sock = sock

        await self._connected_event.wait()

        while True:
            try:
                data = await self._write_queue.get()
                print('[writequeue]', data)
                to_send = self._vc.send(*data)
                sock.transport.write(to_send)
            except caproto.ChannelAccessProtocolError as ex:
                print('channel access protocol error, trying to continue', ex)
            except RuntimeError as ex:
                if loop.is_closed():
                    break
                print('write queue failed', type(ex), ex)
                break
            except Exception as ex:
                print('write queue failed', type(ex), ex)
                break

    def add_event(self, type_, info):
        self._event_queue.put((type_, info), block=False)

    def create_channel(self, pvname, *, callback=None,
                       channel_class=AsyncClientChannel,
                       **ch_kwargs):
        try:
            return self.pv_to_channel[pvname]
        except KeyError:
            pass

        with self._sub_lock:
            channel = channel_class(hub=self._hub, name=pvname,
                                    address=(self._host, self._port),
                                    priority=self._priority,
                                    **ch_kwargs)
            chid = channel.cid
            self._vc.channels[chid] = channel

            self.channel_to_pv[chid] = pvname
            self.pv_to_channel[pvname] = channel

            self._write_request(channel.create()[1:])

            if callback is not None:
                self.subscribe(sig='connection', chid=chid, func=callback,
                               oneshot=True)

        return channel

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
        while self._running:
            try:
                yield q.get(block=True, timeout=0.1)
            except queue.Empty:
                pass

    async def connect_channel(self, chid, timeout=1.0):
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

        await asyncio.wait_for(fut, timeout=timeout)

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

    def start(self):
        loop = self._loop
        self._tasks = [loop.run_in_executor(None, self._event_queue_loop),
                       ]

    def stop(self):
        self._running = False

        if self._tasks:
            for task in self._tasks:
                logger.debug('Stopping task %s', task)
                if not self._loop.is_running():
                    self._loop.run_until_complete(task)
            del self._tasks[:]

        for chid, pvname in list(self.channel_to_pv.items()):
            logger.debug('Destroying channel %s (%d)', pvname, chid)
            self.clear_channel(pvname)

        # ca.flush_io()
        # ca.detach_context()

    def __del__(self):
        self.stop()


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


client_hub = caproto.Hub(our_role=caproto.CLIENT)
context = AsyncVirtualCircuit(client_hub, '127.0.0.1', 5064, priority=0)

def get_current_context():
    return context
