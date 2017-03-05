import asyncio
import logging
import queue
import threading
import ctypes
from functools import partial
from math import log10

import caproto
from caproto import _dbr as dbr

from . import config
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
            ftype = self.channel.native_data_type

        if mask is None:
            mask = self.default_mask
        elif mask <= 0:
            raise ValueError('Invalid subscription mask')

        self.mask = int(mask)
        self.ftype = int(ftype)
        self.native_type = dbr.native_type(self.ftype)
        self._hash_tuple = (self.chid, self.mask, self.ftype)

        # event id returned from ca_create_subscription
        self.evid = None

    def create(self):
        logger.debug('Creating a subscription on %s (ftype=%s mask=%s)',
                     self.pvname, dbr.ChType(self.ftype).name, self.mask)

        self.evid = ctypes.c_void_p()
        # ->_on_monitor_event
        context = self.context
        context._write_request(
            self.channel.subscribe(data_type=self.ftype, data_count=0,
                                   mask=self.mask)[1:]
        )

    def destroy(self):
        logger.debug('Clearing subscription on %s (ftype=%s mask=%s) evid=%s',
                     self.pvname, dbr.ChType(self.ftype).name, self.mask,
                     self.evid)
        super().destroy()
        # import gc
        # gc.collect()

        if self.evid is not None:
            ret = clear_subscription(self.evid)

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
            print('receive fail', type(ex), ex)
            self.avc._ca_failure(ex)
            raise

    def connection_lost(self, exc):
        # The socket has been closed, stop the event loop
        loop.stop()


class AsyncClientChannel(caproto.ClientChannel):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.connected = False
        self.access_rights = None
        self._ioid_to_future = {}
        # TODO this is confusing to have two
        self.async_vc = self.circuit.async_vc

    def state_changed(self, role, old_state, new_state, command=None):
        super().state_changed(role, old_state, new_state, command=command)

        if role is caproto.SERVER:
            return

        print(old_state, '->', new_state, type(command))
        if new_state is caproto.CONNECTED:
            self.connected = True
        elif new_state is caproto.MUST_CLOSE:
            self.connected = False

    def _process_command(self, command):
        super()._process_command(command)

        print('--CH-- process', command)
        if isinstance(command, caproto.AccessRightsResponse):
            self.access_rights = command.access_rights
        elif isinstance(command, caproto.ReadNotifyResponse):
            # ReadNotifyResponse(values=<caproto._dbr.DBR_DOUBLE object at
            # 0x109c0a510>, data_type=6, data_count=1, status=1, ioid=0)
            try:
                future = self._ioid_to_future[command.ioid]
            except KeyError:
                print('cancelled io request')
            else:
                future.set_result(command)

    def _init_io_operation(self):
        ioid = self.circuit.new_ioid()
        future = asyncio.Future()
        self._ioid_to_future[ioid] = future
        return ioid, future

    async def _wait_io_operation(self, ioid, future, timeout):
        try:
            data = await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            future.cancel()
            raise
        finally:
            del self._ioid_to_future[ioid]
        return data

    async def get_ctrlvars(self, timeout=5.0):
        """return the CTRL fields for a Channel.

        Depending on the native type, the keys may include
            *status*, *severity*, *precision*, *units*, enum_strs*,
            *upper_disp_limit*, *lower_disp_limit*, upper_alarm_limit*,
            *lower_alarm_limit*, *upper_warning_limit*, *lower_warning_limit*,
            *upper_ctrl_limit*, *lower_ctrl_limit*

        Notes
        -----
        enum_strs will be a list of strings for the names of ENUM states.

        """
        ftype = dbr.promote_type(self.native_data_type, use_ctrl=True)
        info = await self.get(ftype=ftype, count=1, timeout=timeout)
        return info._asdict()

    async def get_timevars(self, timeout=5.0):
        """returns a dictionary of TIME fields for a Channel.
        This will contain keys of  *status*, *severity*, and *timestamp*.
        """
        ftype = dbr.promote_type(self.native_data_type, use_time=True)
        info = await self.get(ftype=ftype, count=1, timeout=timeout)
        return info._asdict()

    async def get(self, ftype=None, count=None, timeout=None, as_string=False,
                  as_numpy=True):
        """Return the current value for a channel.
        Note that there is not a separate form for array data.

        Parameters
        ----------
        ftype : int
           field type to use (native type is default)
        count : int
           maximum element count to return (full data returned by default)
        as_string : bool
           whether to return the string representation of the value.
           See notes below.
        as_numpy : bool
           whether to return the Numerical Python representation
           for array / waveform data.
        timeout : float
            maximum time to wait for data before returning ``None``.

        Returns
        -------
        data : object

        Notes
        -----
        1. Returning ``None`` indicates an *incomplete get*

        2. The *as_string* option is not as complete as the *as_string*
        argument for :meth:`PV.get`.  For Enum types, the name of the Enum
        state will be returned.  For waveforms of type CHAR, the string
        representation will be returned.  For other waveforms (with *count* >
        1), a string like `<array count=3, type=1>` will be returned.

        3. The *as_numpy* option will convert waveform data to be returned as a
        numpy array.  This is only applied if numpy can be imported.

        4. The *timeout* option sets the maximum time to wait for the data to
        be received over the network before returning ``None``.  Such a timeout
        could imply that the channel is disconnected or that the data size is
        larger or network slower than normal.  In that case, the *get*
        operation is said to be *incomplete*, and the data may become available
        later with :func:`get_complete`.

        """
        if count is not None:
            count = min(count, self.native_data_count)

        ftype, count = self._fill_defaults(ftype, count)
        ioid, future = self._init_io_operation()

        command = caproto.ReadNotifyRequest(ftype, count, self.sid, ioid)
        self.async_vc._write_request(command)

        if timeout is None:
            timeout = 1.0 + log10(max(1, count))

        data = await self._wait_io_operation(ioid, future, timeout)

        unpacked = data.values

        if as_string:
            try:
                unpacked = await self._as_string(unpacked, count, ftype)
            except ValueError:
                pass

        return unpacked

    async def get_enum_strings(self):
        """return list of names for ENUM states of a Channel.  Returns
        None for non-ENUM Channels"""
        if (caproto.native_type[self.native_data_type] not in
                caproto.enum_types):
            raise ValueError('Not an enum type')

        info = await self.get_ctrlvars()
        return info.get('enum_strs', None)

    async def _as_string(self, val, count, ftype):
        '''primitive conversion of value to a string

        This is a coroutine since it may hit channel access to get the enum
        string
        '''
        if (ftype in caproto.char_types
                and count < config.AUTOMONITOR_MAXLENGTH):
            val = ''.join(chr(i) for i in val if i > 0).rstrip()
        elif ftype == ChannelType.ENUM and count == 1:
            val = await self.get_enum_strings()[val]
        elif count > 1:
            val = '<array count=%d, type=%d>' % (count, ftype)

        val = str(val)
        return val


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

        # TODO i think this has to become a subclass?
        self._vc = caproto.VirtualCircuit(hub=hub,
                                          address=(self._host, self._port),
                                          priority=self._priority)
        # create reference for channel usage
        self._vc.async_vc = self

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

    def __del__(self):
        self.stop()


client_hub = caproto.Hub(our_role=caproto.CLIENT)
circuit_addr = ('127.0.0.1', 5064)
context = AsyncVirtualCircuit(client_hub, *circuit_addr, priority=0)

# TODO: vc doesn't get registered until a versionreply, meaning PVs can't be created...
client_hub.circuits[(circuit_addr, 0)] = context._vc

def get_current_context():
    return context
