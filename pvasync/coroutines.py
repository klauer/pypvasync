import asyncio
import ctypes
import ctypes.util

from math import log10
from functools import partial

from . import ca
from . import dbr
from . import config
from . import context
from . import cast
from .ca import (PySEVCHK, withConnectedCHID)

_pending_futures = {}
loop = asyncio.get_event_loop()


class CAFuture(asyncio.Future):
    def __init__(self):
        super().__init__()
        _pending_futures[self] = ctypes.py_object(self)

    @property
    def py_object(self):
        return _pending_futures[self]

    def ca_callback_done(self):
        del _pending_futures[self]
        # TODO GC will definitely be important... not sure about py_object ref
        # counting

        # import gc
        # gc.collect()

        # print('referrers:', )
        # for i, ref in enumerate(gc.get_referrers(self)):
        #     info = str(ref)
        #     if hasattr(ref, 'f_code'):
        #         info = '[frame] {}'.format(ref.f_code.co_name)
        #     print(i, '\t', info)


@withConnectedCHID
@asyncio.coroutine
def put(chid, value, timeout=30, callback=None, callback_data=None):
    """sets the Channel to a value, with options to either wait (block) for the
    processing to complete, or to execute a supplied callback function when the
    process has completed.

    Parameters
    ----------
    chid :  ctypes.c_long
        Channel ID
    timeout : float
        maximum time to wait for processing to complete before returning
        anyway.
    callback : ``None`` of callable
        user-supplied function to run when processing has completed.
    """

    ftype, count, data = cast.get_put_info(chid, value)
    future = CAFuture()
    if callable(callback):
        future.add_done_callback(partial(callback, data=callback_data))

    ret = ca.libca.ca_array_put_callback(ftype, count, chid, data,
                                         context._on_put_event.ca_callback,
                                         future.py_object)

    PySEVCHK('put', ret)

    try:
        ret = yield from asyncio.wait_for(future, timeout=timeout)
    except asyncio.TimeoutError:
        future.cancel()
        raise

    return ret


@withConnectedCHID
def get_ctrlvars(chid, timeout=5.0):
    """return the CTRL fields for a Channel.

    Depending on the native type, the keys may include
        *status*, *severity*, *precision*, *units*, enum_strs*,
        *upper_disp_limit*, *lower_disp_limit*, upper_alarm_limit*,
        *lower_alarm_limit*, upper_warning_limit*, *lower_warning_limit*,
        *upper_ctrl_limit*, *lower_ctrl_limit*

    Notes
    -----
    enum_strs will be a list of strings for the names of ENUM states.

    """
    global _cache

    future = CAFuture()
    ftype = dbr.promote_type(ca.field_type(chid), use_ctrl=True)

    ret = ca.libca.ca_array_get_callback(ftype, 1, chid,
                                         context._on_get_event.ca_callback,
                                         future.py_object)

    PySEVCHK('get_ctrlvars', ret)

    try:
        ctrl_val, nval = yield from asyncio.wait_for(future, timeout=timeout)
    except asyncio.TimeoutError:
        future.cancel()
        raise

    if not isinstance(ctrl_val, dbr.ControlTypeBase):
        raise RuntimeError('Got back a non-ControlType struct. '
                           'Type: {}'.format(type(ctrl_val)))

    return ctrl_val.to_dict()


@ca.withConnectedCHID
@asyncio.coroutine
def get_timevars(chid, timeout=5.0):
    """returns a dictionary of TIME fields for a Channel.
    This will contain keys of  *status*, *severity*, and *timestamp*.
    """
    global _cache
    future = CAFuture()
    ftype = dbr.promote_type(ca.field_type(chid), use_time=True)
    ret = ca.libca.ca_array_get_callback(ftype, 1, chid,
                                         context._on_get_event.ca_callback,
                                         future.py_object)

    PySEVCHK('get_timevars', ret)

    try:
        time_val, nvals = yield from asyncio.wait_for(future, timeout=timeout)
    except asyncio.TimeoutError:
        future.cancel()
        raise

    if not isinstance(time_val, dbr.TimeType):
        raise RuntimeError('Got back a non-TimeType struct. '
                           'Type: {}'.format(type(time_val)))

    return time_val.to_dict()


@asyncio.coroutine
def get_timestamp(chid):
    """return the timestamp of a Channel -- the time of last update."""
    info = yield from get_timevars(chid)
    return info['timestamp']


@asyncio.coroutine
def get_severity(chid):
    """return the severity of a Channel."""
    info = yield from get_timevars(chid)
    return info['severity']


@asyncio.coroutine
def get_precision(chid):
    """return the precision of a Channel."""
    if ca.field_type(chid) not in dbr.native_float_types:
        raise ValueError('Not a floating point type')

    info = yield from get_ctrlvars(chid)
    return info.get('precision', 0)


@asyncio.coroutine
def caput(pvname, value, *, timeout=60):
    """Put to a pv's value.

    >>> def coroutine():
    ...     yield from caput('xx.VAL', 3.0)
    ...     print('put done')
    """
    from .pv import get_pv
    thispv = yield from get_pv(pvname, connect=True)
    if not thispv.connected:
        raise asyncio.TimeoutError()

    ret = yield from thispv.aput(value, timeout=timeout)
    return ret


@asyncio.coroutine
def caget(pvname, *, as_string=False, count=None, as_numpy=True,
          use_monitor=False, timeout=None):
    """caget(pvname, as_string=False)
    simple get of a pv's value..
       >>> x = caget('xx.VAL')

    to get the character string representation (formatted double,
    enum string, etc):
       >>> x = caget('xx.VAL', as_string=True)

    to get a truncated amount of data from an array, you can specify
    the count with
       >>> x = caget('MyArray.VAL', count=1000)
    """
    from .pv import get_pv
    thispv = yield from get_pv(pvname, connect=True)
    if not thispv.connected:
        raise asyncio.TimeoutError()

    if as_string:
        yield from thispv.get_ctrlvars()
    val = yield from thispv.aget(count=count, timeout=timeout,
                                 use_monitor=use_monitor, as_string=as_string,
                                 as_numpy=as_numpy)
    return val


@asyncio.coroutine
def cainfo(pvname):
    """cainfo(pvname)

    return printable information about pv
       >>>cainfo('xx.VAL')

    will return a status report for the pv.
    """
    from .pv import get_pv
    thispv = yield from get_pv(pvname, connect=True)
    if not thispv.connected:
        raise asyncio.TimeoutError()

    yield from thispv.aget()
    yield from thispv.get_ctrlvars()
    return thispv.info


@asyncio.coroutine
def caget_many(pvlist):
    """# TODO unimplemented

    get values for a list of PVs
    This does not maintain PV objects, and works as fast
    as possible to fetch many values.
    """
    chids, out = [], []
    for name in pvlist:
        chids.append(ca.create_channel(name, auto_cb=False))
    for chid in chids:
        ca.connect_channel(chid)
    for chid in chids:
        get(chid, wait=False)
    for chid in chids:
        # out.append(get_complete(chid))
        # removed, necessary?
        pass
    return out
