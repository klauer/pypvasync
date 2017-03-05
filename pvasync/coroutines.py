import asyncio
import ctypes
import ctypes.util

from math import log10
from functools import partial

from . import config
from . import context

_pending_futures = {}
loop = asyncio.get_event_loop()


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


async def get_precision(chid):
    """return the precision of a Channel."""
    if ca.field_type(chid) not in dbr.native_float_types:
        raise ValueError('Not a floating point type')

    info = await get_ctrlvars(chid)
    return info.get('precision', 0)


async def caput(pvname, value, *, timeout=60):
    """Put to a pv's value.

    >>> def coroutine():
    ...     yield from caput('xx.VAL', 3.0)
    ...     print('put done')
    """
    from .pv import get_pv
    thispv = await get_pv(pvname, connect=True)
    if not thispv.connected:
        raise asyncio.TimeoutError()

    ret = await thispv.aput(value, timeout=timeout)
    return ret


async def caget(pvname, *, as_string=False, count=None, as_numpy=True,
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
    thispv = await get_pv(pvname, connect=True)
    if not thispv.connected:
        raise asyncio.TimeoutError()

    if as_string:
        await thispv.get_ctrlvars()
    val = await thispv.aget(count=count, timeout=timeout,
                            use_monitor=use_monitor, as_string=as_string,
                            as_numpy=as_numpy)
    return val


async def cainfo(pvname):
    """cainfo(pvname)

    return printable information about pv
       >>>cainfo('xx.VAL')

    will return a status report for the pv.
    """
    from .pv import get_pv
    thispv = await get_pv(pvname, connect=True)
    if not thispv.connected:
        raise asyncio.TimeoutError()

    await thispv.aget()
    await thispv.get_ctrlvars()
    return thispv.info


@asyncio.coroutine
def caget_many(pvlist):
    """# TODO unimplemented

    get values for a list of PVs
    This does not maintain PV objects, and works as fast
    as possible to fetch many values.
    """
    raise NotImplementedError()
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
