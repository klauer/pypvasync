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


@asyncio.coroutine
def _as_string(val, chid, count, ftype):
    '''primitive conversion of value to a string

    This is a coroutine since it may hit channel access to get the enum string
    '''
    if (ftype in dbr.char_types and count < config.AUTOMONITOR_MAXLENGTH):
        val = ''.join(chr(i) for i in val if i > 0).rstrip()
    elif ftype == dbr.ChannelType.ENUM and count == 1:
        val = yield from get_enum_strings(chid)[val]
    elif count > 1:
        val = '<array count=%d, type=%d>' % (count, ftype)

    val = str(val)
    return val


@withConnectedCHID
@asyncio.coroutine
def get(chid, ftype=None, count=None, timeout=None, as_string=False,
        as_numpy=True):
    """return the current value for a Channel.
    Note that there is not a separate form for array data.

    Parameters
    ----------
    chid :  ctypes.c_long
       Channel ID
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
    wait : bool
        whether to wait for the data to be received, or return immediately.
    timeout : float
        maximum time to wait for data before returning ``None``.

    Returns
    -------
    data : object
       Normally, the value of the data.  Will return ``None`` if the
       channel is not connected, `wait=False` was used, or the data
       transfer timed out.

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

    4. The *wait* option controls whether to wait for the data to be
    received over the network and actually return the value, or to return
    immediately after asking for it to be sent.  If `wait=False` (that is,
    immediate return), the *get* operation is said to be *incomplete*.  The
    data will be still be received (unless the channel is disconnected)
    eventually but stored internally, and can be read later with
    :func:`get_complete`.  Using `wait=False` can be useful in some
    circumstances.

    5. The *timeout* option sets the maximum time to wait for the data to
    be received over the network before returning ``None``.  Such a timeout
    could imply that the channel is disconnected or that the data size is
    larger or network slower than normal.  In that case, the *get*
    operation is said to be *incomplete*, and the data may become available
    later with :func:`get_complete`.

    """

    if ftype is None:
        ftype = field_type(chid)
    if ftype in (None, -1):
        return None
    if count is None:
        count = 0
        # count = element_count(chid)
        # don't default to the element_count here - let EPICS tell us the size
        # in the _onGetEvent callback
    else:
        count = min(count, ca.element_count(chid))

    future = CAFuture()
    ret = ca.libca.ca_array_get_callback(ftype, count, chid,
                                         context._on_get_event.ca_callback,
                                         future.py_object)
    PySEVCHK('get', ret)

    if timeout is None:
        timeout = 1.0 + log10(max(1, count))

    try:
        data = yield from asyncio.wait_for(future, timeout=timeout)
    except asyncio.TimeoutError:
        future.cancel()
        raise

    promoted_data, ntype_array = data
    unpacked = cast.unpack(chid, ntype_array, count=count, ftype=ftype,
                           as_numpy=as_numpy)

    if as_string:
        try:
            unpacked = yield from _as_string(unpacked, chid, count, ftype)
        except ValueError:
            pass
    elif isinstance(unpacked, ctypes.Array) and as_numpy:
        return cast.to_numpy_array(unpacked, count=len(unpacked),
                                   ntype=dbr.native_type(ftype))

    return unpacked


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
def get_ctrlvars(chid, timeout=5.0, warn=True):
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

    out = {}
    for attr in ('precision', 'units', 'severity', 'status',
                 'upper_disp_limit', 'lower_disp_limit',
                 'upper_alarm_limit', 'upper_warning_limit',
                 'lower_warning_limit', 'lower_alarm_limit',
                 'upper_ctrl_limit', 'lower_ctrl_limit'):
        if hasattr(ctrl_val, attr):
            out[attr] = getattr(ctrl_val, attr, None)
            if attr == 'units':
                out[attr] = ctrl_val.units.decode('ascii')

    if (hasattr(ctrl_val, 'strs') and hasattr(ctrl_val, 'no_str') and
            ctrl_val.no_str > 0):
        out['enum_strs'] = tuple([ctrl_val.strs[i].value.decode('ascii')
                                  for i in range(ctrl_val.no_str)])
    return out


@ca.withConnectedCHID
@asyncio.coroutine
def get_timevars(chid, timeout=5.0, warn=True):
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
        value = yield from asyncio.wait_for(future, timeout=timeout)
    except asyncio.TimeoutError:
        future.cancel()
        raise

    if not isinstance(value, dbr._stat_sev_ts):
        raise RuntimeError('Got back a non-stat-severity-timestamp struct. '
                           'Type: {}'.format(type(value)))

    return dict(status=value.status,
                severity=value.severity,
                timestamp=value.stamp.unixtime,
                )


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
def get_enum_strings(chid):
    """return list of names for ENUM states of a Channel.  Returns
    None for non-ENUM Channels"""
    if ca.field_type(chid) != dbr.ChannelType.ENUM:
        raise ValueError('Not an enum type')

    info = yield from get_ctrlvars(chid)
    return info.get('enum_strs', None)


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

    ret = yield from thispv.put(value, timeout=timeout)
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
        thispv.get_ctrlvars()
    val = yield from thispv.get(count=count, timeout=timeout,
                                use_monitor=use_monitor,
                                as_string=as_string, as_numpy=as_numpy)
    # poll()
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

    yield from thispv.get()
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
