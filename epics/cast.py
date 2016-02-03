import ctypes
import ctypes.util

from copy import deepcopy
from .utils import (BYTES2STR, NULLCHAR, NULLCHAR_2, strjoin, is_string,
                    is_string_or_bytes, ascii_string)

import numpy
from . import dbr
from . import config
from .dbr import native_type
from .ca import (element_count, field_type, withConnectedCHID)
from .errors import ChannelAccessException
from .dbr import ChannelType as ChType


def scan_string(data, count):
    """ Scan a string, or an array of strings as a list, depending on
    content """
    out = []
    for elem in range(min(count, len(data))):
        this = strjoin('', BYTES2STR(data[elem].value)).rstrip()
        if NULLCHAR_2 in this:
            this = this[:this.index(NULLCHAR_2)]
        out.append(this)
    if len(out) == 1:
        out = out[0]
    return out


def array_cast(data, count, ntype, use_numpy):
    "cast ctypes array to numpy array (if using numpy)"
    if use_numpy:
        dtype = dbr.NumpyMap.get(ntype, None)
        if dtype is not None:
            out = numpy.empty(shape=(count,), dtype=dbr.NumpyMap[ntype])
            ctypes.memmove(out.ctypes.data, data, out.nbytes)
        else:
            out = numpy.ctypeslib.as_array(deepcopy(data))
    else:
        out = deepcopy(data)
    return out


def unpack_simple(data, count, ntype, use_numpy):
    "simple, native data type"
    if data is None:
        return None
    elif count == 1 and ntype != ChType.STRING:
        return data[0]
    elif ntype == ChType.STRING:
        return scan_string(data, count)
    elif count > 1:
        return array_cast(data, count, ntype, use_numpy)
    return data


def unpack(chid, data, count=None, ftype=None, as_numpy=True):
    """unpacks raw data for a Channel ID `chid` returned by libca functions
    including `ca_get_array_callback` or subscription callback, and returns the
    corresponding Python data

    Parameters
    ------------
    chid  :  ctypes.c_long or ``None``
        channel ID (if not None, used for determining count and ftype)
    data  :  object
        raw data as returned by internal libca functions.
    count :  integer
        number of elements to fetch (defaults to element count of chid  or 1)
    ftype :  integer
        data type of channel (defaults to native type of chid)
    as_numpy : bool
        whether to convert to numpy array.
    """


    # Grab the native-data-type data
    try:
        data = data[1]
    except (TypeError, IndexError):
        return None

    if count is None and chid is not None:
        count = element_count(chid)
    if count is None:
        count = 1

    if ftype is None and chid is not None:
        ftype = field_type(chid)
    if ftype is None:
        ftype = ChType.INT
    ntype = native_type(ftype)
    use_numpy = (as_numpy and ntype != ChType.STRING and count > 1)
    return unpack_simple(data, count, ntype, use_numpy)


@withConnectedCHID
def get_put_info(chid, value):
    ftype = field_type(chid)
    count = nativecount = element_count(chid)
    if count > 1:
        # check that data for array PVS is a list, array, or string
        try:
            count = min(len(value), count)
            if count == 0:
                count = nativecount
        except TypeError:
            print('''PyEpics Warning:
     value put() to array PV must be an array or sequence''')
    if (ftype == ChType.CHAR and nativecount > 1 and
            is_string_or_bytes(value)):
        count += 1

    # if needed (python3, especially) convert to basic string/bytes form
    if is_string(value):
        if value == '':
            value = NULLCHAR
        value = ascii_string(value)

    data = (count * dbr.Map[ftype])()
    if ftype == ChType.STRING:
        if count == 1:
            data[0].value = value
        else:
            for elem in range(min(count, len(value))):
                data[elem].value = value[elem]
    elif nativecount == 1:
        if ftype == ChType.CHAR:
            if is_string_or_bytes(value):
                if isinstance(value, bytes):
                    value = value.decode('ascii', 'replace')
                value = [ord(i) for i in value] + [0, ]
            else:
                data[0] = value
        else:
            # allow strings (even bits/hex) to be put to integer types
            if is_string(value) and isinstance(data[0], (int, )):
                value = int(value, base=0)
            try:
                data[0] = value
            except TypeError:
                data[0] = type(data[0])(value)
            except:
                errmsg = "cannot put value '%s' to PV of type '%s'"
                tname = dbr.Name(ftype).lower()
                raise ChannelAccessException(errmsg % (repr(value), tname))

    else:
        if ftype == ChType.CHAR and is_string_or_bytes(value):
            if isinstance(value, bytes):
                value = value.decode('ascii', 'replace')
            value = [ord(i) for i in value] + [0, ]
        try:
            ndata, nuser = len(data), len(value)
            if nuser > ndata:
                value = value[:ndata]
            data[:nuser] = list(value)

        except (ValueError, IndexError):
            errmsg = "cannot put array data to PV of type '%s'"
            raise ChannelAccessException(errmsg % (repr(value)))

    return ftype, count, data


def cast_monitor_args(args):
    """Event Handler for monitor events: not intended for use"""

    value = dbr.cast_args(args)
    kwds = {'ftype': args.type, 'count': args.count, 'chid': args.chid,
            'status': args.status}

    # add kwds arguments for CTRL and TIME variants
    # this is in a try/except clause to avoid problems
    # caused by uninitialized waveform arrays
    if args.type >= ChType.CTRL_STRING:
        try:
            tmpv = value[0]
            for attr in dbr.ctrl_limits + ('precision', 'units', 'severity'):
                if hasattr(tmpv, attr):
                    kwds[attr] = getattr(tmpv, attr)
                    if attr == 'units':
                        kwds[attr] = BYTES2STR(getattr(tmpv, attr, None))

            if (hasattr(tmpv, 'strs') and hasattr(tmpv, 'no_str') and
                    tmpv.no_str > 0):
                kwds['enum_strs'] = tuple([tmpv.strs[i].value for
                                           i in range(tmpv.no_str)])
        except IndexError:
            pass
    elif args.type >= ChType.TIME_STRING:
        try:
            tmpv = value[0]
            kwds['status'] = tmpv.status
            kwds['severity'] = tmpv.severity
            kwds['timestamp'] = dbr.make_unixtime(tmpv.stamp)
        except IndexError:
            pass

    value = unpack(args.chid, value, count=args.count, ftype=args.type)
    kwds['value'] = value
    return kwds


