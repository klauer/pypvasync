import ctypes
import ctypes.util

import numpy
from . import dbr
from .dbr import native_type
from .ca import (element_count, field_type, withConnectedCHID)
from .errors import ChannelAccessException
from .dbr import ChannelType
from .utils import decode_bytes


def scan_string(data, count):
    """Scan a string, or an array of strings as a list, depending on content"""
    count = min(count, len(data))

    if count == 1:
        return decode_bytes(data[0].value)

    return [decode_bytes(data[elem].value)
            for elem in range(count)]


def to_numpy_array(data, count, ntype):
    "cast ctypes array to numpy array (if using numpy)"
    try:
        dtype = dbr._numpy_map[ntype]
    except KeyError:
        return numpy.ctypeslib.as_array(data)
    else:
        ret = numpy.empty(shape=(count, ), dtype=dtype)
        ctypes.memmove(ret.ctypes.data, data, ret.nbytes)
        return ret


def unpack_simple(data, count, ntype, use_numpy):
    "simple, native data type"
    if count == 1 and ntype != ChannelType.STRING:
        return data[0]
    elif ntype == ChannelType.STRING:
        return scan_string(data, count)
    elif count != 1 and use_numpy:
        return to_numpy_array(data, count, ntype)
    return data


def unpack(chid, data, count=None, ftype=None, as_numpy=True):
    """unpacks raw data for a Channel ID `chid` returned by libca functions
    including `ca_get_array_callback` or subscription callback, and returns the
    corresponding Python data

    Parameters
    ------------
    chid  :  ctypes.c_long or ``None``
        channel ID (if not None, used for determining count and ftype)
    data  :  ctypes.array
        the ctypes array of native-typed elements
    count :  integer
        number of elements to fetch (defaults to element count of chid  or 1)
    ftype :  integer
        data type of channel (defaults to native type of chid)
    as_numpy : bool
        whether to convert to numpy array.
    """

    # Grab the native-data-type data
    if count is None or count == 0:
        count = len(data)
    else:
        count = min(len(data), count)

    if ftype is None and chid is not None:
        ftype = field_type(chid)
    if ftype is None:
        ftype = ChannelType.INT

    ntype = native_type(ftype)
    use_numpy = (as_numpy and ntype != ChannelType.STRING and count != 1)
    return unpack_simple(data, count, ntype, use_numpy)


def get_string_put_info(count, value, encoding='latin-1'):
    data = (count * dbr.string_t)()

    if count == 1:
        value = [value]

    for elem in range(count):
        bytes_ = value[elem].encode(encoding) + b'\0'

        if len(bytes_) > dbr.MAX_STRING_SIZE:
            raise ValueError('Value will not fit into an EPICS string '
                             ' (40 chars) with a null-terminator byte')

        data[elem].value = bytes_

    return ChannelType.STRING, count, data


@withConnectedCHID
def get_put_info(chid, value, encoding='latin-1'):
    ftype = field_type(chid)
    nativecount = element_count(chid)
    count = nativecount
    try:
        count = len(value)
    except TypeError:
        if nativecount > 1:
            raise ValueError('value put to array PV must be an array or '
                             'sequence')
        count = 1

    if ftype == ChannelType.STRING:
        return get_string_put_info(count, value, encoding=encoding)

    if isinstance(value, str):
        # if needed, encode to byte form
        value = value.encode(encoding)

        if nativecount > 1:
            value = value + b'\0'
            count += 1

            if count > nativecount:
                raise ValueError('Value will not fit into the given PV '
                                 'with a null-terminator byte')

    if count > nativecount:
        raise ValueError('PV can only hold {} values. '
                         'Attempting to put {} items.'
                         ''.format(nativecount, count))

    data = (count * dbr._ftype_to_ctype[ftype])()

    if count == 1:
        value = [value]

    try:
        data[:count] = value
    except ValueError:
        errmsg = ("cannot put array data to PV. Given: {} Expected: {}"
                  "".format(type(value), data))
        raise ChannelAccessException(errmsg)

    return ftype, count, data


def cast_monitor_args(event_args):
    """make a dictionary from monitor callback arguments"""
    promoted_val, nvalues = cast_args(event_args)
    value = unpack(event_args.chid, nvalues, count=event_args.count,
                   ftype=event_args.type)

    kwds = event_args.to_dict()
    kwds['value'] = value
    kwds['handler_id'] = event_args.usr

    if promoted_val is not None:
        # add kwds arguments for CTRL and TIME variants
        kwds.update(promoted_val.to_dict())
    return kwds


def cast_args(args):
    """returns casted array contents

    returns: [dbr_ctrl or dbr_time struct,
              count * native_type structs]

    If data is already of a native_type, the first value in the list will be
    None.
    """
    ftype = args.type
    ftype_c = dbr._ftype_to_ctype[ftype]
    ntype = native_type(ftype)
    if ftype != ntype:
        native_start = args.raw_dbr + dbr.value_offset[ftype]
        ntype_c = dbr._ftype_to_ctype[ntype]
        return [ctypes.cast(args.raw_dbr,
                            ctypes.POINTER(ftype_c)).contents,
                ctypes.cast(native_start,
                            ctypes.POINTER(args.count * ntype_c)).contents
                ]
    else:
        return [None,
                ctypes.cast(args.raw_dbr,
                            ctypes.POINTER(args.count * ftype_c)).contents
                ]
