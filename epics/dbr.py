#!/usr/bin/env python
#  M Newville <newville@cars.uchicago.edu>
#  The University of Chicago, 2010
#  Epics Open License
#
# Epics Database Records (DBR) Constants and Definitions
#  most of the code here is copied from db_access.h
#
""" constants and declaration of data types for Epics database records
This is mostly copied from CA header files
"""
import ctypes
import numpy as np
from enum import IntEnum

from .utils import PY64_WINDOWS


# EPICS Constants
class ECA(IntEnum):
    NORMAL = 1
    TIMEOUT = 80
    IODONE = 339
    ISATTACHED = 424
    BADCHID = 410


class ConnStatus(IntEnum):
    CS_CONN = 2
    OP_CONN_UP = 6
    OP_CONN_DOWN = 7
    CS_NEVER_SEARCH = 4


class ChannelType(IntEnum):
    STRING = 0
    INT = 1
    SHORT = 1
    FLOAT = 2
    ENUM = 3
    CHAR = 4
    LONG = 5
    DOUBLE = 6

    STS_STRING = 7
    STS_SHORT = 8
    STS_INT = 8
    STS_FLOAT = 9
    STS_ENUM = 10
    STS_CHAR = 11
    STS_LONG = 12
    STS_DOUBLE = 13

    TIME_STRING = 14
    TIME_INT = 15
    TIME_SHORT = 15
    TIME_FLOAT = 16
    TIME_ENUM = 17
    TIME_CHAR = 18
    TIME_LONG = 19
    TIME_DOUBLE = 20

    CTRL_STRING = 28
    CTRL_INT = 29
    CTRL_SHORT = 29
    CTRL_FLOAT = 30
    CTRL_ENUM = 31
    CTRL_CHAR = 32
    CTRL_LONG = 33
    CTRL_DOUBLE = 34


ChType = ChannelType


enum_types = (ChType.ENUM, ChType.STS_ENUM, ChType.TIME_ENUM, ChType.CTRL_ENUM)
status_types = (ChType.STS_STRING, ChType.STS_SHORT, ChType.STS_INT,
                ChType.STS_FLOAT, ChType.STS_ENUM, ChType.STS_CHAR,
                ChType.STS_LONG, ChType.STS_DOUBLE)

time_types = (ChType.TIME_STRING, ChType.TIME_INT, ChType.TIME_SHORT,
              ChType.TIME_FLOAT, ChType.TIME_ENUM, ChType.TIME_CHAR,
              ChType.TIME_LONG, ChType.TIME_DOUBLE)

control_types = (ChType.CTRL_STRING, ChType.CTRL_INT, ChType.CTRL_SHORT,
                 ChType.CTRL_FLOAT, ChType.CTRL_ENUM, ChType.CTRL_CHAR,
                 ChType.CTRL_LONG, ChType.CTRL_DOUBLE)
char_types = (ChType.CHAR, ChType.TIME_CHAR, ChType.CTRL_CHAR)
native_float_types = (ChType.FLOAT, ChType.DOUBLE)

MAX_STRING_SIZE = 40
MAX_UNITS_SIZE = 8
MAX_ENUM_STRING_SIZE = 26
MAX_ENUMS = 16

# EPICS2UNIX_EPOCH = 631173600.0 - time.timezone
EPICS2UNIX_EPOCH = 631152000.0


class SubscriptionEnum(IntEnum):
    # create_subscription mask constants
    DBE_VALUE = 1
    DBE_LOG = 2
    DBE_ALARM = 4
    DBE_PROPERTY = 8


chid_t = ctypes.c_long

# Note that Windows needs to be told that chid is 8 bytes for 64-bit, except
# that Python2 is very weird -- using a 4byte chid for 64-bit, but needing a 1
# byte padding!
if PY64_WINDOWS:
    chid_t = ctypes.c_int64

short_t = ctypes.c_short
ushort_t = ctypes.c_ushort
int_t = ctypes.c_int
uint_t = ctypes.c_uint
long_t = ctypes.c_long
ulong_t = ctypes.c_ulong
float_t = ctypes.c_float
double_t = ctypes.c_double
byte_t = ctypes.c_byte
ubyte_t = ctypes.c_ubyte
char_t = ctypes.c_char
char_p = ctypes.c_char_p
void_p = ctypes.c_void_p
py_obj = ctypes.py_object
string_t = ctypes.c_char * MAX_STRING_SIZE

value_offset = None


# extended DBR types:
class TimeStamp(ctypes.Structure):
    "emulate epics timestamp"
    _fields_ = [('secs', uint_t),
                ('nsec', uint_t)]


class _stat_sev(ctypes.Structure):
    _fields_ = [('status', short_t),
                ('severity', short_t),
                ]


class _stat_sev_units(_stat_sev):
    _fields_ = [('units', char_t * MAX_UNITS_SIZE),
                ]


class _stat_sev_ts(ctypes.Structure):
    _fields_ = [('status', short_t),
                ('severity', short_t),
                ('stamp', TimeStamp)
                ]


def make_unixtime(stamp):
    "UNIX timestamp (seconds) from Epics TimeStamp structure"
    return (EPICS2UNIX_EPOCH + stamp.secs + 1.e-6 * int(1.e-3 * stamp.nsec))


class time_string(_stat_sev_ts):
    "dbr time string"
    _fields_ = [('value', MAX_STRING_SIZE * char_t)]


class time_short(_stat_sev_ts):
    "dbr time short"
    _fields_ = [('RISC_pad', short_t),
                ('value', short_t)]


class time_float(_stat_sev_ts):
    "dbr time float"
    _fields_ = [('value', float_t)]


class time_enum(_stat_sev_ts):
    "dbr time enum"
    _fields_ = [('RISC_pad', short_t),
                ('value', ushort_t)]


class time_char(_stat_sev_ts):
    "dbr time char"
    _fields_ = [('RISC_pad0', short_t),
                ('RISC_pad1', byte_t),
                ('value', byte_t)]


class time_long(_stat_sev_ts):
    "dbr time long"
    _fields_ = [('value', int_t)]


class time_double(_stat_sev_ts):
    "dbr time double"
    _fields_ = [('RISC_pad', int_t),
                ('value', double_t)]


def _ctrl_lims(t):
    # DBR types with full control and graphical fields
    # yes, this strange order is as in db_access.h!!!

    class ctrl_lims(ctypes.Structure):
        _fields_ = [('upper_disp_limit', t),
                    ('lower_disp_limit', t),
                    ('upper_alarm_limit', t),
                    ('upper_warning_limit', t),
                    ('lower_warning_limit', t),
                    ('lower_alarm_limit', t),
                    ('upper_ctrl_limit', t),
                    ('lower_ctrl_limit', t),
                    ]

    return ctrl_lims


class ctrl_enum(_stat_sev):
    "dbr ctrl enum"
    _fields_ = [('no_str', short_t),
                ('strs', (char_t * MAX_ENUM_STRING_SIZE) * MAX_ENUMS),
                ('value', ushort_t)
                ]


class ctrl_short(_ctrl_lims(short_t), _stat_sev_units):
    "dbr ctrl short"
    _fields_ = [('value', short_t)]


class ctrl_char(_ctrl_lims(byte_t), _stat_sev_units):
    "dbr ctrl long"
    _fields_ = [('RISC_pad', byte_t),
                ('value', ubyte_t)
                ]


class ctrl_long(_ctrl_lims(int_t), _stat_sev_units):
    "dbr ctrl long"
    _fields_ = [('value', int_t)]


class _ctrl_units(_stat_sev):
    _fields_ = [('precision', short_t),
                ('RISC_pad', short_t),
                ('units', char_t * MAX_UNITS_SIZE),
                ]


class ctrl_float(_ctrl_lims(float_t), _ctrl_units):
    "dbr ctrl float"
    _fields_ = [('value', float_t)]


class ctrl_double(_ctrl_lims(double_t), _ctrl_units):
    "dbr ctrl double"
    _fields_ = [('value', double_t)]


NumpyMap = {ChType.INT: np.int16,
            ChType.FLOAT: np.float32,
            ChType.ENUM: np.uint16,
            ChType.CHAR: np.uint8,
            ChType.LONG: np.int32,
            ChType.DOUBLE: np.float64
            }


# map of Epics DBR types to ctypes types
Map = {ChType.STRING: string_t,
       ChType.INT: short_t,
       ChType.FLOAT: float_t,
       ChType.ENUM: ushort_t,
       ChType.CHAR: ubyte_t,
       ChType.LONG: int_t,
       ChType.DOUBLE: double_t,

       # TODO: these right?
       ChType.STS_STRING: string_t,
       ChType.STS_INT: short_t,
       ChType.STS_FLOAT: float_t,
       ChType.STS_ENUM: ushort_t,
       ChType.STS_CHAR: ubyte_t,
       ChType.STS_LONG: int_t,
       ChType.STS_DOUBLE: double_t,

       ChType.TIME_STRING: time_string,
       ChType.TIME_INT: time_short,
       ChType.TIME_SHORT: time_short,
       ChType.TIME_FLOAT: time_float,
       ChType.TIME_ENUM: time_enum,
       ChType.TIME_CHAR: time_char,
       ChType.TIME_LONG: time_long,
       ChType.TIME_DOUBLE: time_double,

       # Note: there is no ctrl string in the C definition
       ChType.CTRL_STRING: time_string,
       ChType.CTRL_SHORT: ctrl_short,
       ChType.CTRL_INT: ctrl_short,
       ChType.CTRL_FLOAT: ctrl_float,
       ChType.CTRL_ENUM: ctrl_enum,
       ChType.CTRL_CHAR: ctrl_char,
       ChType.CTRL_LONG: ctrl_long,
       ChType.CTRL_DOUBLE: ctrl_double
       }


NativeMap = {
    ChType.STRING: ChType.STRING,
    ChType.INT: ChType.INT,
    ChType.FLOAT: ChType.FLOAT,
    ChType.ENUM: ChType.ENUM,
    ChType.CHAR: ChType.CHAR,
    ChType.LONG: ChType.LONG,
    ChType.DOUBLE: ChType.DOUBLE,

    ChType.STS_STRING: ChType.STRING,
    ChType.STS_INT: ChType.INT,
    ChType.STS_FLOAT: ChType.FLOAT,
    ChType.STS_ENUM: ChType.ENUM,
    ChType.STS_CHAR: ChType.CHAR,
    ChType.STS_LONG: ChType.LONG,
    ChType.STS_DOUBLE: ChType.DOUBLE,

    ChType.TIME_STRING: ChType.STRING,
    ChType.TIME_INT: ChType.INT,
    ChType.TIME_SHORT: ChType.SHORT,
    ChType.TIME_FLOAT: ChType.FLOAT,
    ChType.TIME_ENUM: ChType.ENUM,
    ChType.TIME_CHAR: ChType.CHAR,
    ChType.TIME_LONG: ChType.LONG,
    ChType.TIME_DOUBLE: ChType.DOUBLE,

    # Note: there is no ctrl string in the C definition
    ChType.CTRL_STRING: ChType.TIME_STRING,  # <-- correct
    ChType.CTRL_SHORT: ChType.SHORT,
    ChType.CTRL_INT: ChType.INT,
    ChType.CTRL_FLOAT: ChType.FLOAT,
    ChType.CTRL_ENUM: ChType.ENUM,
    ChType.CTRL_CHAR: ChType.CHAR,
    ChType.CTRL_LONG: ChType.LONG,
    ChType.CTRL_DOUBLE: ChType.DOUBLE,
}


def native_type(ftype):
    "return native field type from TIME or CTRL variant"
    return NativeMap[ftype]


def promote_type(ftype, use_time=False, use_ctrl=False):
    """Promotes a native field type to its TIME or CTRL variant.

    Returns
    -------
    ftype : int
        the promoted field value.
    """
    if use_ctrl:
        ftype += ChType.CTRL_STRING
    elif use_time:
        ftype += ChType.TIME_STRING

    if ftype == ChType.CTRL_STRING:
        return ChType.TIME_STRING
    return ftype


def Name(ftype, reverse=False):
    """ convert integer data type to dbr Name, or optionally reverse that
    look up (that is, name to integer)"""

    m = {ChType.STRING: 'STRING',
         ChType.INT: 'INT',
         ChType.FLOAT: 'FLOAT',
         ChType.ENUM: 'ENUM',
         ChType.CHAR: 'CHAR',
         ChType.LONG: 'LONG',
         ChType.DOUBLE: 'DOUBLE',

         ChType.STS_STRING: 'STS_STRING',
         ChType.STS_SHORT: 'STS_SHORT',
         ChType.STS_INT: 'STS_INT',
         ChType.STS_FLOAT: 'STS_FLOAT',
         ChType.STS_ENUM: 'STS_ENUM',
         ChType.STS_CHAR: 'STS_CHAR',
         ChType.STS_LONG: 'STS_LONG',
         ChType.STS_DOUBLE: 'STS_DOUBLE',

         ChType.TIME_STRING: 'TIME_STRING',
         ChType.TIME_SHORT: 'TIME_SHORT',
         ChType.TIME_FLOAT: 'TIME_FLOAT',
         ChType.TIME_ENUM: 'TIME_ENUM',
         ChType.TIME_CHAR: 'TIME_CHAR',
         ChType.TIME_LONG: 'TIME_LONG',
         ChType.TIME_DOUBLE: 'TIME_DOUBLE',

         ChType.CTRL_STRING: 'CTRL_STRING',
         ChType.CTRL_SHORT: 'CTRL_SHORT',
         ChType.CTRL_FLOAT: 'CTRL_FLOAT',
         ChType.CTRL_ENUM: 'CTRL_ENUM',
         ChType.CTRL_CHAR: 'CTRL_CHAR',
         ChType.CTRL_LONG: 'CTRL_LONG',
         ChType.CTRL_DOUBLE: 'CTRL_DOUBLE',
         }
    if reverse:
        name = ftype.upper()
        if name in list(m.values()):
            for key, val in m.items():
                if name == val:
                    return key
    return m.get(ftype, 'unknown')


def cast_args(args):
    """returns casted array contents

    returns: [dbr_ctrl or dbr_time struct,
              count * native_type structs]

    If data is already of a native_type, the first
    value in the list will be None.
    """
    ftype = args.type
    ntype = native_type(ftype)
    if ftype != ntype:
        native_start = args.raw_dbr + value_offset[ftype]
        return [ctypes.cast(args.raw_dbr,
                            ctypes.POINTER(Map[ftype])).contents,
                ctypes.cast(native_start,
                            ctypes.POINTER(args.count * Map[ntype])).contents
                ]
    else:
        return [None,
                ctypes.cast(args.raw_dbr,
                            ctypes.POINTER(args.count * Map[ftype])).contents
                ]


class event_handler_args(ctypes.Structure):
    "event handler arguments"
    _fields_ = [('usr', ctypes.py_object),
                ('chid', chid_t),
                ('type', long_t),
                ('count', long_t),
                ('raw_dbr', void_p),
                ('status', int_t)]


class connection_args(ctypes.Structure):
    "connection arguments"
    _fields_ = [('chid', chid_t),
                ('op', long_t)]
