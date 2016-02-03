import asyncio

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

__doc__ = """
   epics channel access python module

   version: %s
   Principal Authors:
      Matthew Newville <newville@cars.uchicago.edu> CARS, University of Chicago
      Angus Gratton <angus.gratton@anu.edu.au>, Australian National University

== License:

   Except where explicitly noted, this file and all files in this
   distribution are licensed under the Epics Open License See license.txt in
   the top-level directory of this distribution.

== Overview:
   Python Interface to the Epics Channel Access
   protocol of the Epics control system.

""" % (__version__)


import time
import sys
from . import ca
from . import dbr
from . import context
from . import pv
from . import alarm
from . import device
from . import motor
from . import multiproc

from .pv import (get_pv, PV)
from .alarm import (Alarm, )
from .motor import Motor
from .device import Device
from .ca import poll
from .multiproc import (CAProcess, CAPool)

# some constants
NO_ALARM = 0
MINOR_ALARM = 1
MAJOR_ALARM = 2
INVALID_ALARM = 3

_PVmonitors_ = {}


@asyncio.coroutine
def caput(pvname, value, *, wait=True, timeout=60):
    """caput(pvname, value, wait=False, timeout=60)
    simple put to a pv's value.
       >>> caput('xx.VAL',3.0)

    to wait for pv to complete processing, use 'wait=True':
       >>> caput('xx.VAL',3.0,wait=True)
    """
    thispv = yield from get_pv(pvname, connect=True)
    if not thispv.connected:
        raise asyncio.TimeoutError()
    ret = yield from thispv.put(value, wait=wait, timeout=timeout)
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
    thispv = yield from get_pv(pvname, connect=True)
    if not thispv.connected:
        raise asyncio.TimeoutError()

    if as_string:
        thispv.get_ctrlvars()
    val = yield from thispv.get(count=count, timeout=timeout,
                                use_monitor=use_monitor,
                                as_string=as_string, as_numpy=as_numpy)
    poll()
    return val


@asyncio.coroutine
def cainfo(pvname):
    """cainfo(pvname)

    return printable information about pv
       >>>cainfo('xx.VAL')

    will return a status report for the pv.
    """
    thispv = yield from get_pv(pvname, connect=True)
    if not thispv.connected:
        raise asyncio.TimeoutError()

    yield from thispv.get()
    yield from thispv.get_ctrlvars()
    return thispv.info


def camonitor_clear(pvname):
    """clear a monitor on a PV"""
    if pvname in _PVmonitors_:
        _PVmonitors_[pvname].remove_callback(index=-999)
        _PVmonitors_.pop(pvname)


def camonitor(pvname, writer=None, callback=None):
    """ camonitor(pvname, writer=None, callback=None)

    sets a monitor on a PV.
       >>>camonitor('xx.VAL')

    This will write a message with the latest value for that PV each
    time the value changes and when ca.poll() is called.

    To write the result to a file, provide the writer option a write method
    to an open file or some other method that accepts a string.

    To completely control where the output goes, provide a callback method
    and you can do whatever you'd like with them.

    Your callback will be sent keyword arguments for pvname, value, and
    char_value Important: use **kwd!!
    """

    if writer is None:
        writer = print
    if callback is None:
        def callback(pvname=None, value=None, char_value=None, **kwds):
            "generic monitor callback"
            if char_value is None:
                char_value = repr(value)
            writer("%.32s %s %s" % (pvname, pv.fmt_time(), char_value))

    thispv = yield from get_pv(pvname, connect=True)
    if thispv.connected:
        thispv.get()
        thispv.add_callback(callback, index=-999, with_ctrlvars=True)
        _PVmonitors_[pvname] = thispv


def caget_many(pvlist):
    """get values for a list of PVs
    This does not maintain PV objects, and works as fast
    as possible to fetch many values.
    """
    chids, out = [], []
    for name in pvlist:
        chids.append(ca.create_channel(name, auto_cb=False, connect=False))
    for chid in chids:
        ca.connect_channel(chid)
    for chid in chids:
        ca.get(chid, wait=False)
    for chid in chids:
        out.append(ca.get_complete(chid))
    return out
