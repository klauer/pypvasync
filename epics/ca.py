# low level support for Epics Channel Access
#
#  M Newville <newville@cars.uchicago.edu>
#  The University of Chicago, 2010
#  Epics Open License
"""
EPICS Channel Access Interface

See doc/  for user documentation.

documentation here is developer documentation.
"""
import ctypes
import ctypes.util
import functools

import sys
from threading import Thread
from .utils import BYTES2STR

from . import dbr
from . import config
from . import find_libca
from . import errors


# holder for shared library
initial_context = None
error_message = ''
libca = None


def PySEVCHK(func_name, status, expected=dbr.ECA.NORMAL):
    """This checks the return *status* returned from a `libca.ca_***` and
    raises a :exc:`ChannelAccessException` if the value does not match the
    *expected* value (which is nornmally ``dbr.ECA.NORMAL``.

    The message from the exception will include the *func_name* (name of
    the Python function) and the CA message from :func:`message`.
    """
    if status == expected:
        return status
    raise errors.CASeverityException(func_name, message(status))


def withSEVCHK(fcn):
    """decorator to raise a ChannelAccessException if the wrapped
    ca function does not return status = dbr.ECA.NORMAL.  This
    handles the common case of running :func:`PySEVCHK` for a
    function whose return value is from a corresponding libca function
    and whose return value should be ``dbr.ECA.NORMAL``.
    """
    @functools.wraps(fcn)
    def wrapper(*args, **kwds):
        "withSEVCHK wrapper"
        status = fcn(*args, **kwds)
        return PySEVCHK(fcn.__name__, status)
    return wrapper


def withCA(fcn):
    """decorator to ensure that libca and a context are created
    prior to function calls to the channel access library. This is
    intended for functions that need CA started to work, such as
    :func:`create_channel`.

    Note that CA functions that take a Channel ID (chid) as an
    argument are  NOT wrapped by this: to get a chid, the
    library must have been initialized already."""
    @functools.wraps(fcn)
    def wrapper(*args, **kwds):
        "withCA wrapper"
        global libca, initial_context
        if libca is None:
            libca, initial_context = find_libca.initialize_libca()
        return fcn(*args, **kwds)

    return wrapper


def withCHID(fcn):
    """decorator to ensure that first argument to a function is a Channel
    ID, ``chid``.  The test performed is very weak, as any ctypes long or
    python int will pass, but it is useful enough to catch most accidental
    errors before they would cause a crash of the CA library.
    """
    # It may be worth making a chid class (which could hold connection
    # data of _cache) that could be tested here.  For now, that
    # seems slightly 'not low-level' for this module.
    @functools.wraps(fcn)
    def wrapper(chid):
        "withCHID wrapper"
        if isinstance(chid, int):
            chid = dbr.chid_t(chid)

        if not isinstance(chid, dbr.chid_t):
            msg = "%s: not a valid chid %s %s" % (
                (fcn.__name__, chid, type(chid)))
            raise errors.ChannelAccessException(msg)

        return fcn(chid)
    return wrapper


def withConnectedCHID(fcn):
    """decorator to ensure that the first argument of a function is a
    fully connected Channel ID, ``chid``.  This test is (intended to be)
    robust, and will try to make sure a ``chid`` is actually connected
    before calling the decorated function.
    """
    @functools.wraps(fcn)
    def wrapper(chid, *args, **kwds):
        "withConnectedCHID wrapper"
        if isinstance(chid, int):
            chid = dbr.chid_t(chid)

        if libca.ca_state(chid) != dbr.ConnStatus.CS_CONN:
            #     timeout = kwds.get('timeout',
            #     config.DEFAULT_CONNECTION_TIMEOUT)
            #     fmt = ("%s() timed out waiting '%s' to connect (%d"
            #           "seconds)" % (fcn.__name__, name(chid), timeout))
            #     if not connect_channel(chid, timeout=timeout):
            raise errors.ChannelAccessException('Channel not connected')

        return fcn(chid, *args, **kwds)
    return wrapper


def access(chid):
    """returns a string describing read/write access: one of
    `no access`, `read-only`, `write-only`, or `read/write`
    """
    acc = read_access(chid) + 2 * write_access(chid)
    return ('no access', 'read-only', 'write-only', 'read/write')[acc]


def channel_id_to_int(chid):
    '''Get an integer from a channel ID (from dbr.chid_t)'''
    if isinstance(chid, dbr.chid_t):
        chid = chid.value

    return int(chid)


@withCA
@withSEVCHK
def context_create(ctx=None):
    """Create a new context, using the value of :data:`PREEMPTIVE_CALLBACK`
    to set the context type.

    Parameters
    ----------
    ctx : int
       0 -- No preemptive callbacks,
       1 -- use use preemptive callbacks,
       None -- use value of :data:`PREEMPTIVE_CALLBACK`
    """
    if ctx is None:
        ctx = {False: 0, True: 1}[config.PREEMPTIVE_CALLBACK]
    return libca.ca_context_create(ctx)


@withCA
def context_destroy():
    "destroy current context"
    return libca.ca_context_destroy()


@withCA
@withSEVCHK
def attach_context(context):
    "attach to the supplied context"
    return libca.ca_attach_context(context)


@withCA
@withSEVCHK
def use_initial_context():
    """Attaches to the context created when libca is initialized.
    Using this function is recommended when writing threaded programs that
    using CA.

    See Also
    --------
    :ref:`advanced-threads-label` in doc for further discussion.

    """
    global initial_context
    ret = dbr.ECA.NORMAL
    if initial_context != current_context():
        ret = libca.ca_attach_context(initial_context)
    return ret


@withCA
def detach_context():
    "detach context"
    return libca.ca_detach_context()


@withCA
def replace_printf_handler(fcn=None):
    """replace the normal printf() output handler
    with the supplied function (defaults to :func:`sys.stderr.write`)"""
    global error_message
    if fcn is None:
        fcn = sys.stderr.write
    error_message = ctypes.CFUNCTYPE(None, ctypes.c_char_p)(fcn)
    return libca.ca_replace_printf_handler(error_message)


@withCA
def current_context():
    "return the current context"
    ctx = libca.ca_current_context()
    if isinstance(ctx, ctypes.c_long):
        ctx = ctx.value
    return ctx


@withCA
def client_status(context, level):
    """print (to stderr) information about Channel Access status,
    including status for each channel, and search and connection statistics."""
    return libca.ca_client_status(context, level)


@withCA
def flush_io():
    "flush i/o"
    return libca.ca_flush_io()


@withCA
def message(status):
    """Print a message corresponding to a Channel Access status return value.
    """
    return BYTES2STR(libca.ca_message(status))


@withCA
def version():
    """   Print Channel Access version string.
    Currently, this should report '4.13' """
    return BYTES2STR(libca.ca_version())


@withCA
def pend_io(timeout=1.0):
    """polls CA for i/o. """
    ret = libca.ca_pend_io(timeout)
    try:
        return PySEVCHK('pend_io', ret)
    except errors.CASeverityException:
        return ret


@withCA
def pend_event(timeout=1.e-5):
    """polls CA for events """
    ret = libca.ca_pend_event(timeout)
    try:
        return PySEVCHK('pend_event', ret, dbr.ECA.TIMEOUT)
    except errors.CASeverityException:
        return ret


@withCA
def test_io():
    """test if IO is complete: returns True if it is"""
    return (dbr.ECA.IODONE == libca.ca_test_io())


@withCHID
def name(chid):
    "return PV name for channel name"
    return BYTES2STR(libca.ca_name(chid))


@withCHID
def host_name(chid):
    "return host name and port serving Channel"
    return BYTES2STR(libca.ca_host_name(chid))


@withCHID
def element_count(chid):
    """return number of elements in Channel's data.
    1 for most Channels, > 1 for waveform Channels"""

    return libca.ca_element_count(chid)


@withCHID
def read_access(chid):
    "return *read access* for a Channel: 1 for ``True``, 0 for ``False``."
    return libca.ca_read_access(chid)


@withCHID
def write_access(chid):
    "return *write access* for a channel: 1 for ``True``, 0 for ``False``."
    return libca.ca_write_access(chid)


@withCHID
def field_type(chid):
    "return the integer DBR field type."
    # print(" Field Type", chid)
    return libca.ca_field_type(chid)


@withCHID
def clear_channel(chid):
    "clear the channel"
    return libca.ca_clear_channel(chid)


@withCHID
def state(chid):
    "return state (that is, attachment state) for channel"
    return libca.ca_state(chid)


def is_connected(chid):
    """return whether channel is connected by channel id

    This is ``True`` for a connected channel, ``False`` for an unconnected
    channel.
    """
    return dbr.ConnStatus.CS_CONN == state(chid)


@withCA
@withSEVCHK
def clear_subscription(event_id):
    "cancel subscription given its *event_id*"
    return libca.ca_clear_subscription(event_id)


class CAThread(Thread):
    """
    Sub-class of threading.Thread to ensure that the
    initial CA context is used.
    """

    def run(self):
        if sys.platform == 'darwin':
            context_create()
        else:
            use_initial_context()
        super().run()
