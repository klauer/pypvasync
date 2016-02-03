#!usr/bin/python
#
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
import asyncio
import ctypes
import ctypes.util
import functools

import os
import sys
import time
from math import log10
import atexit
import warnings
from threading import Thread
from functools import partial


from .utils import (STR2BYTES, BYTES2STR, NULLCHAR, NULLCHAR_2,
                    strjoin, memcopy, is_string, is_string_or_bytes,
                    ascii_string, PY64_WINDOWS)

# ignore warning about item size... for now??
warnings.filterwarnings('ignore',
                        'Item size computed from the PEP 3118*',
                        RuntimeWarning)

import numpy
from . import dbr
from .dbr import native_type


_pending_futures = set()


def _make_callback(func, args):
    """ make callback function"""
    if PY64_WINDOWS:
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
    fcn.ca_callback = _make_callback(fcn, dbr.event_handler_args)
    return fcn


# holder for shared library
libca = None
initial_context = None
error_message = ''

# PREEMPTIVE_CALLBACK determines the CA context
PREEMPTIVE_CALLBACK = True

AUTO_CLEANUP = (sys.version_info.major == 2)

##
# maximum element count for auto-monitoring of PVs in epics.pv
# and for automatic conversion of numerical array data to numpy arrays
AUTOMONITOR_MAXLENGTH = 65536  # 16384

# default timeout for connection
#   This should be kept fairly short --
#   as connection will be tried repeatedly
DEFAULT_CONNECTION_TIMEOUT = 2.0

# Cache of existing channel IDs:
#  pvname: {'chid':chid, 'conn': isConnected,
#           'ts': ts_conn, 'callbacks': [ user_callback... ])
#  isConnected   = True/False: if connected.
#  ts_conn       = ts of last connection event or failed attempt.
#  user_callback = one or more user functions to be called on
#                  change (accumulated in the cache)
_cache = {}
_namecache = {}

# logging.basicConfig(filename='ca.log',level=logging.DEBUG)

# get a unique python value that cannot be a value held by an
# actual PV to signal "Get is incomplete, awaiting callback"


class Empty:
    """used to create a unique python value that cannot be
    held as an actual PV value"""
    pass
GET_PENDING = Empty()


class ChannelAccessException(Exception):
    """Channel Access Exception: General Errors"""

    def __init__(self, *args):
        super().__init__(*args)
        sys.excepthook(*sys.exc_info())


class CASeverityException(Exception):
    """Channel Access Severity Check Exception:
    PySEVCHK got unexpected return value"""

    def __init__(self, fcn, msg):
        super().__init__()
        self.fcn = fcn
        self.msg = msg

    def __str__(self):
        return " %s returned '%s'" % (self.fcn, self.msg)


def find_libca():
    """
    find location of ca dynamic library
    """
    # Test 1: if PYEPICS_LIBCA env var is set, use it.
    dllpath = os.environ.get('PYEPICS_LIBCA', None)
    if (dllpath is not None and os.path.exists(dllpath) and
            os.path.isfile(dllpath)):
        return dllpath

    # Test 2: look through Python path and PATH env var for dll
    path_sep = ':'
    dylib = 'lib'
    # For windows, we assume the DLLs are installed with the library
    if os.name == 'nt':
        path_sep = ';'
        dylib = 'DLLs'

    _path = [os.path.split(os.path.abspath(__file__))[0],
             os.path.split(os.path.dirname(os.__file__))[0],
             os.path.join(sys.prefix, dylib)]

    search_path = []
    for adir in (_path + sys.path +
                 os.environ.get('PATH', '').split(path_sep) +
                 os.environ.get('LD_LIBRARY_PATH', '').split(path_sep) +
                 os.environ.get('DYLD_LIBRARY_PATH', '').split(path_sep)):
        if adir not in search_path and os.path.isdir(adir):
            search_path.append(adir)

    os.environ['PATH'] = path_sep.join(search_path)

    # with PATH set above, the ctypes utility, find_library *should*
    # find the dll....
    dllpath = ctypes.util.find_library('ca')
    if dllpath is not None:
        return dllpath

    # Test 3: on unixes, look expliticly with EPICS_BASE env var and
    # known architectures for ca.so q
    if os.name == 'posix':
        known_hosts = {'Linux':   ('linux-x86', 'linux-x86_64'),
                       'Darwin':  ('darwin-ppc', 'darwin-x86'),
                       'SunOS':   ('solaris-sparc', 'solaris-sparc-gnu')}

        libname = 'libca.so'
        if sys.platform == 'darwin':
            libname = 'libca.dylib'

        epics_base = os.environ.get('EPICS_BASE', '.')
        epics_host_arch = os.environ.get('EPICS_HOST_ARCH')
        host_arch = os.uname()[0]
        if host_arch in known_hosts:
            epicspath = [os.path.join(
                epics_base, 'lib', epics_host_arch)] if epics_host_arch else []
            for adir in known_hosts[host_arch]:
                epicspath.append(os.path.join(epics_base, 'lib', adir))
        for adir in search_path + epicspath:
            if os.path.exists(adir) and os.path.isdir(adir):
                if libname in os.listdir(adir):
                    return os.path.join(adir, libname)

    raise ChannelAccessException('cannot find Epics CA DLL')


def initialize_libca():
    """Initialize the Channel Access library.

    This loads the shared object library (DLL) to establish Channel Access
    Connection. The value of :data:`PREEMPTIVE_CALLBACK` sets the pre-emptive
    callback model.

   This **must** be called prior to any actual use of the CA library, but
    will be called automatically by the the :func:`withCA` decorator, so
    you should not need to call this directly from most real programs.

    Returns
    -------
    libca : object
        ca library object, used for all subsequent ca calls

    See Also
    --------
    withCA :  decorator to ensure CA is initialized

    Notes
    -----
    This function must be called prior to any real CA calls.

    """
    if 'EPICS_CA_MAX_ARRAY_BYTES' not in os.environ:
        os.environ['EPICS_CA_MAX_ARRAY_BYTES'] = "%i" % 2 ** 24

    dllname = find_libca()
    load_dll = ctypes.cdll.LoadLibrary
    global libca, initial_context, _cache
    if os.name == 'nt':
        load_dll = ctypes.windll.LoadLibrary
    try:
        libca = load_dll(dllname)
    except Exception as exc:
        raise ChannelAccessException(
            'loading Epics CA DLL failed: ' + str(exc))

    ca_context = {False: 0, True: 1}[PREEMPTIVE_CALLBACK]
    ret = libca.ca_context_create(ca_context)
    if ret != dbr.ECA_NORMAL:
        raise ChannelAccessException('cannot create Epics CA Context')

    # set argtypes and non-default return types
    # for several libca functions here
    libca.ca_pend_event.argtypes = [ctypes.c_double]
    libca.ca_pend_io.argtypes = [ctypes.c_double]
    libca.ca_client_status.argtypes = [ctypes.c_void_p, ctypes.c_long]

    libca.ca_current_context.restype = ctypes.c_void_p
    libca.ca_version.restype = ctypes.c_char_p
    libca.ca_host_name.restype = ctypes.c_char_p
    libca.ca_name.restype = ctypes.c_char_p
    # libca.ca_name.argstypes    = [dbr.chid_t]
    # libca.ca_state.argstypes   = [dbr.chid_t]
    libca.ca_message.restype = ctypes.c_char_p

    # save value offests used for unpacking
    # TIME and CTRL data as an array in dbr module
    dbr.value_offset = (39 * ctypes.c_short).in_dll(libca, 'dbr_value_offset')
    initial_context = current_context()
    if AUTO_CLEANUP:
        atexit.register(finalize_libca)
    return libca


def finalize_libca(maxtime=10.0):
    """shutdown channel access:

    run :func:`clear_channel` for all chids in :data:`_cache`,
    then calls :func:`flush_io` and :func:`poll` a few times.

    Parameters
    ----------
    maxtime : float
        maximimum time (in seconds) to wait for :func:`flush_io` and :func:`poll` to complete.

    """
    global libca
    global _cache
    if libca is None:
        return
    try:
        start_time = time.time()
        flush_io()
        poll()
        for ctx in _cache.values():
            for key in list(ctx.keys()):
                ctx.pop(key)
        _cache.clear()
        flush_count = 0
        while (flush_count < 5 and
               time.time() - start_time < maxtime):
            flush_io()
            poll()
            flush_count += 1
        context_destroy()
        libca = None
    except Exception:
        pass
    time.sleep(0.01)


def get_cache(pvname):
    "return cache dictionary for a given pvname in the current context"
    return _cache[current_context()].get(pvname, None)


def show_cache(print_out=True):
    """print out a listing of PVs in the current session to
    standard output.  Use the *print_out=False* option to be
    returned the listing instead of having it printed out.
    """
    global _cache
    out = []
    out.append('#  PVName        ChannelID/Context Connected?')
    out.append('#--------------------------------------------')
    for context, context_chids in list(_cache.items()):
        for vname, val in list(context_chids.items()):
            chid = val['chid']
            if len(vname) < 15:
                vname = (vname + ' ' * 15)[:15]
            out.append(" %s %s/%s  %s" % (vname, repr(chid),
                                          repr(context),
                                          isConnected(chid)))
    out = strjoin('\n', out)
    if print_out:
        print(out)
    else:
        return out


def clear_cache():
    """
    Clears global caches of Epics CA connectsions, and fully
    detached from CA contex.  This is important when doing
    multiprocessing (and is done internally by CAProcess),
    but can be  useful to fully reset a Channel Access session.
    """

    # Clear global state variables
    global _cache
    _cache.clear()

    # The old context is copied directly from the old process
    # in systems with proper fork() implementations
    detach_context()
    create_context()


# decorator functions for ca functionality:
#  decorator name      ensures before running decorated function:
#  --------------      -----------------------------------------------
#   withCA               libca is initialized
#   withCHID             1st arg is a chid (dbr.chid_t)
#   withConnectedCHID    1st arg is a connected chid.
#   withInitialContext   Force the use of the initially defined context
#
#  These tests are not rigorous CA tests (and ctypes.long is
#  accepted as a chid, connect_channel() is tried, but may fail)
##
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
        global libca
        if libca is None:
            initialize_libca()
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
    def wrapper(*args, **kwds):
        "withCHID wrapper"
        if len(args) > 0:
            chid = args[0]
            args = list(args)
            if isinstance(chid, int):
                args[0] = chid = dbr.chid_t(args[0])
            if not isinstance(chid, dbr.chid_t):
                msg = "%s: not a valid chid %s %s args %s kwargs %s!" % (
                    (fcn.__name__, chid, type(chid), args, kwds))
                raise ChannelAccessException(msg)

        return fcn(*args, **kwds)
    return wrapper


def withConnectedCHID(fcn):
    """decorator to ensure that the first argument of a function is a
    fully connected Channel ID, ``chid``.  This test is (intended to be)
    robust, and will try to make sure a ``chid`` is actually connected
    before calling the decorated function.
    """
    @functools.wraps(fcn)
    def wrapper(*args, **kwds):
        "withConnectedCHID wrapper"
        if len(args) > 0:
            chid = args[0]
            args = list(args)
            if isinstance(chid, int):
                args[0] = chid = dbr.chid_t(chid)
            if not isinstance(chid, dbr.chid_t):
                raise ChannelAccessException("%s: not a valid chid!" %
                                             (fcn.__name__))
            if not isConnected(chid):
                timeout = kwds.get('timeout', DEFAULT_CONNECTION_TIMEOUT)
                fmt = "%s() timed out waiting '%s' to connect (%d seconds)"
                if not connect_channel(chid, timeout=timeout):
                    raise ChannelAccessException(fmt % (fcn.__name__,
                                                        name(chid), timeout))

        return fcn(*args, **kwds)
    return wrapper


def withInitialContext(fcn):
    """decorator to ensure that the wrapped function uses the
    initial threading context created at initialization of CA
    """
    @functools.wraps(fcn)
    def wrapper(*args, **kwds):
        "withInitialContext wrapper"
        use_initial_context()
        return fcn(*args, **kwds)
    return wrapper


def PySEVCHK(func_name, status, expected=dbr.ECA_NORMAL):
    """This checks the return *status* returned from a `libca.ca_***` and
    raises a :exc:`ChannelAccessException` if the value does not match the
    *expected* value (which is nornmally ``dbr.ECA_NORMAL``.

    The message from the exception will include the *func_name* (name of
    the Python function) and the CA message from :func:`message`.
    """
    if status == expected:
        return status
    raise CASeverityException(func_name, message(status))


def withSEVCHK(fcn):
    """decorator to raise a ChannelAccessException if the wrapped
    ca function does not return status = dbr.ECA_NORMAL.  This
    handles the common case of running :func:`PySEVCHK` for a
    function whose return value is from a corresponding libca function
    and whose return value should be ``dbr.ECA_NORMAL``.
    """
    @functools.wraps(fcn)
    def wrapper(*args, **kwds):
        "withSEVCHK wrapper"
        status = fcn(*args, **kwds)
        return PySEVCHK(fcn.__name__, status)
    return wrapper

##
# Event Handler for monitor event callbacks


@ca_callback_event
def _onMonitorEvent(args):
    """Event Handler for monitor events: not intended for use"""

    # for 64-bit python on Windows!
    value = dbr.cast_args(args)

    pvname = name(args.chid)
    kwds = {'ftype': args.type, 'count': args.count,
            'chid': args.chid, 'pvname': pvname,
            'status': args.status}

    # add kwds arguments for CTRL and TIME variants
    # this is in a try/except clause to avoid problems
    # caused by uninitialized waveform arrays
    if args.type >= dbr.CTRL_STRING:
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
    elif args.type >= dbr.TIME_STRING:
        try:
            tmpv = value[0]
            kwds['status'] = tmpv.status
            kwds['severity'] = tmpv.severity
            kwds['timestamp'] = dbr.make_unixtime(tmpv.stamp)
        except IndexError:
            pass

    value = _unpack(args.chid, value, count=args.count, ftype=args.type)
    if callable(args.usr):
        args.usr(value=value, **kwds)

# connection event handler:


def _onConnectionEvent(args):
    """set flag in cache holding whteher channel is
    connected. if provided, run a user-function"""
    # for 64-bit python on Windows!
    ctx = current_context()
    conn = (args.op == dbr.OP_CONN_UP)
    global _cache

    if ctx is None and len(_cache.keys()) > 0:
        ctx = list(_cache.keys())[0]
    if ctx not in _cache:
        _cache[ctx] = {}

    # search for PV in any context...
    pv_found = False
    for context in _cache:
        pvname = name(args.chid)

        if pvname in _cache[context]:
            pv_found = True
            break

    # logging.debug("ConnectionEvent %s/%i/%i " % (pvname, args.chid, conn))
    # print("ConnectionEvent %s/%i/%i " % (pvname, args.chid, conn))
    if not pv_found:
        _cache[ctx][pvname] = {'conn': False, 'chid': args.chid,
                               'ts': 0, 'failures': 0, 'value': None,
                               'callbacks': []}

    # set connection time, run connection callbacks
    # in all contexts
    for context, cvals in _cache.items():
        if pvname in cvals:
            entry = cvals[pvname]
            ichid = entry['chid']
            if isinstance(entry['chid'], dbr.chid_t):
                ichid = entry['chid'].value

            if int(ichid) == int(args.chid):
                chid = args.chid
                entry.update({'chid': chid, 'conn': conn,
                              'ts': time.time(), 'failures': 0})
                for callback in entry.get('callbacks', []):
                    poll()
                    if callable(callback):
                        # print( ' ==> connection callback ', callback, conn)
                        callback(pvname=pvname, chid=chid, conn=conn)
    #print('Connection done')

    return




@ca_callback_event
def _onGetEvent(args):
    """get_callback event: simply store data contents which will need
    conversion to python data with _unpack()."""
    future = args.usr
    _pending_futures.remove(future)
    # print("GET EVENT: chid, user ", args.chid, future, hash(future))
    # print("           type, count ", args.type, args.count)
    # print("           status ", args.status, dbr.ECA_NORMAL)

    if future.done():
        print('getevent: hmm, future already done', future, id(future))
        return

    if future.cancelled():
        print('future was cancelled', future)
        return

    # TODO futures not threadsafe? pass in a threading.Lock() too?
    if args.status != dbr.ECA_NORMAL:
        # TODO look up in appdev manual
        future.set_exception(CASeverityException('get', str(args.status)))
    else:
        future.set_result(memcopy(dbr.cast_args(args)))


@ca_callback_event
def _onPutEvent(args, **kwds):
    """set put-has-completed for this channel"""
    future = args.usr
    print('putevent', future)
    _pending_futures.remove(future)

    if future.done():
        print('putevent: hmm, future already done', future, id(future))
    elif not future.cancelled():
        print('finishing put event')
        future.set_result(True)


# create global reference to these callbacks


_CB_CONNECT = _make_callback(_onConnectionEvent, dbr.connection_args)
_CB_EVENT = _make_callback(_onMonitorEvent, dbr.event_handler_args)

# Now we're ready to wrap libca functions
#
###

# contexts


@withCA
@withSEVCHK
def context_create(ctx=None):
    "create a context. if argument is None, use PREEMPTIVE_CALLBACK"
    if ctx is None:
        ctx = {False: 0, True: 1}[PREEMPTIVE_CALLBACK]
    return libca.ca_context_create(ctx)


def create_context(ctx=None):
    """Create a new context, using the value of :data:`PREEMPTIVE_CALLBACK`
    to set the context type. Note that both *context_create* and
    *create_context* (which is more consistent with the Verb_Object of
    the rest of the CA library) are supported.

    Parameters
    ----------
    ctx : int
       0 -- No preemptive callbacks,
       1 -- use use preemptive callbacks,
       None -- use value of :data:`PREEMPTIVE_CALLBACK`

    """
    context_create(ctx=ctx)


@withCA
def context_destroy():
    "destroy current context"
    global _cache
    ctx = current_context()
    ret = libca.ca_context_destroy()
    if ctx in _cache:
        for key in list(_cache[ctx].keys()):
            _cache[ctx].pop(key)
        _cache.pop(ctx)
    return ret


def destroy_context():
    "destroy current context"
    return context_destroy()


@withCA
# @withSEVCHK
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
    ret = dbr.ECA_NORMAL
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
    except CASeverityException:
        return ret

# @withCA


def pend_event(timeout=1.e-5):
    """polls CA for events """
    ret = libca.ca_pend_event(timeout)
    try:
        return PySEVCHK('pend_event', ret, dbr.ECA_TIMEOUT)
    except CASeverityException:
        return ret


@withCA
def poll(evt=1.e-5, iot=1.0):
    """a convenience function which is equivalent to::
       pend_event(evt)
       pend_io_(iot)

    """
    pend_event(evt)
    return pend_io(iot)


@withCA
def test_io():
    """test if IO is complete: returns True if it is"""
    return (dbr.ECA_IODONE == libca.ca_test_io())

# create channel


@withCA
def create_channel(pvname, connect=False, auto_cb=True, callback=None):
    """ create a Channel for a given pvname

    creates a channel, returning the Channel ID ``chid`` used by other
    functions to identify this channel.

    Parameters
    ----------
    pvname :  string
        the name of the PV for which a channel should be created.
    connect : bool
        whether to (try to) connect to PV as soon as possible.
    auto_cb : bool
        whether to automatically use an internal connection callback.
    callback : callable or ``None``
        user-defined Python function to be called when the connection
        state change s.

    Returns
    -------
    chid : ctypes.c_long
        channel ID.


    Notes
    -----
    1. The user-defined connection callback function should be prepared to accept
    keyword arguments of

         ===========  =============================
         keyword      meaning
         ===========  =============================
          `pvname`    name of PV
          `chid`      Channel ID
          `conn`      whether channel is connected
         ===========  =============================


    2. If `auto_cb` is ``True``, an internal connection callback is used so
    that you should not need to explicitly connect to a channel, unless you
    are having difficulty with dropped connections.

    3. If the channel is already connected for the PV name, the callback
    will be called immediately.


    """
    #
    # Note that _CB_CONNECT (defined above) is a global variable, holding
    # a reference to _onConnectionEvent:  This is really the connection
    # callback that is run -- the callack here is stored in the _cache
    # and called by _onConnectionEvent.
    pvn = STR2BYTES(pvname)
    ctx = current_context()
    global _cache
    if ctx not in _cache:
        _cache[ctx] = {}
    if pvname not in _cache[ctx]:  # new PV for this context
        entry = {'conn': False, 'chid': None,
                 'ts': 0, 'failures': 0, 'value': None,
                 'callbacks': [callback]}
        # logging.debug("Create Channel %s " % pvname)
        _cache[ctx][pvname] = entry
    else:
        entry = _cache[ctx][pvname]
        if not entry['conn'] and callback is not None:  # pending connection
            _cache[ctx][pvname]['callbacks'].append(callback)
        elif (callable(callback) and callback not in entry['callbacks']):
            entry['callbacks'].append(callback)
            callback(chid=entry['chid'], pvname=pvname, conn=entry['conn'])

    conncb = 0
    if auto_cb:
        conncb = _CB_CONNECT
    if entry.get('chid', None) is not None:
        # already have or waiting on a chid
        chid = _cache[ctx][pvname]['chid']
    else:
        chid = dbr.chid_t()
        ret = libca.ca_create_channel(pvn, conncb, 0, 0,
                                      ctypes.byref(chid))
        PySEVCHK('create_channel', ret)
        entry['chid'] = chid

    chid_key = chid
    if isinstance(chid_key, dbr.chid_t):
        chid_key = chid.value
    _namecache[chid_key] = BYTES2STR(pvn)
    # print("CREATE Channel ", pvn, chid)
    if connect:
        connect_channel(chid)
    if conncb != 0:
        poll()
    return chid


@withCHID
def connect_channel(chid, timeout=None, verbose=False):
    """connect to a channel, waiting up to timeout for a
    channel to connect.  It returns the connection state,
    ``True`` or ``False``.

    This is usually not needed, as implicit connection will be done
    when needed in most cases.

    Parameters
    ----------
    chid : ctypes.c_long
        Channel ID
    timeout : float
        maximum time to wait for connection.
    verbose : bool
        whether to print out debugging information

    Returns
    -------
    connection_state : bool
         that is, whether the Channel is connected

    Notes
    -----
    1. If *timeout* is ``None``, the value of :data:`DEFAULT_CONNECTION_TIMEOUT` is used (defaults to 2.0 seconds).

    2. Normally, channels will connect in milliseconds, and the connection
    callback will succeed on the first attempt.

    3. For un-connected Channels (that are nevertheless queried), the 'ts'
    (timestamp of last connection attempt) and 'failures' (number of failed
    connection attempts) from the :data:`_cache` will be used to prevent
    spending too much time waiting for a connection that may never happen.

    """
    if verbose:
        print(' connect channel -> %s %s %s ' %
              (repr(chid), repr(state(chid)), repr(dbr.CS_CONN)))
    conn = (state(chid) == dbr.CS_CONN)
    if not conn:
        # not connected yet, either indicating a slow network
        # or a truly un-connnectable channel.
        start_time = time.time()
        ctx = current_context()
        pvname = name(chid)
        global _cache
        if ctx not in _cache:
            _cache[ctx] = {}

        if timeout is None:
            timeout = DEFAULT_CONNECTION_TIMEOUT

        while (not conn and ((time.time() - start_time) < timeout)):
            poll()
            conn = (state(chid) == dbr.CS_CONN)
        if not conn:
            _cache[ctx][pvname]['ts'] = time.time()
            _cache[ctx][pvname]['failures'] += 1
    return conn

# functions with very light wrappings:


@withCHID
def name(chid):
    "return PV name for channel name"
    global _namecache
    # sys.stdout.write("NAME %s %s\n" % (repr(chid), repr(chid.value in _namecache)))
    # sys.stdout.flush()

    if chid.value in _namecache:
        val = _namecache[chid.value]
    else:
        val = _namecache[chid.value] = BYTES2STR(libca.ca_name(chid))
    return val


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


def isConnected(chid):
    """return whether channel is connected:  `dbr.CS_CONN==state(chid)`

    This is ``True`` for a connected channel, ``False`` for an unconnected channel.
    """

    return dbr.CS_CONN == state(chid)


def access(chid):
    """returns a string describing read/write access: one of
    `no access`, `read-only`, `write-only`, or `read/write`
    """
    acc = read_access(chid) + 2 * write_access(chid)
    return ('no access', 'read-only', 'write-only', 'read/write')[acc]


def promote_type(chid, use_time=False, use_ctrl=False):
    """promotes the native field type of a ``chid`` to its TIME or CTRL variant.
    Returns the integer corresponding to the promoted field value."""
    ftype = field_type(chid)
    if use_ctrl:
        ftype += dbr.CTRL_STRING
    elif use_time:
        ftype += dbr.TIME_STRING
    if ftype == dbr.CTRL_STRING:
        ftype = dbr.TIME_STRING
    return ftype


def _unpack(chid, data, count=None, ftype=None, as_numpy=True):
    """unpacks raw data for a Channel ID `chid` returned by libca functions
    including `ca_get_array_callback` or subscription callback, and returns
    the corresponding Python data

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

    def scan_string(data, count):
        """ Scan a string, or an array of strings as a list, depending on content """
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
            dtype = dbr.NP_Map.get(ntype, None)
            if dtype is not None:
                out = numpy.empty(shape=(count,), dtype=dbr.NP_Map[ntype])
                ctypes.memmove(out.ctypes.data, data, out.nbytes)
            else:
                out = numpy.ctypeslib.as_array(memcopy(data))
        else:
            out = memcopy(data)
        return out

    def unpack(data, count, ntype, use_numpy):
        "simple, native data type"
        if data is None:
            return None
        elif count == 1 and ntype != dbr.STRING:
            return data[0]
        elif ntype == dbr.STRING:
            return scan_string(data, count)
        elif count > 1:
            return array_cast(data, count, ntype, use_numpy)
        return data

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
        ftype = dbr.INT
    ntype = native_type(ftype)
    use_numpy = (as_numpy and ntype != dbr.STRING and count > 1)
    return unpack(data, count, ntype, use_numpy)


@withConnectedCHID
@asyncio.coroutine
def get(chid, ftype=None, count=None, timeout=None,
        as_string=False, as_numpy=True):
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
        count = element_count(chid)
    else:
        count = min(count, element_count(chid))

    future = asyncio.Future()
    _pending_futures.add(future)

    print('get future', id(future))
    ret = libca.ca_array_get_callback(ftype, count, chid,
                                      _onGetEvent.ca_callback,
                                      ctypes.py_object(future))
    PySEVCHK('get', ret)

    if timeout is None:
        timeout = 1.0 + log10(max(1, count))

    try:
        value = yield from asyncio.wait_for(future, timeout=timeout)
    except asyncio.TimeoutError as ex:
        future.cancel()
        raise

    # print("Get Complete> Unpack ", value, count, ftype)
    val = _unpack(chid, value, count=count, ftype=ftype, as_numpy=as_numpy)
    # print("Get Complete unpacked to ", val)

    if as_string:
        val = _as_string(val, chid, count, ftype)
    elif isinstance(val, ctypes.Array) and as_numpy:
        val = numpy.ctypeslib.as_array(memcopy(val))

    return val


def _as_string(val, chid, count, ftype):
    "primitive conversion of value to a string"
    try:
        if (ftype in (dbr.CHAR, dbr.TIME_CHAR, dbr.CTRL_CHAR) and
                count < AUTOMONITOR_MAXLENGTH):
            val = strjoin('', [chr(i) for i in val if i > 0]).strip()
        elif ftype == dbr.ENUM and count == 1:
            val = get_enum_strings(chid)[val]
        elif count > 1:
            val = '<array count=%d, type=%d>' % (count, ftype)
        val = str(val)
    except ValueError:
        pass
    return val


@withConnectedCHID
@asyncio.coroutine
def put(chid, value, wait=False, timeout=30, callback=None,
        callback_data=None):
    """sets the Channel to a value, with options to either wait
    (block) for the processing to complete, or to execute a
    supplied callback function when the process has completed.


    Parameters
    ----------
    chid :  ctypes.c_long
        Channel ID
    wait : bool
        whether to wait for processing to complete (or time-out)
        before returning.
    timeout : float
        maximum time to wait for processing to complete before returning
        anyway.
    callback : ``None`` of callable
        user-supplied function to run when processing has completed.
    callback_data :  object
        extra data to pass on to a user-supplied callback function.

    Returns
    -------
    status : int
         1  for success, -1 on time-out

    Notes
    -----
    1. Specifying a callback will override setting `wait=True`.

    2. A put-callback function will be called with keyword arguments
        pvname=pvname, data=callback_data

    """
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
    if ftype == dbr.CHAR and nativecount > 1 and is_string_or_bytes(value):
        count += 1

    # if needed (python3, especially) convert to basic string/bytes form
    if is_string(value):
        if value == '':
            value = NULLCHAR
        value = ascii_string(value)

    data = (count * dbr.Map[ftype])()
    if ftype == dbr.STRING:
        if count == 1:
            data[0].value = value
        else:
            for elem in range(min(count, len(value))):
                data[elem].value = value[elem]
    elif nativecount == 1:
        if ftype == dbr.CHAR:
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
        if ftype == dbr.CHAR and is_string_or_bytes(value):
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

    # simple put, without wait or callback
    if not (wait or callable(callback)):
        ret = libca.ca_array_put(ftype, count, chid, data)
        PySEVCHK('put', ret)
        poll()
        return ret

    future = asyncio.Future()
    _pending_futures.add(future)

    # i think this would run in the epics thread, not a good thing
    # if callable(callback):
    #     print('callback is', callback)
    #     future.add_done_callback(partial(callback, pvname=name(chid),
    #                                      data=callback_data))

    ret = libca.ca_array_put_callback(ftype, count, chid,
                                      data, _onPutEvent.ca_callback,
                                      ctypes.py_object(future))

    PySEVCHK('put', ret)

    try:
        ret = yield from asyncio.wait_for(future, timeout=timeout)
    except asyncio.TimeoutError:
        future.cancel()
        raise

    if callable(callback):
        callback(pvname=name(chid), data=callback_data)

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

    future = asyncio.Future()
    _pending_futures.add(future)

    ftype = promote_type(chid, use_ctrl=True)

    ret = libca.ca_array_get_callback(ftype, 1, chid,
                                      _onGetEvent.ca_callback,
                                      ctypes.py_object(future))

    PySEVCHK('get_ctrlvars', ret)

    out = {}
    for attr in ('precision', 'units', 'severity', 'status',
                 'upper_disp_limit', 'lower_disp_limit',
                 'upper_alarm_limit', 'upper_warning_limit',
                 'lower_warning_limit', 'lower_alarm_limit',
                 'upper_ctrl_limit', 'lower_ctrl_limit'):
        if hasattr(value, attr):
            out[attr] = getattr(value, attr, None)
            if attr == 'units':
                out[attr] = BYTES2STR(getattr(value, attr, None))

    if (hasattr(value, 'strs') and hasattr(value, 'no_str') and
            value.no_str > 0):
        out['enum_strs'] = tuple([BYTES2STR(value.strs[i].value)
                                  for i in range(value.no_str)])
    ncache['ctrl_value'] = None
    return out


@withConnectedCHID
@asyncio.coroutine
def get_timevars(chid, timeout=5.0, warn=True):
    """returns a dictionary of TIME fields for a Channel.
    This will contain keys of  *status*, *severity*, and *timestamp*.
    """
    global _cache

    future = asyncio.Future()
    _pending_futures.add(future)

    ftype = promote_type(chid, use_time=True)
    ret = libca.ca_array_get_callback(ftype, 1, chid,
                                      _onGetEvent.ca_callback,
                                      ctypes.py_object(future))

    PySEVCHK('get_timevars', ret)

    try:
        value = yield from asyncio.wait_for(future, timeout=timeout)
    except asyncio.TimeoutError as ex:
        future.cancel()
        raise

    for attr in ('status', 'severity'):
        if hasattr(value, attr):
            out[attr] = getattr(value, attr)

    if hasattr(value, 'stamp'):
        out['timestamp'] = dbr.make_unixtime(value.stamp)

    return out


def get_timestamp(chid):
    """return the timestamp of a Channel -- the time of last update."""
    return get_timevars(chid).get('timestamp', 0)


def get_severity(chid):
    """return the severity of a Channel."""
    return get_timevars(chid).get('severity', 0)


def get_precision(chid):
    """return the precision of a Channel.  For Channels with
    native type other than FLOAT or DOUBLE, this will be 0"""
    if field_type(chid) in (dbr.FLOAT, dbr.DOUBLE):
        return get_ctrlvars(chid).get('precision', 0)
    return 0


def get_enum_strings(chid):
    """return list of names for ENUM states of a Channel.  Returns
    None for non-ENUM Channels"""
    if field_type(chid) == dbr.ENUM:
        return get_ctrlvars(chid).get('enum_strs', None)
    return None

##
# Default mask for subscriptions (means update on value changes
# exceeding MDEL, and on alarm level changes.) Other option is
# dbr.DBE_LOG for archive changes (ie exceeding ADEL)
DEFAULT_SUBSCRIPTION_MASK = dbr.DBE_VALUE | dbr.DBE_ALARM


@withConnectedCHID
def create_subscription(chid, use_time=False, use_ctrl=False,
                        mask=None, callback=None):
    """create a *subscription to changes*. Sets up a user-supplied
    callback function to be called on any changes to the channel.

    Parameters
    -----------
    chid  : ctypes.c_long
        channel ID
    use_time : bool
        whether to use the TIME variant for the PV type
    use_ctrl : bool
        whether to use the CTRL variant for the PV type
    mask : integer or None
       bitmask combination of :data:`dbr.DBE_ALARM`, :data:`dbr.DBE_LOG`, and
       :data:`dbr.DBE_VALUE`, to control which changes result in a callback.
       If ``None``, defaults to :data:`DEFAULT_SUBSCRIPTION_MASK`.

    callback : ``None`` or callable
        user-supplied callback function to be called on changes

    Returns
    -------
    (callback_ref, user_arg_ref, event_id)

        The returned tuple contains *callback_ref* an *user_arg_ref* which
        are references that should be kept for as long as the subscription
        lives (otherwise they may be garbage collected, causing no end of
        trouble).  *event_id* is the id for the event (useful for clearing
        a subscription).

    Notes
    -----
    Keep the returned tuple in named variable!! if the return argument
    gets garbage collected, a coredump will occur.

    """

    mask = mask or DEFAULT_SUBSCRIPTION_MASK

    ftype = promote_type(chid, use_ctrl=use_ctrl, use_time=use_time)

    uarg = ctypes.py_object(callback)
    evid = ctypes.c_void_p()
    poll()
    ret = libca.ca_create_subscription(ftype, 0, chid, mask,
                                       _CB_EVENT, uarg, ctypes.byref(evid))
    PySEVCHK('create_subscription', ret)

    poll()
    return (_CB_EVENT, uarg, evid)


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
            create_context()
        else:
            use_initial_context()
        Thread.run(self)
