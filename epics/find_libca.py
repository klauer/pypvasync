import ctypes
import ctypes.util

import os
import sys
from . import dbr
from . import config

from .errors import ChannelAccessException


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
        known_hosts = {'Linux': ('linux-x86', 'linux-x86_64'),
                       'Darwin': ('darwin-ppc', 'darwin-x86'),
                       'SunOS': ('solaris-sparc', 'solaris-sparc-gnu')}

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
    initial_context : object
        the initial CA context

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
    if os.name == 'nt':
        load_dll = ctypes.windll.LoadLibrary
    try:
        libca = load_dll(dllname)
    except Exception as exc:
        raise ChannelAccessException(
            'loading Epics CA DLL failed: ' + str(exc))

    ca_context = {False: 0, True: 1}[config.PREEMPTIVE_CALLBACK]
    ret = libca.ca_context_create(ca_context)
    if ret != dbr.ECA.NORMAL:
        raise ChannelAccessException('cannot create Epics CA Context')

    # set argtypes and non-default return types
    # for several libca functions here
    libca.ca_pend_event.argtypes = [ctypes.c_double]
    libca.ca_pend_io.argtypes = [ctypes.c_double]
    libca.ca_client_status.argtypes = [ctypes.c_void_p, ctypes.c_long]

    libca.ca_attach_context.argtypes = [ctypes.c_void_p]
    libca.ca_current_context.restype = ctypes.c_void_p
    libca.ca_version.restype = ctypes.c_char_p
    libca.ca_host_name.restype = ctypes.c_char_p
    libca.ca_name.restype = ctypes.c_char_p
    # libca.ca_name.argtypes    = [dbr.chid_t]
    # libca.ca_state.argtypes   = [dbr.chid_t]
    # libca.ca_clear_channel.argtypes = [dbr.chid_t]

    libca.ca_message.restype = ctypes.c_char_p

    # save value offests used for unpacking
    # TIME and CTRL data as an array in dbr module
    dbr.value_offset = (39 * ctypes.c_short).in_dll(libca, 'dbr_value_offset')
    initial_context = libca.ca_current_context()
    if isinstance(initial_context, ctypes.c_long):
        initial_context = initial_context.value
    return libca, initial_context
