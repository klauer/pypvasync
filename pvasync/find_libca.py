import ctypes
import ctypes.util

import os
import sys
from . import dbr
from . import config

from .errors import ChannelAccessException


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

    # dbr.value_offset = (39 * ctypes.c_short).in_dll(libca, 'dbr_value_offset')
    return None, None
