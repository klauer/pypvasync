from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


import time
import sys

if sys.version_info[0] < 3:
    raise ImportError("Python version 3 required")

from . import context
from . import pv
from . import alarm

from .pv import PV
from .alarm import (Alarm,
                    NO_ALARM, MINOR_ALARM, MAJOR_ALARM, INVALID_ALARM)
from .sync import (caget, caput, caget_many, blocking_mode)
