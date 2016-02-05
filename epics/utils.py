import os
import sys
from platform import architecture

if sys.version_info[0] < 3:
    raise ImportError("Python version 3 required")

PY64_WINDOWS = (os.name == 'nt' and architecture()[0].startswith('64'))
