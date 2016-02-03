"""
String and data utils, where implementation differs between Python 2 & 3
"""
import os
import sys
from copy import deepcopy
from platform import architecture
from . import utils3 as utils_mod

STR2BYTES = utils_mod.STR2BYTES
BYTES2STR = utils_mod.BYTES2STR
NULLCHAR = utils_mod.NULLCHAR
NULLCHAR_2 = utils_mod.NULLCHAR_2
strjoin = utils_mod.strjoin
is_string = utils_mod.is_string
is_string_or_bytes = utils_mod.is_string_or_bytes
ascii_string = utils_mod.ascii_string
memcopy = deepcopy

PY64_WINDOWS = (os.name == 'nt' and architecture()[0].startswith('64'))
PY_MAJOR, PY_MINOR = sys.version_info[:2]
