"""
String and data utils, where implementation differs between Python 2 & 3
"""
import os
import sys
from platform import architecture


if sys.version_info[0] < 3:
    raise ImportError(" Python version 3 required")

NULLCHAR_2 = '\x00'
NULLCHAR = b'\x00'


def STR2BYTES(st1):
    '''string to byte conversion'''
    if isinstance(st1, bytes):
        return st1
    return bytes(st1, 'latin-1')


def ascii_string(s):
    return s.encode('latin-1')


PY64_WINDOWS = (os.name == 'nt' and architecture()[0].startswith('64'))
