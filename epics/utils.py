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


def BYTES2STR(st1):
    '''byte to string conversion'''
    if isinstance(st1, bytes):
        return str(st1, 'latin-1')
    return str(st1)


def strjoin(sep, seq):
    "join string sequence with a separator"
    if isinstance(sep, bytes):
        sep = BYTES2STR(sep)
    if len(seq) == 0:
        seq = ''
    elif isinstance(seq[0], bytes):
        tmp = []
        for i in seq:
            if i == NULLCHAR:
                break
            tmp.append(BYTES2STR(i))
        seq = tmp
    return sep.join(seq)


def is_string(s):
    return isinstance(s, str)


def is_string_or_bytes(s):
    return isinstance(s, str) or isinstance(s, bytes)


def ascii_string(s):
    return s.encode('latin-1')


PY64_WINDOWS = (os.name == 'nt' and architecture()[0].startswith('64'))
