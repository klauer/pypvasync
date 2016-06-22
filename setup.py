#!/usr/bin/env python
# from distutils.core import setup
from setuptools import setup

import os
import sys
import pvasync

import versioneer
versioneer.VCS = 'git'
versioneer.versionfile_source = 'pvasync/_version.py'
versioneer.versionfile_build = 'pvasync/_version.py'
versioneer.tag_prefix = ''
versioneer.parentdir_prefix = 'pvasync-'


long_desc = '''Python 3 Interface to the Epics Channel Access protocol
of the Epics control system.'''

#
no_libca="""*******************************************************
*** WARNING - WARNING - WARNING - WARNING - WARNING ***

       Could not find CA dynamic library!

A dynamic library (libca.so or libca.dylib) for EPICS CA
must be found in order for EPICS calls to work properly.

Please read the INSTALL inststructions, and fix this
problem before tyring to use the epics package.
*******************************************************
"""

# for Windows, we provide dlls
data_files = None
if os.name == 'nt':
    try:
        import platform
        nbits = platform.architecture()[0]
    except:
        nbits = '32bit'
    if nbits.startswith('64'):
        data_files = [('DLLs', ['dlls/win64/ca.dll','dlls/win64/Com.dll']),
                      ('', ['dlls/win64/carepeater.exe'])]
    else:
        data_files = [('DLLs', ['dlls/win32/ca.dll','dlls/win32/Com.dll']),
                      ('', ['dlls/win32/carepeater.exe'])]


setup(name='pypvasync',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      license='Epics Open License',
      description="Epics Channel Access for Python",
      long_description=long_desc,
      platforms=['Windows', 'Linux', 'Mac OS X'],
      classifiers=['Intended Audience :: Science/Research',
                      'Operating System :: OS Independent',
                      'Programming Language :: Python',
                      'Topic :: Scientific/Engineering'],
      packages=['pvasync'],
      data_files=data_files )


try:
    libca = pvasync.find_libca.find_libca()
    print("\n  Will use CA library at:  %s \n\n" % libca)
except Exception as ex:
    print("%s" % no_libca)
