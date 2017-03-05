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
      )
