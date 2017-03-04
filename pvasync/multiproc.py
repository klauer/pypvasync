#!/usr/bin/env python
"""
Provides CAProcess, a multiprocessing.Process that correctly handles Channel Access
and CAPool, pool of CAProcesses

Use CAProcess in place of multiprocessing.Process if your process will be calling
Channel Access or using Epics process variables

   from pvasync import (CAProcess, CAPool)

"""
#
# Author:         Ken Lauer
# Created:        Feb. 27, 2014
# Modifications:  Matt Newville, changed to subclass multiprocessing.Process
#                 3/28/2014  KL, added CAPool

import multiprocessing as mp
from multiprocessing.pool import Pool


__all__ = ['CAProcess', 'CAPool']


class CAProcess(mp.Process):
    """
    A Channel-Access aware (and safe) subclass of multiprocessing.Process

    Use CAProcess in place of multiprocessing.Process if your Process will
    be doing CA calls!
    """
    def __init__(self, **kws):
        super().__init__(**kws)

    def run(self):
        # cm = get_contexts()
        # # TODO this is definitely wrong
        # cm.stop()
        # cm.add_context()
        super().run()


class CAPool(Pool):
    """
    An EPICS-aware multiprocessing.Pool of CAProcesses.
    """
    def __init__(self, *args, **kwargs):
        self.Process = CAProcess

        super().__init__(*args, **kwargs)
