import pytest
import logging
import functools
from pvasync.sync import (caget, caput, blocking_mode,
                          _cleanup)

from . import pvnames


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def setup_module(module):
    blocking_mode()


def teardown_module(module):
    if False:
        cleanup()
    pass


def no_simulator_updates(fcn):
    '''Context manager which pauses and resumes simulator PV updating'''
    @functools.wraps(fcn)
    def inner(*args, **kwargs):
        try:
            logger.debug('Pausing updating of simulator PVs')
            print('* Pausing updating of simulator PVs')
            caput(pvnames.pause_pv, 1)
            return fcn(*args, **kwargs)
        finally:
            logger.debug('Resuming updating of simulator PVs')
            print('* Resuming updating of simulator PVs')
            caput(pvnames.pause_pv, 0)

    return inner


def test_caget():
    value = caget(pvnames.double_pv)
    assert value is not None


@no_simulator_updates
def test_caput():
    caput(pvnames.enum_pv, 'Stop')
    assert caget(pvnames.enum_pv) == 0
