import asyncio
import functools
import logging

from epics import coroutines

from . import pvnames

loop = asyncio.get_event_loop()
logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)


def no_simulator_updates(coroutine):
    '''Context manager which pauses and resumes simulator PV updating'''
    @functools.wraps(coroutine)
    def inner(self, *args, **kwargs):
        try:
            logger.debug('Pausing updating of simulator PVs')
            print('* Pausing updating of simulator PVs')
            yield from coroutines.caput(pvnames.pause_pv, 1)
            yield from coroutine(self, *args, **kwargs)
        finally:
            logger.debug('Resuming updating of simulator PVs')
            print('* Resuming updating of simulator PVs')
            yield from coroutines.caput(pvnames.pause_pv, 0)

    return inner


def async_test(coroutine):
    '''Blocks to test a coroutine's functionality'''
    @functools.wraps(coroutine)
    def wrapped(self, *args, **kwargs):
        future = coroutine(self, *args, **kwargs)
        loop.run_until_complete(future)

    return wrapped
