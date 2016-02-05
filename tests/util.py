import asyncio
import functools
import epics

from contextlib import contextmanager


loop = asyncio.get_event_loop()


@contextmanager
def no_simulator_updates(coroutine):
    '''Context manager which pauses and resumes simulator PV updating'''
    @functools.wraps(coroutine)
    def inner(self, *args, **kwargs):
        try:
            yield from epics.caput(pvnames.pause_pv, 1)
            yield from coroutine()
        finally:
            yield from epics.caput(pvnames.pause_pv, 0)

    return inner


def async_test(coroutine):
    '''Blocks to test a coroutine's functionality'''
    @functools.wraps(coroutine)
    def wrapped(self, *args, **kwargs):
        future = coroutine(self, *args, **kwargs)
        loop.run_until_complete(future)

    return wrapped
