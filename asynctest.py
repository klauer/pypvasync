import logging

import asyncio
import epics
import time
import threading
import atexit


logger = logging.getLogger(__name__)

pvname = 'XF:31IDA-OP{Tbl-Ax:X1}Mtr.RBV'
write_pvname = 'XF:31IDA-OP{Tbl-Ax:X1}Mtr.VAL'


@asyncio.coroutine
def test_caget():
    value = yield from epics.caget(pvname)
    print('value', value)

    try:
        value = yield from epics.caget(write_pvname, timeout=1e-9)
        print('value was really fast', value)
    except Exception as ex:
        print('caget failed, as expected', pvname, ex.__class__.__name__, ex)

    print('move to 1.2')
    yield from epics.caput(write_pvname, 1.2, timeout=2.0)
    yield from asyncio.sleep(0.1)
    value = yield from epics.caget(pvname)
    print('read back', value)

    print('move to 1.0')
    yield from epics.caput(write_pvname, 1.0, timeout=2.0)
    yield from asyncio.sleep(0.1)
    value = yield from epics.caget(pvname)
    print('read back', value)


def threaded_poll(ctx):
    '''Poll context ctx in an executor thread'''
    global running
    epics.ca.attach_context(ctx)
    while running:
        epics.ca.poll()
        time.sleep(0.01)


def wait_pollers():
    global running

    running = False
    for future in poll_threads:
        loop.run_until_complete(future)


loop = asyncio.get_event_loop()
running = True
try:
    poll_threads = [loop.run_in_executor(None, threaded_poll,
                                         epics.ca.current_context())]
    loop.run_until_complete(test_caget())
finally:
    wait_pollers()

    epics.ca.clear_cache()
    epics.ca.detach_context()
    loop.close()
