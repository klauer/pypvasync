import logging

import asyncio
import epics
import time
import threading
import atexit

print(epics.__file__)

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

    print()
    print('-----------')
    print('move to 1.2')
    yield from epics.caput(write_pvname, 1.2, timeout=2.0)
    yield from asyncio.sleep(0.1)
    value = yield from epics.caget(pvname)
    print('read back', value)

    print()
    print('-----------')
    print('move to 0.9')
    yield from epics.caput(write_pvname, 0.9, timeout=2.0)
    yield from asyncio.sleep(0.1)
    value = yield from epics.caget(pvname)
    print('read back', value)

    pv = epics.PV(pvname)
    value = yield from pv.get(with_ctrlvars=True)
    print('ctrlvars', pv._args)

    def move_done(future, pvname=None, data=None):
        data.append(threading.get_ident())
        print('* [put callback] move completed', pvname, data, future)

    print('main thread ident', threading.get_ident())
    print()
    print('----------------------------------------')
    print('final move to 1.0 with put callback test')

    thread_id = []
    write_pv = epics.PV(write_pvname)
    yield from write_pv.put(1.0, callback=move_done, callback_data=thread_id)

    assert threading.get_ident() == thread_id[0], 'Callback not in same thread'
    yield from asyncio.sleep(0.1)

loop = asyncio.get_event_loop()
loop.run_until_complete(test_caget())
