import logging

import asyncio
import pvasync
import threading
from pvasync.coroutines import caget, caput

print(pvasync.__file__)

logger = logging.getLogger(__name__)

logging.getLogger('epics').setLevel(logging.DEBUG)
logging.basicConfig()

pvname = 'XF:31IDA-OP{Tbl-Ax:X1}Mtr.RBV'
write_pvname = 'XF:31IDA-OP{Tbl-Ax:X1}Mtr.VAL'


def monitor(value=None, pvname=None, **kwd):
    print('!!! monitor {}={}'.format(pvname, value))


@asyncio.coroutine
def test_caget():
    mon_pv = pvasync.PV(pvname, auto_monitor=True)
    mon_pv.add_callback(monitor)

    value = yield from caget(pvname)
    print('value', value)

    try:
        value = yield from caget(write_pvname, timeout=1e-9)
        print('value was really fast', value)
    except Exception as ex:
        print('[expected failure] caget:', pvname, ex.__class__.__name__, ex)

    print()
    print('-----------')
    print('move to 1.2')
    yield from caput(write_pvname, 1.2, timeout=5.0)
    yield from asyncio.sleep(0.1)
    value = yield from caget(pvname)
    print('read back', value)

    print()
    print('-----------')
    print('move to 0.9')
    yield from caput(write_pvname, 0.9, timeout=5.0)
    yield from asyncio.sleep(0.1)
    value = yield from caget(pvname)
    print('read back', value)

    pv = pvasync.PV(pvname)
    print('pv created', pv)
    yield from pv.wait_for_connection()
    print('pv connected', pv)
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
    write_pv = pvasync.PV(write_pvname)
    yield from write_pv.put(1.0, callback=move_done, callback_data=thread_id)
    print('main function, move completed')

    assert threading.get_ident() == thread_id[0], 'Callback not in same thread'
    yield from asyncio.sleep(0.1)

    write_pv.disconnect()
    yield from asyncio.sleep(0.5)

loop = asyncio.get_event_loop()
loop.run_until_complete(test_caget())
