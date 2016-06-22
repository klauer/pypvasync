#!/usr/bin/env python
# test expression parsing
import os
import time
import numpy
import ctypes
import asyncio
from epics import (ca, dbr, coroutines)
from epics.context import get_current_context
from epics.errors import ChannelAccessException
import pytest

from .util import (no_simulator_updates, async_test)
from . import pvnames


@asyncio.coroutine
def _ca_connect(chid, timeout=5.0):
    n = 0
    t0 = time.time()
    conn = (2 == ca.state(chid))
    while (not conn) and (time.time() - t0 < timeout):
        yield from asyncio.sleep(0.1)
        conn = (2 == ca.state(chid))
        n += 1
    return conn, time.time()-t0, n


_connection_dict = {}
_change_dict = {}


# API: conn kwarg is now connected
def onConnect(pvname=None, connected=None, chid=None,  **kws):
    print('  [onConnect] Connection status changed:  %s %s kw=%s'
          '' % (pvname, connected, repr(kws)))
    _connection_dict[pvname] = connected


def onChanges(pvname=None, value=None, **kws):
    print('[onChanges] %s=%s, kw=%s' % (pvname, str(value), repr(kws)))
    _change_dict[pvname] = value


@pytest.fixture
def ctx():
    return get_current_context()


def testA_CreateChid(ctx):
    chid = ctx.create_channel(pvnames.double_pv)
    assert chid is not None


@async_test
@asyncio.coroutine
def testA_GetNonExistentPV(ctx):
    print('Simple Test: get on a non-existent PV')
    chid = ctx.create_channel('Definitely-Not-A-Real-PV')
    with pytest.raises(ChannelAccessException):
        yield from coroutines.get(chid)


@async_test
@asyncio.coroutine
def testA_CreateChid_CheckTypeCount(ctx):
    print('Simple Test: create chid, check count, type, host, and access')
    chid = ctx.create_channel(pvnames.double_pv)
    yield from ctx.connect_channel(chid)

    ftype = ca.field_type(chid)
    count = ca.element_count(chid)
    host = ca.host_name(chid)
    rwacc = ca.access(chid)

    assert chid is not None
    assert host is not None
    assert count == 1
    assert ftype == 6
    assert rwacc == 'read/write'


@async_test
@asyncio.coroutine
def testA_CreateChidWithConn(ctx):
    print('Simple Test: create chid with conn callback')
    chid = ctx.create_channel(pvnames.int_pv, callback=onConnect)
    print('created')
    yield from ctx.connect_channel(chid)
    print('connected')
    yield from coroutines.get(chid)
    print('got')

    conn = _connection_dict.get(pvnames.int_pv, None)
    print('conn dat is', _connection_dict)
    assert conn


@async_test
@asyncio.coroutine
def test_Connect1(ctx):
    chid = ctx.create_channel(pvnames.double_pv)
    conn, dt, n = yield from _ca_connect(chid, timeout=2)
    print('CA Connection Test1: connect to existing PV')
    print(' connected in %.4f sec' % (dt))
    assert conn


@async_test
@asyncio.coroutine
def test_Connected(ctx):
    pvn = pvnames.double_pv
    chid = ctx.create_channel(pvn)
    yield from ctx.connect_channel(chid)
    isconn = ca.is_connected(chid)
    print('CA test Connected (%s) = %s' % (pvn, isconn))
    assert isconn
    state = ca.state(chid)
    assert state == dbr.ConnStatus.CS_CONN
    acc = ca.access(chid)
    assert acc == 'read/write'


@async_test
@asyncio.coroutine
def test_DoubleVal(ctx):
    pvn = pvnames.double_pv
    chid = ctx.create_channel(pvn)
    yield from ctx.connect_channel(chid)
    cdict = yield from coroutines.get_ctrlvars(chid)
    print('CA testing CTRL Values for a Double (%s)' % (pvn))
    assert 'units' in cdict
    assert 'precision' in cdict
    assert 'severity' in cdict

    hostname = ca.host_name(chid)
    assert len(hostname) > 1

    count = ca.element_count(chid)
    assert count == 1

    ftype = ca.field_type(chid)
    assert ftype == dbr.ChannelType.DOUBLE

    prec = yield from coroutines.get_precision(chid)
    assert prec == pvnames.double_pv_prec

    cvars = yield from coroutines.get_ctrlvars(chid)
    units = cvars['units']
    assert units == pvnames.double_pv_units

    rwacc = ca.access(chid)
    assert rwacc.startswith('read')


@async_test
@asyncio.coroutine
def test_UnConnected(ctx):
    print('CA Connection Test1: connect to non-existing PV (2sec timeout)')
    chid = ctx.create_channel('impossible_pvname_certain_to_fail')
    conn, dt, n = yield from _ca_connect(chid, timeout=2)
    assert not conn


@async_test
@asyncio.coroutine
def test_putwait(ctx):
    'test put with wait'
    pvn = pvnames.non_updating_pv
    chid = ctx.create_channel(pvn)
    yield from ctx.connect_channel(chid)
    yield from coroutines.put(chid, -1)
    val = yield from coroutines.get(chid)
    assert val == -1
    yield from coroutines.put(chid, 2)
    val = yield from coroutines.get(chid)
    assert val == 2


@async_test
@asyncio.coroutine
def test_promote_type(ctx):
    pvn = pvnames.double_pv
    chid = ctx.create_channel(pvn)
    yield from ctx.connect_channel(chid)
    print('CA promote type (%s)' % (pvn))
    f_t = dbr.promote_type(ca.field_type(chid), use_time=True)
    f_c = dbr.promote_type(ca.field_type(chid), use_ctrl=True)
    assert f_t == dbr.ChannelType.TIME_DOUBLE
    assert f_c == dbr.ChannelType.CTRL_DOUBLE


@async_test
@asyncio.coroutine
def test_ProcPut(ctx):
    pvn = pvnames.enum_pv
    chid = ctx.create_channel(pvn)
    yield from ctx.connect_channel(chid)
    print('CA test put to PROC Field (%s)' % (pvn))
    for input in (1, '1', 2, '2', 0, '0', 50, 1):
        ret = None
        try:
            ret = yield from coroutines.put(chid, 1)
        except:
            pass
        assert ret is not None


@async_test
@asyncio.coroutine
def test_subscription_double(ctx):
    pvn = pvnames.updating_pv1
    chid = ctx.create_channel(pvn)
    yield from ctx.connect_channel(chid)
    handler, cbid = ctx.subscribe(sig='monitor', chid=chid, func=onChanges)

    start_time = time.time()
    global _change_dict
    while time.time()-start_time < 5.0:
        yield from asyncio.sleep(0.01)
        if _change_dict.get(pvn, None) is not None:
            break
    val = _change_dict.get(pvn, None)
    ctx.unsubscribe(cbid)
    assert val is not None


@async_test
@asyncio.coroutine
def test_subscription_custom(ctx):
    pvn = pvnames.updating_pv1
    chid = ctx.create_channel(pvn)
    yield from ctx.connect_channel(chid)

    global change_count
    change_count = 0

    def my_callback(pvname=None, value=None, **kws):
        print(' Custom Callback  %s  value=%s' % (pvname, str(value)))
        global change_count
        change_count = change_count + 1

    handler, cbid = ctx.subscribe(sig='monitor', chid=chid, func=my_callback)

    yield from asyncio.sleep(2.0)

    ctx.unsubscribe(cbid)
    yield from asyncio.sleep(0.2)
    assert change_count > 2


@async_test
@asyncio.coroutine
def test_subscription_str(ctx):
    pvn = pvnames.updating_str1
    print(" Subscription on string: %s " % pvn)
    chid = ctx.create_channel(pvn)
    yield from ctx.connect_channel(chid)
    handler, cbid = ctx.subscribe(sig='monitor', chid=chid, func=onChanges)

    start_time = time.time()
    global _change_dict
    while (time.time() - start_time) < 3.0:
        yield from asyncio.sleep(0.01)
        yield from coroutines.put(chid, "%.1f" % (time.time() - start_time))
        if _change_dict.get(pvn, None) is not None:
            break
    val = _change_dict.get(pvn, None)
    # ca.clear_subscription(eventID)
    ctx.unsubscribe(cbid)
    assert val is not None
    yield from asyncio.sleep(0.2)


@async_test
@asyncio.coroutine
@pytest.mark.parametrize(
    'arrayname, array_type, length, element_type',
    [(pvnames.long_arr_pv, numpy.ndarray, 2048, numpy.int32),
     (pvnames.double_arr_pv, numpy.ndarray, 2048, numpy.float64),
     (pvnames.string_arr_pv, list, 128, str),
     (pvnames.char_arr_pv, numpy.ndarray, 128, numpy.uint8),
     ],
    )
def test_array_callbacks(ctx, arrayname, array_type, length, element_type):
    """ Helper function to subscribe to a PV array and check it
    receives at least one subscription callback w/ specified type,
    length & uniform element type. Checks separately for normal,
    TIME & CTRL subscription variants. Returns the array or fails
    an assertion."""
    results = {}
    for form in ['normal', 'time', 'ctrl']:
        chid = ctx.create_channel(arrayname)
        yield from ctx.connect_channel(chid)
        ftype = dbr.promote_type(ca.field_type(chid),
                                 use_time=(form == 'time'),
                                 use_ctrl=(form == 'ctrl'))
        handler, cbid = ctx.subscribe(sig='monitor', chid=chid, ftype=ftype,
                                      func=onChanges)

        _change_dict.pop(arrayname, None)
        timeout = 0
        # wait up to 6 seconds, if no callback probably due to simulator.py
        # not running...
        while timeout < 120 and not arrayname in _change_dict:
            yield from asyncio.sleep(0.05)
            timeout = timeout+1
        val = _change_dict.get(arrayname, None)
        ctx.unsubscribe(cbid)
        assert val is not None
        assert type(val) == array_type
        assert len(val) == length
        assert type(val[0]) == element_type
        assert all(type(e) == element_type for e in val)
        results[form] = val
    return results


# def test_subscription_string_array(ctx):
#     """ Check that string array callbacks successfully send correct data """
#     results = _array_callback(ctx,
#     assert len(results["normal"][0]) > 0
#     assert len(results["time"][0]) > 0
#     assert len(results["ctrl"][0]) > 0


@async_test
@asyncio.coroutine
@no_simulator_updates
def test_Values(ctx):
    print('CA test Values (compare 5 values with caget)')
    os.system('rm ./caget.tst')
    vals = {}
    for pvn in (pvnames.str_pv,  pvnames.int_pv,
                pvnames.float_pv, pvnames.enum_pv,
                pvnames.long_pv):
        os.system('caget  -n -f5 %s >> ./caget.tst' % pvn)
        chid = ctx.create_channel(pvn)
        yield from ctx.connect_channel(chid)
        vals[pvn] = yield from coroutines.get(chid)
    rlines = open('./caget.tst', 'r').readlines()
    for line in rlines:
        pvn, sval = [i.strip() for i in line[:-1].split(' ', 1)]
        tval = str(vals[pvn])
        if pvn in (pvnames.float_pv, pvnames.double_pv):
            # use float precision!
            tval = "%.5f" % vals[pvn]
        assert tval == sval


@async_test
@asyncio.coroutine
@no_simulator_updates
def test_type_conversions_1(ctx):
    print("CA type conversions scalars")
    pvlist = (pvnames.str_pv, pvnames.int_pv, pvnames.float_pv,
              pvnames.enum_pv,  pvnames.long_pv,  pvnames.double_pv2)
    chids = []
    for name in pvlist:
        chid = ctx.create_channel(name)
        yield from ctx.connect_channel(chid)
        chids.append((chid, name))
        # yield from asyncio.sleep(0.1)

    values = {}
    for chid, name in chids:
        values[name] = yield from coroutines.get(chid, as_string=True)

    for promotion in ('ctrl', 'time'):
        for chid, pvname in chids:
            print('=== %s  chid=%s as %s' % (ca.name(chid), repr(chid),
                                             promotion))
            yield from asyncio.sleep(0.01)
            if promotion == 'ctrl':
                ntype = dbr.promote_type(ca.field_type(chid), use_ctrl=True)
            else:
                ntype = dbr.promote_type(ca.field_type(chid), use_time=True)

            val = yield from coroutines.get(chid, ftype=ntype)
            cval = yield from coroutines.get(chid, as_string=True)
            if ca.element_count(chid) > 1:
                val = val[:12]
            assert cval == values[pvname]


@async_test
@asyncio.coroutine
@no_simulator_updates
def test_type_converions_2(ctx):
    print("CA type conversions arrays")
    pvlist = (pvnames.char_arr_pv,
              pvnames.long_arr_pv,
              pvnames.double_arr_pv)
    chids = []
    for name in pvlist:
        chid = ctx.create_channel(name)
        yield from ctx.connect_channel(chid)
        chids.append((chid, name))
        # yield from asyncio.sleep(0.1)

    values = {}
    for chid, name in chids:
        values[name] = yield from coroutines.get(chid)
    for promotion in ('ctrl', 'time'):
        for chid, pvname in chids:
            print('=== %s  chid=%s as %s' % (ca.name(chid), repr(chid),
                                             promotion))
            yield from asyncio.sleep(0.01)
            if promotion == 'ctrl':
                ntype = dbr.promote_type(ca.field_type(chid), use_ctrl=True)
            else:
                ntype = dbr.promote_type(ca.field_type(chid), use_time=True)

            val = yield from coroutines.get(chid, ftype=ntype)
            yield from coroutines.get(chid, as_string=True)
            for a, b in zip(val, values[pvname]):
                assert a == b


@async_test
@asyncio.coroutine
def test_Array0(ctx):
    print('Array Test: get double array as numpy array, ctypes Array, and list')
    chid = ctx.create_channel(pvnames.double_arrays[0])
    yield from ctx.connect_channel(chid)
    aval = yield from coroutines.get(chid)
    cval = yield from coroutines.get(chid, as_numpy=False)

    assert isinstance(aval, numpy.ndarray)
    assert len(aval) > 2

    assert isinstance(cval, ctypes.Array)
    assert len(cval) > 2
    lval = list(cval)
    assert isinstance(lval, list)
    assert len(lval) > 2
    assert lval == list(aval)


@async_test
@asyncio.coroutine
def test_xArray1(ctx):
    chid = ctx.create_channel(pvnames.double_arrays[0])
    val0 = yield from coroutines.get(chid)
    # aval = yield from coroutines.get(chid, wait=False)
    # assert aval is None
    # NOTE: not a useful test anymore
    val1 = yield from coroutines.get(chid)
    assert all(val0 == val1)


@async_test
@asyncio.coroutine
def test_xArray2(ctx):
    print('Array Test: get fewer than max vals using ca.get(count=0)')
    chid = ctx.create_channel(pvnames.double_arrays[0])
    maxpts = ca.element_count(chid)
    npts = int(max(2, maxpts/2.3 - 1))
    print('max points is %s' % (maxpts, ))
    dat = numpy.random.normal(size=npts)
    print('setting array to a length of npts=%s' % (npts, ))
    yield from coroutines.put(chid, dat)
    out1 = yield from coroutines.get(chid)
    assert isinstance(out1, numpy.ndarray)
    assert len(out1) == npts
    out2 = yield from coroutines.get(chid, count=0)
    assert isinstance(out2, numpy.ndarray)
    assert len(out2) == npts


@async_test
@asyncio.coroutine
def test_xArray3(ctx):
    print('Array Test: get char array as string')
    chid = ctx.create_channel(pvnames.char_arrays[0])
    val = yield from coroutines.get(chid)
    assert isinstance(val, numpy.ndarray)
    char_val = yield from coroutines.get(chid, as_string=True)
    assert isinstance(char_val, str)
    conv = ''.join([chr(i) for i in val])
    assert conv == char_val
