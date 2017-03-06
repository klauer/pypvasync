#!/usr/bin/env python
# unit-tests for pv interface

import unittest
import asyncio
import numpy as np
from .util import (no_simulator_updates, async_test)
from . import pvnames

from pvasync import PV
from pvasync.coroutines import (caget, caput)
from unittest import mock


class PV_Tests(unittest.TestCase):
    def testA_CreatePV(self):
        '''Simple Test: create pv'''
        pv = PV(pvnames.double_pv)
        self.assertIsNot(pv, None)

    @async_test
    @asyncio.coroutine
    def testA_CreatedWithConn(self):
        '''Simple Test: create pv with conn callback'''
        on_connect = mock.Mock()
        pv = PV(pvnames.int_pv, connection_callback=on_connect)
        value = yield from pv.aget()

        on_connect.assert_called_with(pvname=pvnames.int_pv, connected=True,
                                      pv=pv)

    @async_test
    @asyncio.coroutine
    def test_caget(self):
        '''Simple Test of caget() function'''
        pvs = (pvnames.double_pv, pvnames.enum_pv, pvnames.str_pv)
        for p in pvs:
            val = yield from caget(p)
            self.assertIsNot(val, None)

        sval = yield from caget(pvnames.str_pv)
        self.assertEqual(sval, 'ao')

    @async_test
    @no_simulator_updates
    @asyncio.coroutine
    def test_get1(self):
        '''Simple Test: test value and char_value on an integer'''
        pv = PV(pvnames.int_pv)
        val = yield from pv.aget()
        cval = yield from pv.aget(as_string=True)

        self.assertEquals(int(cval), val)

    @async_test
    @no_simulator_updates
    @asyncio.coroutine
    def test_get_string_waveform(self):
        '''String Array: '''
        pv = PV(pvnames.string_arr_pv)
        val = yield from pv.aget()
        self.assertGreater(len(val), 10)
        self.assertIsInstance(val[0], str)
        self.assertGreater(len(val[0]), 1)
        self.assertIsInstance(val[1], str)
        self.assertGreater(len(val[1]), 1)

    @async_test
    @no_simulator_updates
    @asyncio.coroutine
    def test_put_string_waveform(self):
        '''String Array: '''
        pv = PV(pvnames.string_arr_pv)
        put_value = ['a', 'b', 'c']
        yield from pv.aput(put_value)
        get_value = yield from pv.aget(use_monitor=False, count=len(put_value))
        np.testing.assert_array_equal(get_value, put_value)

    @async_test
    def test_timeout(self):
        '''Ensure a timeout happens with wait_for_connection'''
        p = PV('absolutely_made_up_pvname_seriously')
        try:
            yield from p.wait_for_connection(timeout=0.1)
        except asyncio.TimeoutError:
            pass
        else:
            self.fail('Did not timeout')

    @async_test
    def test_putcomplete(self):
        '''Put with wait and put_complete (using real motor!) '''
        vals = (1.35, 1.50, 1.44, 1.445, 1.45, 1.453, 1.446, 1.447, 1.450,
                1.450, 1.490, 1.5, 1.500)
        p = PV(pvnames.motor1)
        # this works with a real motor, fail if it doesn't connect quickly
        try:
            yield from p.wait_for_connection(timeout=0.2)
        except asyncio.TimeoutError:
            self.skipTest('Unable to connect to real motor record')

        retry_deadband = yield from caget('{}.RDBD'.format(pvnames.motor1))

        for v in vals:
            done_callback = mock.Mock()
            yield from p.aput(v, use_complete=True, callback=done_callback)
            rbv = yield from p.aget()
            self.assertAlmostEqual(rbv, v, delta=retry_deadband)
            self.assertTrue(done_callback.called)

    @async_test
    def test_get_callback(self):
        print("Callback test:  changing PV must be updated\n")
        mypv = PV(pvnames.updating_pv1)
        callback = mock.Mock()

        print('Added a callback.  Now wait for changes...\n')
        mypv.add_callback(callback)

        yield from asyncio.sleep(1)

        self.assertGreater(callback.call_count, 3)
        print('   saw %i changes.\n' % callback.call_count)
        mypv.clear_callbacks()

    @async_test
    def test_subarrays(self):
        print("Subarray test:  dynamic length arrays\n")
        driver = PV(pvnames.subarr_driver)
        subarr1 = PV(pvnames.subarr1)
        yield from subarr1.wait_for_connection()

        len_full = 64
        len_sub1 = 16
        full_data = np.arange(len_full) / 1.0

        yield from caput("%s.NELM" % pvnames.subarr1, len_sub1)
        yield from caput("%s.INDX" % pvnames.subarr1, 0)

        yield from driver.aput(full_data)
        yield from asyncio.sleep(0.1)
        subval = yield from subarr1.aget()

        self.assertEqual(len(subval), len_sub1)
        np.testing.assert_array_equal(subval, full_data[:len_sub1])
        print("Subarray test:  C\n")
        yield from caput("%s.NELM" % pvnames.subarr2, 19)
        yield from caput("%s.INDX" % pvnames.subarr2, 3)

        subarr2 = PV(pvnames.subarr2)
        yield from subarr2.aget()

        yield from driver.aput(full_data)
        yield from asyncio.sleep(0.1)
        subval = yield from subarr2.aget()

        self.assertEqual(len(subval), 19)
        np.testing.assert_array_equal(subval, full_data[3:3 + 19])

        yield from caput("%s.NELM" % pvnames.subarr2, 5)
        yield from caput("%s.INDX" % pvnames.subarr2, 13)

        yield from driver.aput(full_data)
        yield from asyncio.sleep(0.1)
        subval = yield from subarr2.aget()

        self.assertEqual(len(subval), 5)
        np.testing.assert_array_equal(subval, full_data[13:5+13])

#    # waiting on upstream fix decision
#    @async_test
#    def test_subarray_zerolen_no_monitor(self):
#        # a test of a char waveform of length 1 (NORD=1): value "\0"
#        # without using autom_onitor
#        zerostr = PV(pvnames.char_arr_zeroish_length_pv, auto_monitor=False)
#        yield from zerostr.wait_for_connection()

#        val = yield from zerostr.aget(as_string=True)
#        self.assertEquals(val, '')
#        val = yield from zerostr.aget(as_string=False)
#        self.assertEquals(val, 0)

#     @async_test
#     def test_subarray_zerolen_monitor(self):
#         # a test of a char waveform of length 1 (NORD=1): value "\0"
#         # with using auto_monitor
#         zerostr = PV(pvnames.char_arr_zeroish_length_pv, auto_monitor=True)
#         zerostr.wait_for_connection()

#         self.assertEquals(zerostr.get(as_string=True), '')
#         self.assertEquals(zerostr.get(as_string=False), 0)

    @async_test
    def test_subarray_zerolen(self):
        subarr1 = PV(pvnames.zero_len_subarr1)
        yield from subarr1.wait_for_connection()

        val = yield from subarr1.aget(use_monitor=True, as_numpy=True)
        self.assertIsInstance(val, np.ndarray, msg='using monitor')
        self.assertEquals(len(val), 0, msg='using monitor')
        self.assertEquals(val.dtype, np.float64, msg='using monitor')

        val = yield from subarr1.aget(use_monitor=False, as_numpy=True)
        self.assertIsInstance(val, np.ndarray, msg='no monitor')
        self.assertEquals(len(val), 0, msg='no monitor')
        self.assertEquals(val.dtype, np.float64, msg='no monitor')

    @async_test
    def test_enum_put(self):
        pv = PV(pvnames.enum_pv)
        yield from pv.aput('Stop')
        yield from asyncio.sleep(0.1)
        val = yield from pv.aget()
        self.assertEqual(val, 0)

    @async_test
    def test_DoubleVal(self):
        pvn = pvnames.double_pv
        pv = PV(pvn)
        yield from pv.aget()
        cdict = yield from pv.get_ctrlvars()
        print('Testing CTRL Values for a Double (%s)\n' % (pvn))
        self.assertIn('severity', cdict)
        self.assertEqual(pv.count,1)
        self.assertEqual(pv.precision, pvnames.double_pv_prec)
        self.assertEqual(pv.units, pvnames.double_pv_units)
        self.failUnless(pv.access.startswith('read'))
        self.assertGreater(len(pv.host), 1)

    @async_test
    @no_simulator_updates
    @asyncio.coroutine
    def test_type_conversions_2(self):
        print("CA type conversions arrays\n")
        pvlist = (pvnames.char_arr_pv,
                  pvnames.long_arr_pv,
                  pvnames.double_arr_pv)

        native_pvs = [PV(pv, form='native') for pv in pvlist]
        native_values = {}

        for native_pv in native_pvs:
            native_values[native_pv] = yield from native_pv.aget()
            info = yield from native_pv.get_info()

        for promotion in ('ctrl', 'time'):
            for native_pv, native_value in native_values.items():
                promoted_pv = PV(native_pv.pvname, form=promotion)

                val = yield from promoted_pv.aget(as_numpy=True)
                cval = yield from promoted_pv.aget(as_string=True)

                msg = 'pv {} form={}'.format(native_pv.pvname, promotion)
                if isinstance(val, np.ndarray):
                    np.testing.assert_array_almost_equal(val, native_value,
                                                         err_msg=msg)
                else:
                    self.assertEqual(val, native_value, msg=msg)

    @async_test
    def test_waveform_get_1elem(self):
        pv = PV(pvnames.double_arr_pv)
        val = yield from pv.aget(count=1, use_monitor=False)
        self.assertIsInstance(val, np.ndarray)
        self.assertEquals(len(val), 1)
        # TODO other fix
        # info = yield from pv.get_info()


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase( PV_Tests)
    unittest.TextTestRunner(verbosity=1).run(suite)
