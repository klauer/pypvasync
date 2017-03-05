#!/usr/bin/env python
#  M Newville <newville@cars.uchicago.edu>
#  The University of Chicago, 2010
#  Epics Open License

"""
  Epics Process Variable
"""
import time
import copy
import asyncio
import functools

from math import log10
import numpy as np
import caproto
from caproto import ChannelType

from . import config
from .context import get_current_context, AsyncClientChannel
from . import coroutines
from .utils import format_time
from .sync import blocking_wrapper

_PVcache_ = {}


@asyncio.coroutine
def get_pv(pvname, form='time', connect=False, context=None, timeout=5.0,
           **kws):
    """get PV from PV cache or create one if needed.

    Arguments
    =========
    form      PV form: one of 'native' (default), 'time', 'ctrl'
    connect   whether to wait for connection (default False)
    context   PV threading context (default None)
    timeout   connection timeout, in seconds (default 5.0)
    """

    if form not in ('native', 'time', 'ctrl'):
        form = 'native'

    context = get_current_context()
    key = (pvname, form, context)
    thispv = _PVcache_.get(key, None)

    # not cached -- create pv (automaticall saved to cache)
    if thispv is None:
        thispv = PV(pvname, form=form, **kws)

    if connect:
        yield from thispv.wait_for_connection(timeout=timeout)

    return thispv


class PVClientChannel(AsyncClientChannel):
    def __init__(self, *, pv_instance, **kwargs):
        super().__init__(**kwargs)
        self.pv = pv_instance

    def state_changed(self, role, old_state, new_state, command=None):
        super().state_changed(role, old_state, new_state, command=command)

        if role is caproto.SERVER:
            return

        if new_state is caproto.CONNECTED:
            self.pv._on_connect(element_count=self.native_data_count,
                                native_type=self.native_data_type,
                                access_rights=self.access_rights,
                                host=self.circuit.host)
        elif new_state is caproto.MUST_CLOSE:
            self.pv._on_disconnect(deleted=False)


class PV(object):
    """Asyncio access to Epics Process Variables

    A PV encapsulates an Epics Process Variable.

    The primary interface methods for a pv are to get() and put() is value::

      >>> @asyncio.coroutine
      ... def test():
      ...     p = PV(pv_name)             # create a pv object given a pv name
      ...     val = yield from p.aget()   # get pv value
      ...     yield from p.aput(val)       # set pv to specified value.
      ...     return p

    Additional important attributes include::
      >>> p = yield from test()
      >>> p.pvname         # name of pv
      >>> p.value          # pv value (can be set or get)
      >>> p.char_value     # string representation of pv value
      >>> p.count          # number of elements in array pvs
      >>> p.type           # EPICS data type:
                           #  'string','double','enum','long',..
"""

    _fmtsca = ("PV(%(pvname)r, count=%(count)i, type=%(typefull)r, "
               "access=%(access)r>")
    _fmtarr = ("PV(%(pvname)r, count=%(count)i/%(nelm)i, "
               "type=%(typefull)r, access=%(access)r>")

    def __init__(self, pvname, form='time', auto_monitor=None,
                 connection_callback=None, connection_timeout=None,
                 monitor_mask=None, context=None):

        if context is None:
            context = get_current_context()

        self._context = context
        self._connect_event = asyncio.Event()
        self.monitor_mask = monitor_mask
        self.pvname = pvname.strip()
        self.form = form.lower()
        self.auto_monitor = auto_monitor
        self.ftype = None
        self.connected = False
        self.connection_timeout = connection_timeout
        # holder of data returned from create_subscription
        self._mon_cbid = None
        self._conn_started = False
        self.connection_callbacks = ([connection_callback]
                                     if connection_callback else [])
        self.callbacks = {}
        self._args = dict(value=None,
                          pvname=self.pvname,
                          count=-1,
                          precision=None,
                          enum_strs=None)

        self.channel = self._context.create_channel(self.pvname,
                                                    channel_class=PVClientChannel,
                                                    pv_instance=self)
        self.chid = self.channel.cid

        self._args['type'] = None
        self.ftype = None

        pvid = self._pvid
        if pvid not in _PVcache_:
            _PVcache_[pvid] = self

    @property
    def _pvid(self):
        return (self.pvname, self.form, self._context)

    def __hash__(self):
        return hash(self._pvid)

    def _on_connect(self, element_count, native_type, access_rights, host):
        self.chid = self.channel.cid

        if access_rights is None:
            access_rights = 0

        self.ftype = caproto.promote_type(native_type,
                                          use_ctrl=(self.form == 'ctrl'),
                                          use_time=(self.form == 'time'))

        ftype_name = ChannelType(self.ftype).name.lower()

        access_str = (('no access', 'read-only', 'write-only',
                       'read/write')[access_rights])
        self._args.update(
            count=element_count,
            nelm=element_count,
            host=host,
            access=access_str,
            # TODO upstream somewhere?
            read_access=((access_rights & 1) == 1),
            write_access=((access_rights & 2) == 2),
            type=ftype_name,
            typefull=ftype_name,
            ftype=self.ftype,
            )

        if self.auto_monitor is None:
            self.auto_monitor = element_count < config.AUTOMONITOR_MAXLENGTH
        if self._mon_cbid is None and self.auto_monitor:
            # you can explicitly request a subscription mask (ie
            # DBE_ALARM|DBE_LOG) by passing it as the auto_monitor arg,
            # otherwise if you specify 'True' you'll just get the default
            # set in ca.DEFAULT_SUBSCRIPTION_MASK
            mask = self.monitor_mask
            use_ctrl = (self.form == 'ctrl')
            use_time = (self.form == 'time')
            ptype = caproto.promote_type(self.ftype, use_ctrl=use_ctrl,
                                         use_time=use_time)

            ctx = self._context
            # handler, cbid = ctx.subscribe(sig='monitor',
            #                               func=self._monitor_update,
            #                               chid=self.chid, ftype=ptype,
            #                               mask=mask)
            # self._mon_cbid = cbid

        self._update_connection_status(connected=True)

    def _on_disconnect(self):
        self._update_connection_status(connected=False)

    def _update_connection_status(self, connected):
        try:
            for conn_cb in self.connection_callbacks:
                conn_cb(pvname=self.pvname, connected=connected, pv=self)
        finally:
            # waiting until the very end until to set self.connected prevents
            # threads from thinking a connection is complete when it is
            # actually still in progress.

            if connected:
                self._connect_event.set()
            else:
                self._connect_event.clear()

            self.connected = connected

    async def wait_for_connection(self, timeout=None):
        """wait for a connection that started with connect() to finish"""

        if self.connected:
            return True

        if timeout is None:
            timeout = self.connection_timeout

        await asyncio.wait_for(self._connect_event.wait(), timeout=timeout)
        return True

    @asyncio.coroutine
    def reconnect(self):
        "try to reconnect PV"
        # TODO not implemented
        self.disconnect()
        self.connected = False
        self._conn_started = False
        yield from self.wait_for_connection()

    async def aget(self, count=None, as_string=False, as_numpy=True,
                   timeout=None, with_ctrlvars=False, use_monitor=True):
        """Asychronously get the current value of the PV.

        Parameters
        ----------
        count : int, optional
            explicitly limit count for array data
        as_string : bool, optional
            get a string representation of the value.
        as_numpy : bool, optional
            use numpy array as the return type for array data.
        timeout : float, optional
            maximum time to wait for value to be received.
                (default = 0.5 + log10(count) seconds)
        use_monitor : bool, optional
            use value from latest monitor callback (True, default) or to make an
            explicit CA call for the value.
        """
        await self.wait_for_connection()

        if with_ctrlvars and self.units is None:
            await self.get_ctrlvars()

        if ((not use_monitor) or
                (not self.auto_monitor) or
                (self._args['value'] is None) or
                (count is not None and count > len(self._args['value']))):
            self._args['value'] = await self.channel.get(ftype=self.ftype,
                                                         count=count,
                                                         timeout=timeout,
                                                         as_numpy=as_numpy)

        val = self._args['value']
        if as_string:
            return self._set_charval(val)
        if self.count <= 1 or val is None:
            return val

        if count is None:
            count = len(val)
        if (as_numpy and not isinstance(val, np.ndarray)):
            if count == 1:
                val = [val]
            val = np.array(val)
        elif (as_numpy and count == 1 and
                not isinstance(val, np.ndarray)):
            val = np.array([val])
        elif (not as_numpy and isinstance(val, np.ndarray)):
            val = list(val)
        # allow asking for less data than actually exists in the cached value
        if count < len(val):
            val = val[:count]
        return val

    async def aput(self, value, timeout=30.0, callback=None,
                   callback_data=None):
        """set value for PV

        Optionally waiting until the processing is complete, and optionally
        specifying a callback function to be run when the processing is
        complete.
        """
        await self.wait_for_connection()

        if self.ftype in caproto.enum_types and isinstance(value, str):
            enum_strs = self._args['enum_strs']
            if enum_strs is None:
                ctrlvars = await self.get_ctrlvars()
                enum_strs = ctrlvars.enum_strs

            if value in self._args['enum_strs']:
                # tuple.index() not supported in python2.5
                # value = self._args['enum_strs'].index(value)
                for ival, val in enumerate(self._args['enum_strs']):
                    if val == value:
                        value = ival
                        break

        fut = self.channel.put(value, timeout=timeout)

        if callable(callback):
            fut.add_done_callback(functools.partial(callback,
                                                    data=callback_data))

        await fut

    get = blocking_wrapper(aget)
    put = blocking_wrapper(aput)

    def _put_callback(self, pvname=None, **kws):
        '''default put-callback function'''
        pass

    def _set_charval(self, val, call_ca=True):
        """ sets the character representation of the value.
        intended only for internal use"""
        if val is None:
            self._args['char_value'] = 'None'
            return 'None'
        ftype = self._args['ftype']
        ntype = caproto.native_type(ftype)
        if ntype == ChannelType.STRING:
            self._args['char_value'] = val
            return val
        # char waveform as string
        if ntype == ChannelType.CHAR and self.count < config.AUTOMONITOR_MAXLENGTH:
            if isinstance(val, np.ndarray):
                val = val.tolist()
            elif self.count == 1:  # handles single character in waveform
                val = [val]
            val = list(val)
            if 0 in val:
                firstnull = val.index(0)
            else:
                firstnull = len(val)
            try:
                cval = ''.join([chr(i) for i in val[:firstnull]]).rstrip()
            except ValueError:
                cval = ''
            self._args['char_value'] = cval
            return cval

        cval = repr(val)
        if self.count > 1:
            typename = ChannelType(ftype).name.lower()
            cval = '<array size=%d, type=%s>' % (len(val), typename)
        elif ntype in caproto.native_float_types:
            if call_ca and self._args['precision'] is None:
                self.get_ctrlvars()
                raise NotImplementedError()
            try:
                prec = self._args['precision']
                fmt = "%%.%if"
                if 4 < abs(int(log10(abs(val + 1.e-9)))):
                    fmt = "%%.%ig"
                cval = (fmt % prec) % val
            except (ValueError, TypeError, ArithmeticError):
                cval = str(val)
        elif ntype == ChannelType.ENUM:
            if call_ca and self._args['enum_strs'] in ([], None):
                self.get_ctrlvars()
            try:
                cval = self._args['enum_strs'][val]
            except (TypeError, KeyError, IndexError):
                cval = str(val)

        self._args['char_value'] = cval
        return cval

    async def get_ctrlvars(self, timeout=5):
        "get control values for variable"
        await self.wait_for_connection(timeout=timeout)
        kwds = await self.channel.get_ctrlvars(timeout=timeout)
        self._args.update(kwds)
        return kwds

    async def get_timevars(self, timeout=5):
        "get time values for variable"
        await self.wait_for_connection()
        kwds = await self.channel.get_timevars(timeout=timeout)
        self._args.update(kwds)
        return kwds

    def _monitor_update(self, value=None, **kwd):
        """internal callback function: do not overwrite!!
        To have user-defined code run when the PV value changes,
        use add_callback()
        """
        self._args.update(kwd)
        self._args['value'] = value
        self._args['timestamp'] = kwd.get('timestamp', time.time())
        self._set_charval(self._args['value'], call_ca=False)
        self.run_callbacks()

    def run_callbacks(self):
        """run all user-defined callbacks with the current data

        Normally, this is to be run automatically on event, but
        it is provided here as a separate function for testing
        purposes.
        """
        for index in sorted(list(self.callbacks.keys())):
            self.run_callback(index)

    def run_callback(self, index):
        """run a specific user-defined callback, specified by index,
        with the current data
        Note that callback functions are called with keyword/val
        arguments including:
             self._args  (all PV data available, keys = __fields)
             keyword args included in add_callback()
             keyword 'cb_info' = (index, self)
        where the 'cb_info' is provided as a hook so that a callback
        function  that fails may de-register itself (for example, if
        a GUI resource is no longer available).
        """
        try:
            fcn, kwargs = self.callbacks[index]
        except KeyError:
            return
        kwd = copy.copy(self._args)
        kwd.update(kwargs)
        kwd['cb_info'] = (index, self)
        if callable(fcn):
            fcn(**kwd)

    def add_callback(self, callback=None, index=None, run_now=False,
                     with_ctrlvars=True, **kw):
        """add a callback to a PV.  Optional keyword arguments
        set here will be preserved and passed on to the callback
        at runtime.

        Note that a PV may have multiple callbacks, so that each
        has a unique index (small integer) that is returned by
        add_callback.  This index is needed to remove a callback."""
        if callable(callback):
            if index is None:
                index = 1
                if len(self.callbacks) > 0:
                    index = 1 + max(self.callbacks.keys())
            self.callbacks[index] = (callback, kw)

        # if with_ctrlvars and self.connected:
        #     self.get_ctrlvars()  # <-- TODO coroutine
        if run_now:
            self.get(as_string=True)
            if self.connected:
                self.run_callback(index)
        return index

    def remove_callback(self, index=None):
        """remove a callback by index"""
        if index in self.callbacks:
            self.callbacks.pop(index)

    def clear_callbacks(self):
        "clear all callbacks"
        self.callbacks = {}

    @asyncio.coroutine
    def get_info(self, timeout=2.0):
        "get information paragraph"
        yield from self.wait_for_connection(timeout=timeout)
        yield from self.get_ctrlvars(timeout=timeout)

        out = []
        mod = 'native'
        xtype = self._args['typefull']
        if '_' in xtype:
            mod, xtype = xtype.split('_')

        fmt = '%i'
        if xtype in ('float', 'double'):
            fmt = '%g'
        elif xtype in ('string', 'char'):
            fmt = '%s'

        self._set_charval(self._args['value'], call_ca=False)
        out.append("== %s  (%s_%s) ==" % (self.pvname, mod, xtype))
        if self.count == 1:
            val = self._args['value']
            out.append('   value      = %s' % fmt % val)
        else:
            ext = {True: '...', False: ''}[self.count > 10]
            elems = range(min(5, self.count))
            try:
                aval = [fmt % self._args['value'][i] for i in elems]
            except TypeError:
                aval = ('unknown',)
            out.append("   value      = array  [%s%s]" % (",".join(aval), ext))
        for nam in ('char_value', 'count', 'nelm', 'type', 'units',
                    'precision', 'host', 'access',
                    'status', 'severity', 'timestamp',
                    'upper_ctrl_limit', 'lower_ctrl_limit',
                    'upper_disp_limit', 'lower_disp_limit',
                    'upper_alarm_limit', 'lower_alarm_limit',
                    'upper_warning_limit', 'lower_warning_limit'):
            att = getattr(self, nam)
            if att is None:
                continue

            if nam == 'timestamp':
                att = "%.3f (%s)" % (att, format_time(att))
            elif nam == 'char_value':
                att = "'%s'" % att
            if len(nam) < 12:
                out.append('   %.11s= %s' % (nam + ' ' * 12, str(att)))
            else:
                out.append('   %.20s= %s' % (nam + ' ' * 20, str(att)))

        if xtype == 'enum':  # list enum strings
            out.append('   enum strings: ')
            for index, nam in enumerate(self.enum_strs):
                out.append("       %i = %s " % (index, nam))

        if self._mon_cbid is not None:
            msg = 'PV is internally monitored'
            out.append('   %s, with %i user-defined callbacks:'
                       '' % (msg, len(self.callbacks)))
            if len(self.callbacks) > 0:
                for nam in sorted(self.callbacks.keys()):
                    cback = self.callbacks[nam][0]
                    out.append('      %s in file %s'
                               '' % (cback.func_name,
                                     cback.func_code.co_filename))
        else:
            out.append('   PV is NOT internally monitored')
        out.append('=============================')
        return '\n'.join(out)

    @property
    def nelm(self):
        """native count (number of elements).

        For array data this will return the full array size (ie, the
        .NELM field).  See also 'count' property
        """
        return self.channel.native_element_count

    def __repr__(self):
        "string representation"

        if self.connected:
            if self.count == 1:
                return self._fmtsca % self._args
            else:
                return self._fmtarr % self._args
        else:
            return "PV<%r: disconnected>" % self.pvname

    def __eq__(self, other):
        "test for equality"
        try:
            return (self.chid == other.chid)
        except AttributeError:
            return False

    def _disconnect(self, deleted):
        self.connected = False

        ctx = self._context
        pvid = self._pvid
        try:
            if pvid in _PVcache_:
                _PVcache_.pop(pvid)
        except TypeError:
            if not deleted:
                raise
            # _pvcache_ can get deleted and set to None when getting teared
            # down

        self.callbacks = {}

        if self._mon_cbid is not None:
            cbid = self._mon_cbid
            self._mon_cbid = None
            try:
                ctx.unsubscribe(cbid)
            except KeyError:
                # on channel destruction, subscriptions may be deleted from
                # underneath us, but not otherwise
                if not deleted:
                    raise

    def disconnect(self):
        "disconnect PV"
        self._disconnect(deleted=False)

    def __del__(self):
        self._disconnect(deleted=True)

    def _get_arg(self, arg):
        if arg not in self._args:
            return None

        return self._args[arg]

    def _arg_property(arg, doc):
        "wrapper for property retrieval"
        def fget(self):
            return self._get_arg(arg)

        return property(fget, doc=doc)

    char_value = _arg_property('char_value',
                               doc='character string representation of value')
    status = _arg_property('status', doc='pv status')
    type = _arg_property('type', doc='pv type')
    typefull = _arg_property('typefull', doc='pv typefull')
    host = _arg_property('host', doc='hostname of the IOC')
    count = _arg_property('count', doc='pv count')
    read_access = _arg_property('read_access', doc='pv read access')
    write_access = _arg_property('write_access', doc='pv write access')
    access = _arg_property('access', doc='pv write access')
    severity = _arg_property('severity', doc='pv severity')
    timestamp = _arg_property('timestamp', doc='timestamp of last pv action')
    precision = _arg_property('precision',
                              doc='number of digits after decimal point')
    units = _arg_property('units', doc='engineering units for pv')
    enum_strs = _arg_property('enum_strs', doc='list of enumeration strings')
    lower_disp_limit = _arg_property('lower_disp_limit',
                                     doc='pv lower display limit')
    upper_disp_limit = _arg_property('upper_disp_limit',
                                     doc='pv upper display limit')
    lower_alarm_limit = _arg_property('lower_alarm_limit',
                                      doc='pv lower alarm limit')
    upper_alarm_limit = _arg_property('upper_alarm_limit',
                                      doc='pv upper alarm limit')
    lower_warning_limit = _arg_property('lower_warning_limit',
                                        doc='pv lower warning limit')
    upper_warning_limit = _arg_property('upper_warning_limit',
                                        doc='pv upper warning limit')
    lower_ctrl_limit = _arg_property('lower_ctrl_limit',
                                     doc='pv lower ctrl limit')
    upper_ctrl_limit = _arg_property('upper_ctrl_limit',
                                     doc='pv upper ctrl limit')
