import asyncio
import sys
import logging
import functools

from collections import OrderedDict

from . import ca
from . import dbr


logger = logging.getLogger(__name__)
loop = asyncio.get_event_loop()


def _locked(func):
    '''Lock functions with the context's subscription lock'''
    @functools.wraps(func)
    def inner(self, *args, **kwargs):
        with self._sub_lock:
            return func(self, *args, **kwargs)

    return inner


class ChannelCallbackBase:
    sig = None

    def __init__(self, registry, chid):
        self.registry = registry
        self.chid = chid
        self.handler_id = None

        self.context = registry.context
        self.pvname = self.context.channel_to_pv[self.chid]
        self._sub_lock = registry._sub_lock

        self.callbacks = OrderedDict()
        self.oneshots = []

    @property
    def channel(self):
        # TODO: reason for indirection now? don't think so
        return self.context._vc.channels[self.chid]

    def create(self):
        pass

    def destroy(self):
        for cbid in list(self.callbacks.keys()):
            self.remove_callback(cbid, destroy_if_empty=False)

    @_locked
    def add_callback(self, cbid, func, *, oneshot=False):
        self.callbacks[cbid] = func
        if oneshot:
            self.oneshots.append(cbid)
        return cbid

    def clear_callbacks(self):
        for cbid in list(self.callbacks.keys()):
            self.remove_callback(cbid)

    @_locked
    def remove_callback(self, cbid, *, destroy_if_empty=True):
        del self.callbacks[cbid]

        try:
            self.oneshots.remove(cbid)
        except ValueError:
            pass

        if len(self.callbacks) == 0 and destroy_if_empty:
            self.destroy()

    @_locked
    def process(self, **kwargs):
        with self.context._sub_lock:
            exceptions = []
            for cbid, func in self.callbacks.items():
                try:
                    func(chid=self.chid, **kwargs)
                except Exception as ex:
                    exceptions.append((ex, sys.exc_info()[2]))
                    logger.error('Unhandled callback exception (chid: %s kw: '
                                 '%s)', self.chid, kwargs, exc_info=ex)

            for cbid in list(self.oneshots):
                self.remove_callback(cbid)

            return exceptions

    def __repr__(self):
        return '{0}({1.pvname!r})'.format(self.__class__.__name__, self)


class ChannelCallbackRegistry:
    def __init__(self, context, sig_classes):
        self.context = context
        self.sig_classes = sig_classes
        self.handlers_by_chid = {}
        self.handlers = {}
        self.cbid_owner = {}
        self._cbid = 0
        self._handler_id = 0
        self._sub_lock = context._sub_lock

    def __getstate__(self):
        # We cannot currently pickle the callables in the registry, so
        # return an empty dictionary.
        return {}

    def __setstate__(self, state):
        # re-initialise an empty callback registry
        self.__init__()

    @_locked
    def subscribe(self, sig, chid, func, *, oneshot=False, **kwargs):
        if sig not in self.sig_classes:
            raise ValueError("Allowed signals are {0}".format(
                tuple(self.sig_classes.keys())))

        self._cbid += 1
        cbid = self._cbid

        if chid not in self.handlers_by_chid:
            self.handlers_by_chid[chid] = {sig: []
                                           for sig in self.sig_classes.keys()}

        sig_handlers = self.handlers_by_chid[chid][sig]

        handler_class = self.sig_classes[sig]
        new_handler = handler_class(self, chid, **kwargs)

        # fair warning to anyone looking to make this more efficient (you know
        # who you are): there shouldn't be enough entries in the callback list
        # to make it worth optimizing
        for handler in sig_handlers:
            if handler >= new_handler:
                new_handler = handler
                break

        self.cbid_owner[cbid] = new_handler
        new_handler.add_callback(cbid, func, oneshot=oneshot)

        if new_handler not in sig_handlers:
            self._handler_id += 1
            new_handler.handler_id = self._handler_id

            self.handlers[new_handler.handler_id] = new_handler
            sig_handlers.append(new_handler)
            new_handler.create()

        return new_handler, cbid

    @_locked
    def unsubscribe(self, cbid):
        """Disconnect the callback registered with callback id *cbid*
        Parameters
        ----------
        cbid : int
            The callback index and return value from ``connect``
        """
        owner = self.cbid_owner.pop(cbid)
        owner.remove_callback(cbid)

        if not owner.callbacks:
            chid, sig = owner.chid, owner.sig
            self.handlers_by_chid[chid][sig].remove(owner)

            # TODO here is where chid can be checked to see if it's in use
            # anywhere and can potentially be cleared
            subs = list(self.subscriptions_by_chid(chid))
            if not subs:
                self.context.clear_channel(chid)

    @_locked
    def process(self, sig, chid, *, cbid=None, handler_id=None, **kwargs):
        try:
            if sig == 'connection':
                handler = self.handlers_by_chid[chid]['connection'][0]
            elif handler_id is not None:
                handler = self.handlers[handler_id]
            elif cbid is not None:
                handler = self.cbid_owner[cbid]
            else:
                logger.debug('Handler information not present?')
                return
        except (KeyError, IndexError) as ex:
            logger.debug('Cannot determine callback event handler',
                         exc_info=ex)
            return

        return handler.process(**kwargs)

    def subscriptions_by_chid(self, chid):
        for sig, handlers in self.handlers_by_chid[chid].items():
            for handler in handlers:
                yield sig, handler
