import sys
import logging
from . import ca


logger = logging.getLogger(__name__)


class CallbackRegistry:
    def __init__(self, ignore_exceptions=False, allowed_sigs=None):
        self.ignore_exceptions = ignore_exceptions
        self.allowed_sigs = allowed_sigs
        self.callbacks = dict()
        self._cbid = 0
        self._cbid_map = {}
        self._oneshots = {}

    def __getstate__(self):
        # We cannot currently pickle the callables in the registry, so
        # return an empty dictionary.
        return {}

    def __setstate__(self, state):
        # re-initialise an empty callback registry
        self.__init__()

    def subscribe(self, sig, chid, func, *, oneshot=False):
        """Register ``func`` to be called when ``sig`` is generated
        Parameters
        ----------
        sig
        func
        Returns
        -------
        cbid : int
            The callback index. To be used with ``disconnect`` to deregister
            ``func`` so that it will no longer be called when ``sig`` is
            generated
        """
        if self.allowed_sigs is not None:
            if sig not in self.allowed_sigs:
                raise ValueError("Allowed signals are {0}".format(
                    self.allowed_sigs))

        self._cbid += 1
        cbid = self._cbid
        chid = ca.channel_id_to_int(chid)

        self.callbacks.setdefault(sig, dict())
        self.callbacks[sig].setdefault(chid, dict())

        self.callbacks[sig][chid][cbid] = func
        self._cbid_map[cbid] = (sig, chid)

        if oneshot:
            self._oneshots[cbid] = True
        return cbid

    def unsubscribe(self, cbid):
        """Disconnect the callback registered with callback id *cbid*
        Parameters
        ----------
        cbid : int
            The callback index and return value from ``connect``
        """
        sig, chid = self._cbid_map[cbid]
        del self._cbid_map[cbid]

        del self.callbacks[sig][chid][cbid]
        if not self.callbacks[sig][chid]:
            del self.callbacks[sig][chid]

        try:
            del self._oneshots[cbid]
        except KeyError:
            pass

    def process(self, sig, chid, **kwargs):
        """Process ``sig``
        All of the functions registered to receive callbacks on ``sig``
        will be called with ``args`` and ``kwargs``
        Parameters
        ----------
        sig
        args
        kwargs
        """
        if self.allowed_sigs is not None:
            if sig not in self.allowed_sigs:
                raise ValueError("Allowed signals are {0}"
                                 "".format(self.allowed_sigs))

        exceptions = []

        print('sig', sig, 'chid', chid)
        if sig not in self.callbacks:
            logger.error('? sig')
            return
        if chid not in self.callbacks[sig]:
            logger.error('? chid')
            return

        callbacks = self.callbacks[sig][chid]
        # TODO more efficient way
        for cbid, func in list(callbacks.items()):
            oneshot = self._oneshots.get(cbid, False)
            try:
                func(chid=chid, **kwargs)
            except Exception as ex:
                if not self.ignore_exceptions:
                    raise

                exceptions.append((ex, sys.exc_info()[2]))
                logger.error('Unhandled callback exception', exc_info=ex)
            finally:
                if oneshot:
                    self.unsubscribe(cbid)

        return exceptions
