import time
import asyncio
import atexit
from . import ca


class CAContextHandler:
    def __init__(self, ctx):
        self._running = True
        self._ctx = ctx
        self._loop = asyncio.get_event_loop()
        self._task = None

    def _poll_thread(self):
        '''Poll context ctx in an executor thread'''
        ca.attach_context(self._ctx)
        try:
            while self._running:
                ca.poll()
                time.sleep(0.01)
        finally:
            print('context detach')
            ca.detach_context()
            print('done')

    def start(self):
        self._task = self._loop.run_in_executor(None, self._poll_thread)

    def stop(self):
        self._running = False
        if self._task:
            self._loop.run_until_complete(self._task)
            self._task = None

    def __del__(self):
        self.stop()


class CAContexts:
    def __init__(self):
        if hasattr(CAContexts, 'instance'):
            raise RuntimeError('CAContexts is a singleton')

        CAContexts.instance = self

        self.contexts = {}
        self.add_context()
        atexit.register(self.stop)

    def add_context(self, ctx=None):
        if ctx is None:
            ctx = ca.current_context()

        ctx_id = int(ctx)
        if ctx_id in self.contexts:
            raise ValueError('Context handler already exists')

        handler = CAContextHandler(ctx=ctx)
        self.contexts[ctx_id] = handler
        handler.start()

    def stop(self):
        print('stopping')
        for ctx_id, context in list(self.contexts.items()):
            context.stop()
            del self.contexts[ctx_id]

        ca.clear_cache()
        ca.detach_context()


def get_contexts():
    '''The global context handler'''
    global _cm
    return _cm

_cm = CAContexts()
