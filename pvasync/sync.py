import asyncio
import functools
import atexit

from . import (ca, coroutines, context)


def blocking_wrapper(coroutine):
    @functools.wraps(coroutine)
    def wrapped(*args, **kwargs):
        loop = asyncio.get_event_loop()
        future = asyncio.run_coroutine_threadsafe(coroutine(*args, **kwargs),
                                                  loop)
        timeout = kwargs.get('timeout', None)
        return future.result(timeout)

    return wrapped


_loop_thread = None


def _background_loop(loop):
    '''Run the event loop forever (in a background thread)'''
    global _loop_thread

    try:
        asyncio.set_event_loop(loop)
        loop.run_forever()
    finally:
        _loop_thread = None


def blocking_mode(loop=None):
    '''Run the event loop forever (in a background thread)'''
    global _loop_thread

    if _loop_thread is not None:
        return
    if loop is None:
        loop = asyncio.get_event_loop()

    def exit_cleanup(*args, **kwargs):
        ctxh = context.get_contexts()
        ctxh.stop()
        loop.stop()
        _loop_thread.join()

    atexit.register(exit_cleanup)
    _loop_thread = ca.CAThread(target=_background_loop, kwargs=dict(loop=loop),
                               daemon=True)
    try:
        _loop_thread.start()
    except Exception:
        _loop_thread = None
        raise


caget = blocking_wrapper(coroutines.caget)
caput = blocking_wrapper(coroutines.caput)
caget_many = blocking_wrapper(coroutines.caget_many)
