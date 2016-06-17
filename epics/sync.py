import asyncio
import functools

from . import coroutines


def blocking_wrapper(coroutine):
    @functools.wraps(coroutine)
    def wrapped(*args, **kwargs):
        loop = asyncio.get_event_loop()
        future = asyncio.run_coroutine_threadsafe(coroutine(*args, **kwargs),
                                                  loop)
        timeout = kwargs.get('timeout', None)
        return future.result(timeout)

    return wrapped


caget = blocking_wrapper(coroutines.caget)
caput = blocking_wrapper(coroutines.caput)
caget_many = blocking_wrapper(coroutines.caget_many)
