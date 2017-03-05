import asyncio

_pending_futures = {}
loop = asyncio.get_event_loop()


async def caput(pvname, value, *, timeout=60):
    """Put to a pv's value.

    >>> def coroutine():
    ...     yield from caput('xx.VAL', 3.0)
    ...     print('put done')
    """
    from .pv import get_pv
    thispv = await get_pv(pvname, connect=True)
    if not thispv.connected:
        raise asyncio.TimeoutError()

    ret = await thispv.aput(value, timeout=timeout)
    return ret


async def caget(pvname, *, as_string=False, count=None, as_numpy=True,
                use_monitor=False, timeout=None):
    """caget(pvname, as_string=False)
    simple get of a pv's value..
       >>> x = caget('xx.VAL')

    to get the character string representation (formatted double,
    enum string, etc):
       >>> x = caget('xx.VAL', as_string=True)

    to get a truncated amount of data from an array, you can specify
    the count with
       >>> x = caget('MyArray.VAL', count=1000)
    """
    from .pv import get_pv
    thispv = await get_pv(pvname, connect=True)
    if not thispv.connected:
        raise asyncio.TimeoutError()

    if as_string:
        await thispv.get_ctrlvars()
    val = await thispv.aget(count=count, timeout=timeout,
                            use_monitor=use_monitor, as_string=as_string,
                            as_numpy=as_numpy)
    return val


async def cainfo(pvname):
    """cainfo(pvname)

    return printable information about pv
       >>>cainfo('xx.VAL')

    will return a status report for the pv.
    """
    from .pv import get_pv
    thispv = await get_pv(pvname, connect=True)
    if not thispv.connected:
        raise asyncio.TimeoutError()

    await thispv.aget()
    await thispv.get_ctrlvars()
    return thispv.info


@asyncio.coroutine
def caget_many(pvlist):
    """# TODO unimplemented

    get values for a list of PVs
    This does not maintain PV objects, and works as fast
    as possible to fetch many values.
    """
    raise NotImplementedError()
    chids, out = [], []
    for name in pvlist:
        chids.append(ca.create_channel(name, auto_cb=False))
    for chid in chids:
        ca.connect_channel(chid)
    for chid in chids:
        get(chid, wait=False)
    for chid in chids:
        # out.append(get_complete(chid))
        # removed, necessary?
        pass
    return out
