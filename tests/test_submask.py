import pytest

from epics import dbr
from epics.callback_registry import MonitorCallback
from epics.dbr import native_types


def check_order(lesser, greater):
    assert lesser < greater
    assert lesser <= greater
    assert greater > lesser
    assert greater >= lesser


@pytest.mark.parametrize('ntype', native_types)
def test_promoted_type(ntype):
    ncb = MonitorCallback(chid=0, ftype=ntype)

    ptype = dbr.promote_type(ntype, use_ctrl=True)
    pcb = MonitorCallback(chid=0, ftype=ptype)
    check_order(lesser=ncb, greater=pcb)

    ptype = dbr.promote_type(ntype, use_time=True)
    pcb = MonitorCallback(chid=0, ftype=ptype)
    check_order(lesser=ncb, greater=pcb)


ST = dbr.SubscriptionType
val, log, alarm, prop = ST.DBE_VALUE, ST.DBE_LOG, ST.DBE_ALARM, ST.DBE_PROPERTY


@pytest.mark.parametrize('ntype', native_types)
@pytest.mark.parametrize('mask1,mask2', [(val, val),
                                         (val, val | log),
                                         (val | alarm, val | alarm | prop),
                                         (val | prop, val | alarm | prop),
                                         ])
def test_mask(ntype, mask1, mask2):
    ptype = dbr.promote_type(ntype, use_ctrl=True)

    native_m1 = MonitorCallback(chid=0, ftype=ntype, mask=mask1)
    promoted_m1 = MonitorCallback(chid=0, ftype=ptype, mask=mask1)

    native_m2 = MonitorCallback(chid=0, ftype=ntype, mask=mask2)
    promoted_m2 = MonitorCallback(chid=0, ftype=ptype, mask=mask2)

    if mask1 != mask2:
        check_order(lesser=native_m1, greater=native_m2)
        check_order(lesser=promoted_m1, greater=promoted_m2)
        check_order(lesser=native_m1, greater=promoted_m1)
        check_order(lesser=native_m1, greater=promoted_m2)
    else:
        assert native_m1 <= native_m2
        assert native_m2 >= native_m1
        assert native_m2 == native_m1

        assert promoted_m1 <= promoted_m2
        assert promoted_m2 >= promoted_m1
        assert promoted_m2 == promoted_m1

    assert list(sorted([native_m2, native_m1])) == [native_m1, native_m2]
    assert list(sorted([promoted_m2, promoted_m1])) == [promoted_m1,
                                                        promoted_m2]
    # assert (list(sorted([native_m1, promoted_m2, native_m2, promoted_m1])) ==
    #         [native_m1, native_m2, promoted_m1, promoted_m2])
    # TODO ordering here really isn't well defined, probably should remove
    #      __lt__ on MonitorCallback
