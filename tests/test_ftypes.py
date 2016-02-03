import epics
import pytest

from epics.dbr import (ChannelType as ChType,
                       native_types,
                       promote_type,
                       )


@pytest.mark.parametrize('ntype', native_types)
def test_promote_time(ntype):
    ptype = ChType(promote_type(ntype, use_time=True))
    assert ptype.name == 'TIME_' + ntype.name


@pytest.mark.parametrize('ntype', native_types)
def test_promote_ctrl(ntype):
    ptype = ChType(promote_type(ntype, use_ctrl=True))
    if ntype == ChType.STRING:
        assert ptype.name == 'TIME_' + ntype.name
    else:
        assert ptype.name == 'CTRL_' + ntype.name


@pytest.mark.parametrize('ntype', native_types)
def test_promote_ctrl_time(ntype):
    ptype = ChType(promote_type(ntype, use_ctrl=True, use_time=True))
    if ntype == ChType.STRING:
        assert ptype.name == 'TIME_' + ntype.name
    else:
        assert ptype.name == 'CTRL_' + ntype.name


@pytest.mark.parametrize('ntype', native_types)
def test_promote_ctrl_time(ntype):
    ptype = ChType(promote_type(ntype, use_ctrl=True, use_time=True))
    if ntype == ChType.STRING:
        assert ptype.name == 'TIME_' + ntype.name
    else:
        assert ptype.name == 'CTRL_' + ntype.name
