import epics
import pytest

from epics import (ca, dbr)
from epics.callback_registry import MonitorCallback
from epics.dbr import (ChannelType as ChType,
                       native_types,
                       )


# class OrderingTest(unittest.TestCase):
#     def test_native(self):
#         pass


