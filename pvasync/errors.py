import sys


class ChannelAccessException(Exception):
    """Channel Access Exception: General Errors"""

    def __init__(self, *args):
        super().__init__(*args)
        exctype, value, traceback = sys.exc_info()
        if exctype is not None:
            sys.excepthook(exctype, value, traceback)


class CASeverityException(Exception):
    """Channel Access Severity Check Exception:
    PySEVCHK got unexpected return value"""

    def __init__(self, fcn, msg):
        super().__init__()
        self.fcn = fcn
        self.msg = msg

    def __str__(self):
        return " %s returned '%s'" % (self.fcn, self.msg)
