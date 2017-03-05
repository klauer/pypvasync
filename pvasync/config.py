import sys

# maximum element count for auto-monitoring of PVs in epics.pv and for
# automatic conversion of numerical array data to numpy arrays
AUTOMONITOR_MAXLENGTH = 65536  # 16384

# default timeout for connection
#   This should be kept fairly short --
#   as connection will be tried repeatedly
DEFAULT_CONNECTION_TIMEOUT = 2.0
