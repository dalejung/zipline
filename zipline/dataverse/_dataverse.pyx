#cython: cdivision=True

cimport numpy as np
from cpython cimport bool
import numpy as np
from pandas.tslib import Timestamp

from numpy cimport *
from cpython.dict cimport PyDict_Clear, PyDict_DelItem, PyDict_SetItem
