# Copyright 2018 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from operator import attrgetter

import pandas as pd
import numpy as np


class DType:
    write_slice_key = None


class IntDType(DType):
    dtype = '[]int'
    slice_key = 'ints'
    pd_dtype = np.int64
    py_types = (np.integer, int)
    to_py = int
    label_key = 'ival'

    @staticmethod
    def to_pylist(col):
        return col.tolist()


class FloatDType(DType):
    dtype = '[]float64'
    slice_key = 'floats'
    pd_dtype = np.float64
    py_types = (np.inexact, float)
    to_py = float
    label_key = 'fval'

    @staticmethod
    def to_pylist(col):
        return col.fillna(0.0).tolist()


def identity(val):
    return val


class StringDType(DType):
    dtype = '[]string'
    slice_key = 'strings'
    pd_dtype = str
    py_types = str
    to_py = identity
    label_key = 'sval'

    @staticmethod
    def to_pylist(col):
        return col.fillna('').tolist()


class TimeDType(DType):
    dtype = '[]time.Time'
    slice_key = 'times'
    pd_dtype = 'timedelta64[ns]'
    py_types = pd.Timestamp
    to_py = attrgetter('value')
    write_slice_key = 'ns_times'
    label_key = 'tval'

    @staticmethod
    def to_pylist(col):
        return col.fillna(pd.Timestamp.min).values.tolist()


class BoolDType(DType):
    dtype = '[]bool'
    slice_key = 'bools'
    pd_dtype = bool
    py_types = (bool, np.bool_)
    to_py = bool  # TODO: Is this what we want?
    label_key = 'bval'

    @staticmethod
    def to_pylist(col):
        return col.fillna(False).tolist()


dtypes = {dtype.dtype: dtype for dtype in DType.__subclasses__()}
