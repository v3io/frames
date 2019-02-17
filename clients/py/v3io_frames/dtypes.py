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

from datetime import datetime

import numpy as np

from . import frames_pb2 as fpb


class DType:
    @classmethod
    def match(cls, value):
        return isinstance(value, cls.py_types)


class BoolDType(DType):
    dtype = fpb.BOOLEAN
    col_key = 'bools'
    val_key = 'bval'
    py_types = (bool, np.bool_)

    @classmethod
    def match(cls, value):
        # We can't check isinstance since bool is instance of int
        return type(value) in cls.py_types


class FloatDType(DType):
    dtype = fpb.FLOAT
    col_key = 'floats'
    val_key = 'fval'
    py_types = (np.inexact, float)


class IntDType(DType):
    dtype = fpb.INTEGER
    col_key = 'ints'
    val_key = 'ival'
    py_types = (np.integer, int)


class StringDType(DType):
    dtype = fpb.STRING
    col_key = 'strings'
    val_key = 'sval'
    py_types = str


class TimeDType(DType):
    dtype = fpb.TIME
    col_key = 'times'
    val_key = 'tval'
    py_types = datetime


# BoolDType must be first
dtypes = [BoolDType] + \
    [dtype for dtype in DType.__subclasses__() if dtype is not BoolDType]


def dtype_of(val):
    for dtype in dtypes:
        if dtype.match(val):
            return dtype

    raise TypeError('unknown type - {!r}'.format(val))
