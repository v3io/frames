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

import sys
from os import path
from operator import attrgetter

import grpc
import pandas as pd

# See https://github.com/protocolbuffers/protobuf/issues/1491
here = path.dirname(path.abspath(__file__))
sys.path.append(here)

from . import frames_pb2 as fpb  # noqa
from . import frames_pb2_grpc as fgrpc  # noqa

_int_dtype = '[]int'
_float_dtype = '[]float64'
_string_dtype = '[]string'
_time_dtype = '[]time.Time'
_bool_dtype = '[]bool'

_slice_get = {
    _int_dtype: getattr('ints'),
    _float_dtype: getattr('floats'),
    _string_dtype: getattr('strings'),
    _time_dtype: getattr('times'),
    _bool_dtype: getattr('bools'),
}


class Error(Exception):
    pass


class MessageError(Error):
    pass


class Client:
    def __init__(self, address=''):
        self.address = address

    def read(self, backend='', query='', table='', columns=None, filter='',
             group_by='', limit=0, data_format='', row_layout=False,
             max_in_message=0, marker='', **kw):
        # TODO: Create channel once?
        with grpc.insecure_channel(self.address) as channel:
            stub = fgrpc.FramesStub(channel)
            request = fpb.ReadRequest(
                backend=backend,
                query=query,
                table=table,
            )
            for frame in stub.Read(request):
                yield frame


    def _frame2df(self, frame):
        cols = {}
        for col in frame.columns:
            if c.HasField('slice'):
                scol = c.slice
                get = _slice_get(scol.dtype)
                if not get:
                    raise MessageError('unknown dtype: {}'.format(scol.dtype))
                cols[scol.name] = get(scol)

