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

import grpc
import pandas as pd
import numpy as np

from . import frames_pb2 as fpb  # noqa
from . import frames_pb2_grpc as fgrpc  # noqa
from .errors import MessageError


class gRPCClient:
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
                message_limit=max_in_message,
                limit=limit,
            )
            for frame in stub.Read(request):
                yield self._frame2df(frame)

    def _frame2df(self, frame):
        cols = {col.name: self._col2series(col) for col in frame.columns}
        df = pd.DataFrame(cols)

        indices = [self._col2series(idx) for idx in frame.indices]
        if len(indices) == 1:
            df.index = indices[0]
        elif len(indices) > 1:
            df.index = pd.MultiIndex.from_arrays(indices)

        return df

    def _col2series(self, col):
        if col.dtype == fpb.BOOLEAN:
            data = pd.Series(col.bools, dtype=np.bool)
        elif col.dtype == fpb.FLOAT:
            data = pd.Series(col.floats, dtype=np.float64)
        elif col.dtype == fpb.INTEGER:
            data = pd.Series(col.ints, dtype=np.int64)
        elif col.dtype == fpb.STRING:
            data = pd.Series(col.strings)
        elif col.dtype == fpb.TIME:
            data = pd.to_datetime(pd.Series(col.times, unit='ns'))
        else:
            raise MessageError('unknown dtype - {}'.format(col.dtype))

        if col.kind == col.LABEL:
            data = data.reindex(pd.RangeIndex(col.size), method='pad')

        return data
