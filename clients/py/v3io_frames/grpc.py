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

from .dtypes import dtypes

from . import frames_pb2 as fpb  # noqa
from . import frames_pb2_grpc as fgrpc  # noqa


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
                message_limit=max_in_message,
                limit=limit,
            )
            for frame in stub.Read(request):
                yield self._frame2df(frame)

    def _frame2df(self, frame):
        cols = {}
        for col in frame.columns:
            if col.HasField('slice'):
                scol = col.slice
                dtype = dtypes.get(scol.dtype)
                if not dtype:
                    raise MessageError('unknown dtype: {}'.format(scol.dtype))
                cols[scol.name] = getattr(scol, dtype.slice_key)
            elif col.HasField('label'):
                lcol = col.label
                dtype = dtypes[lcol.dtype]
                if not dtype:
                    raise MessageError('unknown dtype: {}'.format(lcol.dtype))
                value = getattr(lcol, dtype.label_key)
                # TODO: np dtype
                cols[lcol.name] = np.full(lcol.size, value)
        # TODO: indices
        return pd.DataFrame(cols)
