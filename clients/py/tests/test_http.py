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

import re
import struct
from datetime import datetime
from io import BytesIO
from os.path import abspath, dirname

import pandas as pd
import pytz

import v3io_frames as v3f
from v3io_frames import frames_pb2 as fpb
from v3io_frames import http
from v3io_frames.pbutils import df2msg

here = dirname(abspath(__file__))


class RequestSessionMock(object):

    def __init__(self, data=None):
        self.requests = []
        self.data = [] if data is None else data
        self.write_request = None
        self.write_frames = []

    def post(self, *args, **kw):
        self.requests.append((args, kw))
        if args[0].endswith('/read'):
            return self._read(*args, **kw)
        elif args[0].endswith('/write'):
            return self._write(*args, **kw)

    def _read(self, *args, **kw):
        io = BytesIO()
        for df in self.data:
            data = df2msg(df, None).SerializeToString()
            io.write(struct.pack(http.header_fmt, len(data)))
            io.write(data)

        io.seek(0, 0)

        class Response:
            raw = io
            ok = True

        return Response

    def _write(self, *args, **kw):
        it = iter(kw.get('data', []))
        data = next(it)
        self.write_request = fpb.InitialWriteRequest.FromString(data)
        for chunk in it:
            msg = fpb.Frame.FromString(chunk)
            self.write_frames.append(msg)

        class Response:
            ok = True

            @staticmethod
            def json():
                # TODO: Real values
                return {
                    'num_frames': -1,
                    'num_rows': -1,
                }

        return Response


def test_read():
    address = 'https://nuclio.io'
    query = 'SELECT 1'
    data = [
        pd.DataFrame({
            'x': [1, 2, 3],
            'y': [4., 5., 6.],
        }),
        pd.DataFrame({
            'x': [10, 20, 30],
            'y': [40., 50., 60.],
        }),
    ]

    client = new_test_client(address=address)

    # mock the session
    client._session = RequestSessionMock(data)
    dfs = client.read(
        backend='backend', table='table', query=query, iterator=True)

    assert len(client._session.requests) == 1

    args, kw = client._session.requests[0]
    assert args == (address + '/read',)

    df = pd.concat(dfs)
    assert len(df) == 6
    assert list(df.columns) == ['x', 'y']

    df = client.read(backend='backend', query=query, iterator=False)
    assert isinstance(df, pd.DataFrame), 'iterator=False return'


def col_name(msg):
    val = msg.get('slice') or msg.get('label')
    return val['name']


def test_format_go_time():
    tz = pytz.timezone('Asia/Jerusalem')
    now = datetime.now()
    dt = now.astimezone(tz)
    ts = v3f.http.format_go_time(dt)

    # 2018-10-04T16:54:05.434079562+03:00
    match = re.match(
        r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+\+\d{2}:\d{2}$', ts)
    assert match, 'bad timestamp format'

    # ...+03:00 -> (3, 0)
    hours, minutes = map(int, ts[ts.find('+')+1:].split(':'))
    offset = hours * 60 * 60 + minutes * 60
    assert offset == tz.utcoffset(now).total_seconds(), 'bad offset'


def new_test_client(address='', session=None):
    return v3f.HTTPClient(
        address=address or 'http://example.com',
        session=session,
    )
