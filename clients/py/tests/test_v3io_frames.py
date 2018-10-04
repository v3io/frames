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
from io import BytesIO
from os.path import abspath, dirname
import re

import msgpack
import pandas as pd
import pytz

import v3io_frames as v3f

here = dirname(abspath(__file__))


class patch_requests:
    orig_requests = v3f.requests

    def __init__(self, data=None):
        self.requests = []
        self.data = [] if data is None else data

    def __enter__(self):
        v3f.requests = self
        return self

    def __exit__(self, exc_type=None, exc_val=None, tb=None):
        v3f.requests = self.orig_requests

    def post(self, *args, **kw):
        self.requests.append((args, kw))
        io = BytesIO()
        for chunk in self.data:
            msgpack.dump(chunk, io)

        io.seek(0, 0)

        class Response:
            raw = io
            ok = True

        return Response


def test_read():
    api_key = 'test api key'
    url = 'https://nuclio.io'
    query = 'SELECT 1'
    data = [
        {
            'columns': [
                {'slice': {'name': 'x', 'ints': [1, 2, 3]}},
                {'slice': {'name': 'y', 'floats': [4., 5., 6.]}},
            ],
        },
        {
            'columns': [
                {'slice': {'name': 'x', 'ints': [10, 20, 30]}},
                {'slice': {'name': 'y', 'floats': [40., 50., 60.]}},
            ],
        },
    ]

    c = v3f.Client(url, api_key)
    with patch_requests(data) as patch:
        dfs = c.read(query=query)

    assert len(patch.requests) == 1

    args, kw = patch.requests[0]
    assert args == (url + '/read',)

    df = pd.concat(dfs)
    assert len(df) == 6
    assert list(df.columns) == ['x', 'y']


def test_encode_df():
    c = v3f.Client('http://localhost:8080')
    labels = {
        'int': 7,
        'str': 'wassup?',
    }

    df = pd.read_csv('{}/weather.csv'.format(here))
    data = c._encode_df(df, labels)
    msg = msgpack.unpackb(data, raw=False)

    names = [col_name(col) for col in msg['columns']]
    assert set(names) == set(df.columns), 'columns mismatch'
    assert not msg.get('indices'), 'has index'
    assert msg['labels'] == labels, 'lables mismatch'

    # Now with index
    index_name = 'DATE'
    df.index = df.pop(index_name)
    data = c._encode_df(df, None)
    msg = msgpack.unpackb(data, raw=False)

    names = [col_name(col) for col in msg['columns']]
    assert set(names) == set(df.columns), 'columns mismatch'
    idx = msg.get('indices')
    assert idx, 'no index'
    assert col_name(idx[0]) == index_name, 'bad index name'


def col_name(msg):
    val = msg.get('slice') or msg.get('label')
    return val['name']


def test_decode():
    df = pd.DataFrame({
        'x': [1, 2, 3],
        'y': ['a', 'b', 'c'],
    })

    labels = {
        'l1': 1,
        'l2': 'two',
    }

    c = v3f.Client('http://localhost:8080')
    data = c._encode_df(df, labels)
    dfs = list(c._iter_dfs(BytesIO(data)))

    assert len(dfs) == 1, 'wrong number of dfs'
    assert dfs[0].to_dict() == df.to_dict(), 'bad encoding'
    assert getattr(dfs[0], 'labels') == labels, 'bad labels'


def test_format_go_time():
    tz = pytz.timezone('Asia/Jerusalem')
    now = datetime.now()
    dt = now.astimezone(tz)
    ts = v3f.format_go_time(dt)

    # 2018-10-04T16:54:05.434079562+03:00
    match = \
        re.match('\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+\+\d{2}:\d{2}$', ts)
    assert match, 'bad timestamp format'

    # ...+03:00 -> (3, 0)
    hours, minutes = map(int, ts[ts.find('+')+1:].split(':'))
    offset = hours * 60 * 60 + minutes * 60
    assert offset == tz.utcoffset(now).total_seconds(), 'bad offset'
