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

import pytest
from io import BytesIO
import json

import v3io_frames
import msgpack


class patch_urlopen:
    orig_urlopen = v3io_frames.urlopen

    def __init__(self, data=None):
        self.requests = []
        self.data = [] if data is None else data

    def __enter__(self):
        v3io_frames.urlopen = self.urlopen
        return self

    def __exit__(self, exc_type=None, exc_val=None, tb=None):
        v3io_frames.urlopen = self.orig_urlopen

    def urlopen(self, request):
        self.requests.append(request)
        io = BytesIO()
        for chunk in self.data:
            msgpack.dump(chunk, io)

        io.seek(0, 0)
        return io


def test_orient():
    # Valid orient
    for orient in ('rows', 'columns'):
        v3io_frames.Client('http://example.com', orient=orient)

    # Invalid orient
    with pytest.raises(ValueError):
        v3io_frames.Client('http://example.com', orient='cols')


def test_call():
    api_key = 'test api key'
    url = 'https://nuclio.io'
    orient = 'rows'
    query = 'SELECT 1'
    data = [
        {
            'x': [1, 2, 3],
            'y': [4, 5, 6],
        },
        {
            'x': [10, 20, 30],
            'y': [40, 50, 60],
        },
    ]

    c = v3io_frames.Client(url, api_key, orient)
    with patch_urlopen(data) as patch:
        df = c.query(query)

    assert len(patch.requests) == 1

    req = patch.requests[0]
    assert req.headers['Authorization'] == api_key
    assert req.get_full_url() == url
    assert req.get_method() == 'POST'
    post_req = json.loads(req.data.decode('utf-8'))
    assert post_req == {'orient': orient, 'query': query}

    assert len(df) == 6
    assert list(df.columns) == ['x', 'y']
