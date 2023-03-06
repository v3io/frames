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
#
import pytest

import v3io_frames as v3f
from conftest import has_go

from test_integration import integ_params, csv_df

wdf = csv_df(1982)


def read_benchmark(client):
    for df in client.read('csv', 'weather.csv'):
        assert len(df), 'empty df'


def write_benchmark(client, df):
    client.write('csv', 'write-bench.csv', df)


@pytest.mark.parametrize('protocol,backend', integ_params)
def test_read(benchmark, framesd, protocol, backend):
    if not has_go:
        raise AssertionError("Go SDK not found")

    addr = getattr(framesd, '{}_addr'.format(protocol))
    client = v3f.Client(addr)
    benchmark(read_benchmark, client)


@pytest.mark.parametrize('protocol,backend', integ_params)
def test_write(benchmark, framesd, protocol, backend):
    if not has_go:
        raise AssertionError("Go SDK not found")

    addr = getattr(framesd, '{}_addr'.format(protocol))
    client = v3f.Client(addr)
    benchmark(write_benchmark, client, wdf)
