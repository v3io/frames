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

from time import sleep, time

import pytest

import v3io_frames as v3f
from conftest import has_go

try:
    import cudf
    has_cudf = True
except ImportError:
    has_cudf = False


@pytest.mark.skipif(not has_cudf, reason='cudf not found')
@pytest.mark.skipif(not has_go, reason='Go SDK not found')
def test_cudf(framesd, session):
    df = cudf.DataFrame({
        'a': [1, 2, 3],
        'b': [1.1, 2.2, 3.3],
    })

    c = v3f.Client(framesd.grpc_addr, frame_factory=cudf.DataFrame)
    backend = 'csv'
    table = 'cudf-{}'.format(int(time()))
    print('table = {}'.format(table))

    c.write(backend, table, [df])
    sleep(1)  # Let db flush
    rdf = c.read(backend, table=table)
    assert isinstance(rdf, cudf.DataFrame), 'not a cudf.DataFrame'
    assert len(rdf) == len(df), 'wrong frame size'
    assert set(rdf.columns) == set(df.columns), 'columns mismatch'
