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

from time import sleep

import numpy as np
import pandas as pd
import pytest

import v3io_frames as v3f

from conftest import has_go

schema = v3f.Schema(
    type='type',
    namespace='namesapce',
    name='name',
    doc='doc',
    fields=[
        v3f.SchemaField('field1', '', '', 't1', None),
        v3f.SchemaField('field2', '', '', 't2', None),
        v3f.SchemaField('field3', '', '', 't3', None),
    ],
)


def random_df(size):
    times = pd.date_range('2018-01-01', '2018-10-10', periods=size)
    data = {
        'icol': np.random.randint(-17, 99, size),
        'fcol': np.random.rand(size),
        'scol': ['val-{}'.format(i) for i in range(size)],
        'bcol': np.random.choice([True, False], size=size),
        'tcol': times,
    }

    return pd.DataFrame(data)


@pytest.mark.skipif(not has_go, reason='Go SDK not found')
@pytest.mark.parametrize('protocol', ['grpc', 'http'])
def test_integration(framesd, session, protocol):
    size = 1932
    table = 'random.csv'
    df = random_df(size)
    lables = {
        'li': 17,
        'lf': 3.22,
        'ls': 'hi',
    }

    port = getattr(framesd, '{}_port'.format(protocol))
    addr = 'localhost:{}'.format(port)
    c = v3f.Client(addr, protocol=protocol, **session)

    for cfg in framesd.config['backends']:
        backend = cfg['type']
        c.write(backend, table, [df], labels=lables)

        sleep(1)  # Let disk flush

        dfs = [df for df in c.read(backend, table=table)]
        df2 = pd.concat(dfs)

        assert set(df2.columns) == set(df.columns), 'columns mismatch'
        for name in df.columns:
            if name == 'tcol':
                # FIXME: Time zones
                continue
            col = df[name]
            col2 = df2[name]
            assert col2.equals(col), 'column {} mismatch'.format(name)

        new_table = 'test-table'
        c.create(backend, new_table, schema=schema)
        c.delete(backend, new_table)
        c.execute(backend, table, 'ping', {'ival': 1, 'sval': 'two'})


@pytest.mark.skipif(not has_go, reason='Go SDK not found')
def test_integration_http_error(framesd):
    addr = 'http://localhost:{}'.format(framesd.http_port)
    c = v3f.HTTPClient(addr, session=None)

    with pytest.raises(v3f.ReadError):
        for df in c.read('no-such-backend', table='no such table'):
            pass
