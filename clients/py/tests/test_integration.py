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
from uuid import uuid4

import numpy as np
import pandas as pd
import pytest

import v3io_frames as v3f
from conftest import has_go, test_backends


tsdb_span = 5  # hours


def kv_df(df):
    df = df[['fcol']]
    df.index = ['idx-{}'.format(i) for i in df.index]
    return df


def stream_df(df):
    df = df[['fcol', 'icol']]
    df.index = pd.date_range('2016', '2018', periods=len(df))
    return df


def tsdb_df(df):
    df = df[['fcol', 'icol']]
    end = pd.Timestamp.now()
    start = end - pd.Timedelta(hours=tsdb_span)
    df.index = pd.date_range(start, end, periods=len(df))
    return df


# Backend specific configuration. None means don't run this step
test_config = {
    'kv': {
        'df_fn': kv_df,
        'create': None,
        'execute': None,
    },
    'stream': {
        'df_fn': stream_df,
        'create': {
            'attrs': {
                'retention_hours': 48,
                'shards': 1,
            },
        },
        'read': {
            'seek': 'earliest',
            'shard': '0',
        },
        'execute': None,
    },
    'tsdb': {
        'df_fn': tsdb_df,
        'create': {
            'attrs': {
                'rate': '1/m',
            },
        },
        'read': {
            'step': '10m',
            'aggragators': 'avg,max,count',
            'start': 'now-{}h'.format(tsdb_span),
            'end': 'now',
        },
    },
}

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
    data = {
        'icol': np.random.randint(-17, 99, size),
        'fcol': np.random.rand(size),
        'scol': ['val-{}'.format(i) for i in range(size)],
        'bcol': np.random.choice([True, False], size=size),
        'tcol': pd.date_range('2018-01-01', '2018-10-10', periods=size),
    }

    return pd.DataFrame(data)


integ_params = [(p, b) for p in ['grpc', 'http'] for b in test_backends]


@pytest.mark.skipif(not has_go, reason='Go SDK not found')
@pytest.mark.parametrize('protocol,backend', integ_params)
def test_integration(framesd, session, protocol, backend):
    if backend != 'stream':
        return
    test_id = uuid4().hex
    size = 293
    table = 'integtest{}'.format(test_id)
    df = random_df(size)
    labels = {
        'li': 17,
        'lf': 3.22,
        'ls': 'hi',
    }

    port = getattr(framesd, '{}_port'.format(protocol))
    addr = 'localhost:{}'.format(port)
    c = v3f.Client(addr, protocol=protocol, **session)
    cfg = test_config.get(backend, {})
    df_fn = cfg.get('df_fn')
    if df_fn:
        df = df_fn(df)

    create_kw = cfg.get('create', {})
    if create_kw is not None:
        c.create(backend, table, **create_kw)

    write_kw = cfg.get('write', {})
    c.write(backend, table, [df], **write_kw, labels=labels)

    sleep(1)  # Let db flush

    read_kw = cfg.get('read', {})
    dfs = [df for df in c.read(backend, table=table, **read_kw)]
    df2 = pd.concat(dfs)

    if backend != 'tsdb':
        compare_dfs(df, df2, backend)
    else:
        compare_dfs_tsdb(df, df2, backend)

    c.delete(backend, table)
    exec_kw = cfg.get('execute', {})
    if exec_kw is not None:
        c.execute(backend, table, **exec_kw)


def compare_dfs(df1, df2, backend):
    assert set(df2.columns) == set(df1.columns), \
        '{}: columns mismatch'.format(backend)
    for name in df1.columns:
        if name == 'tcol':
            # FIXME: Time zones
            continue
        col1 = df1[name].sort_index()
        col2 = df2[name].sort_index()
        if col1.dtype == np.float:
            ok = np.allclose(col1.values, col2.values)
        else:
            ok = col1.equals(col2)
        assert ok, '{}: column {} mismatch'.format(backend, name)


def compare_dfs_tsdb(df1, df2, backend):
    # TODO
    pass


@pytest.mark.skipif(not has_go, reason='Go SDK not found')
def test_integration_http_error(framesd):
    addr = 'http://localhost:{}'.format(framesd.http_port)
    c = v3f.HTTPClient(addr, session=None)

    with pytest.raises(v3f.ReadError):
        for df in c.read('no-such-backend', table='no such table'):
            pass
