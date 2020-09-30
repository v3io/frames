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
from datetime import datetime
import pytz
from conftest import has_go, test_backends, protocols, has_session

tsdb_span = 5  # hours
integ_params = [(p, b) for p in protocols for b in test_backends]


def csv_df(size):
    data = {
        'icol': np.random.randint(-17, 99, size),
        'fcol': np.random.rand(size),
        'scol': ['val-{}'.format(i) for i in range(size)],
        'bcol': np.random.choice([True, False], size=size),
        'tcol': pd.date_range('2018-01-01', '2018-10-10', periods=size),
    }

    return pd.DataFrame(data)


def kv_df(size):
    index = ['mike', 'joe', 'jim', 'rose', 'emily', 'dan']
    columns = ['n1', 'n2', 'n3']
    data = np.random.randn(len(index), len(columns))
    return pd.DataFrame(data, index=index, columns=columns)


def stream_df(size):
    end = pd.Timestamp.now().replace(minute=0, second=0, microsecond=0)
    index = pd.date_range(end=end, periods=60, freq='300s', tz='Israel')
    columns = ['cpu', 'mem', 'disk']
    data = np.random.randn(len(index), len(columns))
    return pd.DataFrame(data, index=index, columns=columns)


def tsdb_df(size):
    return stream_df(size)


# Backend specific configuration. None means don't run this step
test_config = {
    'csv': {
        'df_fn': csv_df,
        'execute': {
            'command': 'ping',
        },
    },
    'kv': {
        'df_fn': kv_df,
        'create': None,
        'execute': None,
    },
    'stream': {
        'df_fn': stream_df,
        'create': {
            'retention_hours': 48,
            'shards': 1,
        },
        'read': {
            'seek': 'earliest',
            'shard_id': '0',
        },
        'execute': None,
    },
    'tsdb': {
        'df_fn': tsdb_df,
        'create': {
            'rate': '1/m',
        },
        'read': {
            'step': '10m',
            'aggregators': 'avg,max,count',
            'start': 'now-{}h'.format(tsdb_span),
            'end': 'now',
        },
        'execute': None,
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


@pytest.mark.skipif(not has_go, reason='Go SDK not found')
@pytest.mark.parametrize('protocol,backend', integ_params)
def test_integration(framesd, session, protocol, backend):
    test_id = uuid4().hex
    size = 293
    table = 'integtest{}'.format(test_id)

    addr = getattr(framesd, '{}_addr'.format(protocol))
    client = v3f.Client(addr, **session)
    cfg = test_config.get(backend, {})
    df = cfg['df_fn'](size)

    create_kw = cfg.get('create', {})
    if create_kw is not None:
        client.create(backend, table, **create_kw)

    write_kw = cfg.get('write', {})

    labels = {}
    if backend == 'tsdb':
        labels = {
            'li': 17,
            'lf': 3.22,
            'ls': 'hi',
        }

    client.write(backend, table, [df], **write_kw, labels=labels)
    sleep(1)  # Let db flush

    read_kw = cfg.get('read', {})
    dfs = list(client.read(backend, table=table, iterator=True, **read_kw))
    df2 = pd.concat(dfs)

    if backend == 'tsdb':
        compare_dfs_tsdb(df, df2, backend)
    elif backend == 'stream':
        compare_dfs_stream(df, df2, backend)
    else:
        if backend == 'kv':
            # FIXME: Probably the schema
            df2.dropna(inplace=True)
        compare_dfs(df, df2, backend)

    df = client.read(backend, table=table, **read_kw)
    assert isinstance(df, pd.DataFrame), 'iterator=False returned generator'

    client.delete(backend, table)
    exec_kw = cfg.get('execute', {})
    if exec_kw is not None:
        client.execute(backend, table, **exec_kw)


def compare_dfs(df1, df2, backend):
    assert set(df2.columns) == set(df1.columns), \
        '{}: columns mismatch'.format(backend)
    for name in df1.columns:
        if name == 'tcol':
            # FIXME: Time zones
            continue
        col1 = df1[name].sort_index()
        col2 = df2[name].sort_index()
        assert len(col1) == len(col2), \
            '{}: column {} size mismatch'.format(backend, name)
        if col1.dtype == np.float:
            ok = np.allclose(col1.values, col2.values)
        else:
            ok = col1.equals(col2)
        assert ok, '{}: column {} mismatch'.format(backend, name)


def compare_dfs_stream(df1, df2, backend):
    assert set(df1.columns) < set(df2.columns), 'bad columns'


def compare_dfs_tsdb(df1, df2, backend):
    # TODO
    pass


@pytest.mark.skipif(not has_go, reason='Go SDK not found')
def test_integration_http_error(framesd):
    c = v3f.HTTPClient(framesd.http_addr, session=None)

    with pytest.raises(v3f.ReadError):
        for df in c.read('no-such-backend', table='no such table'):
            pass


@pytest.mark.skipif(not has_session, reason='No session found')
@pytest.mark.skipif(not has_go, reason='Go SDK not found')
@pytest.mark.parametrize('protocol', protocols)
def test_kv_read_empty_df(framesd, session, protocol):
    backend = 'kv'
    test_id = uuid4().hex
    tableName = 'integtest{}'.format(test_id)

    addr = getattr(framesd, '{}_addr'.format(protocol))
    client = v3f.Client(addr, **session)

    index = [str(i) for i in range(1, 4)]
    df = pd.DataFrame(data={'col1': [i for i in range(1, 4)], 'col2': ['aaa', 'bad', 'cffd']}, index=index)
    client.write(backend, table=tableName, dfs=df, condition="starts({col2}, 'aaa') AND {col1} == 3")

    df = client.read(backend, table=tableName)
    assert df.to_json() == '{}'
    assert isinstance(df, pd.DataFrame), 'iterator=False returned generator'

    client.delete(backend, tableName)


@pytest.mark.skipif(not has_session, reason='No session found')
@pytest.mark.skipif(not has_go, reason='Go SDK not found')
@pytest.mark.parametrize('protocol', protocols)
def test_datetime(framesd, session, protocol):
    backend = 'kv'
    test_id = uuid4().hex
    tableName = 'integtest{}'.format(test_id)

    addr = getattr(framesd, '{}_addr'.format(protocol))
    client = v3f.Client(addr, **session)

    col = pd.DataFrame(data=pd.Series([datetime.now(pytz.timezone("Africa/Abidjan")), datetime.now(pytz.timezone("America/Nassau")), None, datetime.now()]))
    df = pd.DataFrame({'col': col})
    client.write(backend, table=tableName, dfs=df)

    df = client.read(backend, table=tableName)

    client.delete(backend, tableName)


@pytest.mark.skipif(not has_session, reason='No session found')
@pytest.mark.skipif(not has_go, reason='Go SDK not found')
@pytest.mark.parametrize('protocol', protocols)
def test_timestamp(framesd, session, protocol):
    backend = 'kv'
    test_id = uuid4().hex
    tableName = 'integtest{}'.format(test_id)

    addr = getattr(framesd, '{}_addr'.format(protocol))
    client = v3f.Client(addr, **session)

    df = pd.DataFrame({'birthday': [pd.Timestamp('1940-04-25', tz='Asia/Dubai'), pd.Timestamp('1940-04-25', tz='US/Pacific'), None, pd.Timestamp('1940-04-25')]})
    client.write(backend, table=tableName, dfs=df)

    df = client.read(backend, table=tableName)

    client.delete(backend, tableName)
