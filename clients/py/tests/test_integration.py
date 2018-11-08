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

from subprocess import call, Popen, check_output
from os import path, makedirs
from time import sleep, time
from socket import socket, error as SocketError
from shutil import rmtree
from contextlib import contextmanager

import pytest
import pandas as pd
import numpy as np
import re

import v3io_frames as v3f


schema = v3f.Schema(
    'type',
    'namesapce',
    'name',
    'doc',
    [],  # aliases
    [
        v3f.SchemaField('field1', '', '', 't1', None),
        v3f.SchemaField('field2', '', '', 't2', None),
        v3f.SchemaField('field3', '', '', 't3', None),
    ],
    None,  # Key
)


def has_working_go():
    """Check we have go version >= 1.11"""
    try:
        out = check_output(['go', 'version']).decode('utf-8')
        match = re.search(r'(\d+)\.(\d+)', out)
        if not match:
            print('warning: cannot find version in {!r}'.format(out))
            return False

        major, minor = int(match.group(1)), int(match.group(2))
        return (major, minor) >= (1, 11)
    except FileNotFoundError:
        return False


here = path.dirname(path.abspath(__file__))
backend = 'weather'
http_port = 8765
grpc_port = 8766
server_timeout = 30  # seconds
has_go = has_working_go()
root_dir = '/tmp/test-integration-root'


def wait_for_server(port, timeout):
    start = time()
    while time() - start <= timeout:
        with socket() as sock:
            try:
                sock.connect(('localhost', port))
                return True
            except SocketError:
                sleep(0.1)

    return False


@contextmanager
def new_server():
    if path.exists(root_dir):
        rmtree(root_dir)
    makedirs(root_dir)
    print('root dir: {}'.format(root_dir))

    config = '''
    log:
      level: debug

    backends:
      - name: "{name}"
        type: "csv"
        rootDir: "{root_dir}"
    '''.format(name=backend, root_dir=root_dir)

    cfg_file = '{}/config.yaml'.format(root_dir)
    with open(cfg_file, 'wb') as out:
        out.write(config.encode('utf-8'))

    server_exe = '/tmp/test-framesd'
    cmd = [
        'go', 'build',
        '-o', server_exe,
        '{}/../../../cmd/framesd/framesd.go'.format(here),
    ]
    assert call(cmd) == 0, 'cannot build server'

    cmd = [
        server_exe,
        '-httpAddr', ':{}'.format(http_port),
        '-grpcAddr', ':{}'.format(grpc_port),
        '-config', cfg_file,
    ]
    pipe = Popen(cmd)
    assert wait_for_server(http_port, server_timeout), 'server did not start'

    try:
        yield pipe
    finally:
        pipe.kill()


def random_df(size):
    times = pd.date_range('2018-01-01', '2018-10-10', periods=size)
    data = {
        'icol': np.random.randint(-17, 99, size),
        'fcol': np.random.rand(size),
        'scol': ['val-{}'.format(i) for i in range(size)],
        'bcol': np.random.choice([True, False], size=size),
        # FIXME
        'tcol': times,
    }

    return pd.DataFrame(data)


integration_cases = [
    (v3f.HTTPClient, f'http://localhost:{http_port}'),
    (v3f.gRPCClient, f'localhost:{grpc_port}'),
]


@pytest.mark.skipif(not has_go, reason='Go SDK not found')
@pytest.mark.parametrize('client_cls,addr', integration_cases)
def test_integration(client_cls, addr):
    with new_server():
        size = 1932
        table = 'random.csv'
        df = random_df(size)
        lables = {
            'li': 17,
            'lf': 3.22,
            'ls': 'hi',
        }

        c = client_cls(addr)
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


@pytest.mark.skipif(not has_go, reason='Go SDK not found')
def test_integration_http_error():
    with new_server():
        c = v3f.HTTPClient('http://localhost:{}'.format(http_port))

        with pytest.raises(v3f.ReadError):
            for df in c.read('no-such-backend', table='no such table'):
                pass
