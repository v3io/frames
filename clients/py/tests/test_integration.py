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

from subprocess import call, Popen, PIPE
from os import path, makedirs
from time import sleep, time
from socket import socket, error as SocketError
from shutil import rmtree

import pytest
import pandas as pd
import numpy as np

import v3io_frames as v3f

here = path.dirname(path.abspath(__file__))
backend = 'weather'
server_port = 8765
server_timeout = 30  # seconds
has_go = call(['go', 'version'], stderr=PIPE, stdout=PIPE) == 0
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


@pytest.fixture(scope='function')
def server():
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

    server_log = open('{}.log'.format(server_exe), 'wb')
    cmd = [
        server_exe,
        '-addr', ':{}'.format(server_port),
        '-config', cfg_file,
    ]
    pipe = Popen(cmd, stdout=server_log, stderr=server_log)
    yield pipe
    pipe.kill()
    server_log.close()


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


@pytest.mark.skipif(not has_go, reason='Go SDK not found')
def test_integration(server):
    assert wait_for_server(server_port, server_timeout), 'server did not start'

    size = 1932
    table = 'random.csv'
    df = random_df(size)
    lables = {
        'li': 17,
        'lf': 3.22,
        'ls': 'hi',
    }
    c = v3f.Client('http://localhost:{}'.format(server_port))
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
