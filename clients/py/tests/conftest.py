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

import json
import re
from collections import namedtuple
from os import environ, makedirs, path
from shutil import rmtree
from socket import error as SocketError
from socket import socket
from subprocess import Popen, call, check_output
from time import sleep, time
from uuid import uuid4

import pytest
import yaml


SESSION_ENV_KEY = 'V3IO_SESSION'
has_session = SESSION_ENV_KEY in environ

extra_backends = [
    {'type': 'kv'},
    {'type': 'stream'},
    {'type': 'tsdb', 'workers': 16},
]

test_backends = ['csv']
if has_session:
    test_backends.extend(backend['type'] for backend in extra_backends)


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
test_id = uuid4().hex
root_dir = '/tmp/test-integration-root-{}'.format(test_id)


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


Framesd = namedtuple('Framesd', 'grpc_port http_port config')


@pytest.fixture(scope='module')
def framesd():
    if path.exists(root_dir):
        rmtree(root_dir)
    makedirs(root_dir)

    config = {
        'log': {
            'level': 'error',
        },
        'backends': [
            {
                'type': 'csv',
                'rootDir': root_dir,
            },
        ]
    }

    if has_session:
        # We assume we have backends
        config['backends'].extend(extra_backends)

    cfg_file = '{}/config.yaml'.format(root_dir)
    with open(cfg_file, 'wt') as out:
        yaml.dump(config, out, default_flow_style=False)

    server_exe = '/tmp/test-framesd-{}'.format(test_id)
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
        yield Framesd(
            grpc_port=grpc_port,
            http_port=http_port,
            config=config,
        )
    finally:
        pipe.kill()


@pytest.fixture(scope='module')
def session():
    session_info = environ.get(SESSION_ENV_KEY, '')
    session = json.loads(session_info) if session_info else {}
    if 'host' in session:
        session['data_url'] = session.pop('host')
    return session
