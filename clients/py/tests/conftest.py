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

import re
from collections import namedtuple
from os import environ, makedirs, path
from shutil import rmtree, copyfile
from socket import error as SocketError
from socket import socket
from subprocess import Popen, call, check_output
from time import sleep, time
from uuid import uuid4

import pytest
import v3io_frames as v3f
import yaml

has_session = v3f.SESSION_ENV_KEY in environ
is_travis = 'TRAVIS' in environ
test_id = uuid4().hex
here = path.dirname(path.abspath(__file__))

csv_file = '{}/weather.csv'.format(here)
git_root = path.abspath('{}/../../..'.format(here))
grpc_port = 8766
http_port = 8765
protocols = ['grpc', 'http']
root_dir = '/tmp/test-integration-root-{}'.format(test_id)
server_timeout = 30  # seconds

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


has_go = has_working_go()


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


Framesd = namedtuple('Framesd', 'grpc_addr http_addr config')


def initialize_csv_root():
    if path.exists(root_dir):
        rmtree(root_dir)
    makedirs(root_dir)

    dest = '{}/{}'.format(root_dir, path.basename(csv_file))
    copyfile(csv_file, dest)


@pytest.fixture(scope='module')
def framesd():
    initialize_csv_root()

    config = {
        'log': {
            'level': 'debug',
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
        '{}/cmd/framesd/framesd.go'.format(git_root),
    ]
    assert call(cmd) == 0, 'cannot build server'

    log_file = open('/tmp/framesd-integration.log', 'at')
    cmd = [
        server_exe,
        '-httpAddr', ':{}'.format(http_port),
        '-grpcAddr', ':{}'.format(grpc_port),
        '-config', cfg_file,
    ]
    pipe = Popen(cmd, stdout=log_file, stderr=log_file)
    assert wait_for_server(http_port, server_timeout), 'server did not start'
    try:
        yield Framesd(
            grpc_addr='grpc://localhost:{}'.format(grpc_port),
            http_addr='http://localhost:{}'.format(http_port),
            config=config,
        )
    finally:
        pipe.kill()
        log_file.close()


@pytest.fixture(scope='module')
def session():
    """Return session parameters fit for v3f.Client arguments"""
    obj = v3f.session_from_env()
    session = {desc.name: value for desc, value in obj.ListFields()}
    if 'url' in session:
        session['data_url'] = session.pop('url')
    return session
