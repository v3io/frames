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
from contextlib import contextmanager
from shutil import copy
from subprocess import PIPE, run
from sys import executable
from tempfile import mkdtemp

import pytest

from conftest import here, is_travis

config = '''
log:
  level: debug

backends:
  - type: "csv"
    rootDir: "/csv-root"
'''

docker_image = 'quay.io/v3io/frames:unstable'


# docker run \
#    -v /path/to/config.yaml:/etc/framesd.yaml \
#    quay.io/v3io/frames


def docker_ports(cid):
    out = run(['docker', 'inspect', cid], stdout=PIPE, check=True)
    obj = json.loads(out.stdout.decode('utf-8'))[0]
    grpc_port = obj['NetworkSettings']['Ports']['8081/tcp'][0]['HostPort']
    http_port = obj['NetworkSettings']['Ports']['8080/tcp'][0]['HostPort']

    return grpc_port, http_port


@contextmanager
def docker(tmp, cfg_file):
    cmd = [
        'docker', 'run',
        '-d',
        '-v', '{}:/csv-root'.format(tmp),
        '-v', '{}:/etc/framesd.yaml'.format(cfg_file),
        '-p', '8080',
        '-p', '8081',
        docker_image,
    ]
    proc = run(cmd, stdout=PIPE, check=True)
    cid = proc.stdout.decode('utf-8').strip()
    grpc_port, http_port = docker_ports(cid)
    try:
        yield grpc_port, http_port
    finally:
        run(['docker', 'rm', '-f', cid])


@pytest.mark.skipif(not is_travis, reason='integration test')
def test_pip_docker():
    tmp = mkdtemp()
    run(['virtualenv', '-p', executable, tmp], check=True)
    python = '{}/bin/python'.format(tmp)
    # Run in different directoy so local v3io_frames won't be used
    run([python, '-m', 'pip', 'install', 'v3io_frames'], check=True, cwd=tmp)
    run(['docker', 'pull', docker_image], check=True)

    cfg_file = '{}/framesd.yaml'.format(tmp)
    with open(cfg_file, 'w') as out:
        out.write(config)

    copy('{}/weather.csv'.format(here), tmp)
    with docker(tmp, cfg_file) as (grpc_port, http_port):
        cmd = [
            python, '{}/pip_docker.py'.format(here),
            '--grpc-port', grpc_port,
            '--http-port', http_port,
        ]
        # Run in different directoy so local v3io_frames won't be used
        run(cmd, check=True, cwd=tmp)
