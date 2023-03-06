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

from os import environ

import v3io_frames as v3f


@contextmanager
def setenv(key, value):
    old = environ.get(key)
    environ[key] = value
    try:
        yield
    finally:
        if old is not None:
            environ[key] = old


def test_client():
    c = v3f.Client('localhost:8081', should_check_version=False)
    assert isinstance(c, v3f.gRPCClient), 'default is not grpc'
    c = v3f.Client('grpc://localhost:8081', should_check_version=False)
    assert isinstance(c, v3f.gRPCClient), 'not gRPC'
    c = v3f.Client('http://localhost:8081', should_check_version=False)
    assert isinstance(c, v3f.HTTPClient), 'not HTTP'


def test_client_env():
    url = 'localhost:8080'
    data = json.dumps({'url': url})
    with setenv(v3f.SESSION_ENV_KEY, data):
        with setenv("V3IO_API", ""):
            c = v3f.Client('localhost:8081', should_check_version=False)

    assert c.session.url == url, 'missing URL from env'


def test_session_from_env():
    obj = {field.name: field.name for field in v3f.Session.DESCRIPTOR.fields}
    data = json.dumps(obj)
    with setenv(v3f.SESSION_ENV_KEY, data):
        with setenv("V3IO_API", ""):
            with setenv("V3IO_ACCESS_KEY", ""):
                s = v3f.session_from_env()

    env_obj = {field.name: value for field, value in s.ListFields()}
    assert env_obj == obj, 'bad session from environment'
