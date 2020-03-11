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

import pytest
import v3io_frames as v3f


test_client_params = [
    ('grpc', v3f.gRPCClient),
    ('http', v3f.HTTPClient),
]


@pytest.mark.parametrize('proto,cls', test_client_params)
def test_client(proto, cls):
    address = '{}://localhost:8080'.format(proto)
    session_params = {
        'data_url': 'http://iguazio.com',
        'container': 'large one',
        'user': 'bugs',
        'password': 'duck season',
    }

    client = v3f.Client(address, **session_params)
    assert client.__class__ is cls, 'wrong class'
    for key, value in session_params.items():
        key = 'url' if key == 'data_url' else key
        assert getattr(client.session, key) == value, \
            'bad session value for {}'.format(key)


@pytest.mark.parametrize('proto,cls', test_client_params)
def test_client_wrong_params(proto, cls):
    address = '{}://localhost:8080'.format(proto)
    session_params = {
        'data_url': 'http://iguazio.com',
        'container': 'large one',
        'user': 'bugs',
        'password': 'duck season',
        'token': 'a quarter',
    }

    try:
        v3f.Client(address, **session_params)
        raise ValueError('expected fail but finished successfully')
    except ValueError:
        return


def test_partition_keys():
    class Proto(v3f.client.ClientBase):
        def _write(self, request, dfs, labels, index_cols):
            self.request = request

    c = Proto('localhost:8081', None)
    keys = ['pk1', 'pk2']
    c.write('backend', 'table', None, partition_keys=keys)
    assert c.request.partition_keys == keys, 'bad partition keys'
