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

"""Stream data from/to Nuclio into pandas DataFrame"""

__version__ = '0.3.3'

from os import environ
import json
from urllib.parse import urlparse


from .http import Client as HTTPClient  # noqa
from .grpc import Client as gRPCClient  # noqa
from .errors import *  # noqa
from .frames_pb2 import TableSchema as Schema, SchemaKey, FAIL, IGNORE  # noqa
from .pbutils import SchemaField, Session  # noqa 


SESSION_ENV_KEY = 'V3IO_SESSION'

_known_protocols = {'grpc', 'http', 'https'}


def Client(address='', data_url='', container='', path='', user='',
           password='', token=''):
    """Return a new client.

    Parameters
    ----------
    address : str
        framesd backend address. Use grpc:// or http:// prefix to specify
        protocol (default is gRPC)
    data_url : str
        Backend URL (session info)
    container : str
        Container name (session info)
    path : str
        Path in container (session info)
    user : str
        Login user (session info)
    password : str
        Login password (session info)
    token : str
        Login token (session info)
    """

    protocol = urlparse(address).scheme or 'grpc'
    if protocol not in _known_protocols:
        raise ValueError('unknown protocol - {}'.format(protocol))

    env = session_from_env()

    session = Session(
        url=data_url or env.url,
        container=container or env.container,
        path=path or env.path,
        user=user or env.user,
        password=password or env.password,
        token=token or env.token,
    )

    cls = gRPCClient if protocol == 'grpc' else HTTPClient
    return cls(address, session)


def session_from_env():
    """Load session from V3IO_SESSION environment variable (JSON encoded)"""
    data = environ.get(SESSION_ENV_KEY)
    if data is None:
        return Session()

    obj = json.loads(data)
    return Session(**obj)
