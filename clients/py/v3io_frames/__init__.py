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

__version__ = '0.3.1'

from urllib.parse import urlparse


from .http import Client as HTTPClient  # noqa
from .grpc import Client as gRPCClient  # noqa
from .errors import *  # noqa
from .frames_pb2 import TableSchema as Schema, SchemaKey, FAIL, IGNORE  # noqa
from .pbutils import SchemaField, Session  # noqa 


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

    session = Session(
        url=data_url,
        container=container,
        path=path,
        user=user,
        password=password,
        token=token,
    )

    cls = gRPCClient if protocol == 'grpc' else HTTPClient
    return cls(address, session)
