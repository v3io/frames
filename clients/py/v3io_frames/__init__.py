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

__version__ = '0.2.2'

import re


from .http import Client as HTTPClient  # noqa
from .grpc import Client as gRPCClient  # noqa
from .errors import *  # noqa
from .frames_pb2 import TableSchema as Schema, SchemaKey, FAIL, IGNORE  # noqa
from .pbutils import SchemaField, Session  # noqa 

GRPC_PROTOCOL = 'grpc'
HTTP_PROTOCOL = 'http'


def Client(address='localhost:8081', protocol=GRPC_PROTOCOL,
           data_url='', container='', path='', user='', password='', token=''):
    """Return a new client.

    Parameters
    ----------
    address : str
        framesd backend address
    protocol : str
        Client protocol (GRPC_PROTOCOL or HTTP_PROTOCOL)
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

    if protocol not in (GRPC_PROTOCOL, HTTP_PROTOCOL):
        raise ValueError('unknown protocol - {}'.format(protocol))

    # TODO: Remove schema ourselves? What about Python Zen #12?
    if protocol == GRPC_PROTOCOL and re.match('^http(s)?://', address):
        raise ValueError('grpc address should not have http:// prefix')

    session = Session(
        url=data_url,
        container=container,
        path=path,
        user=user,
        password=password,
        token=token,
    )

    if protocol == HTTP_PROTOCOL:
        return HTTPClient(address, session)

    return gRPCClient(address, session)
