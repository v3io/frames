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

__version__ = '0.4.1'

import json
import pandas as pd
from os import environ
from urllib.parse import urlparse

from .errors import *  # noqa
from .frames_pb2 import TableSchema as Schema, SchemaKey, FAIL, IGNORE, Session  # noqa
from .grpc import Client as gRPCClient  # noqa
from .http import Client as HTTPClient  # noqa
from .pbutils import SchemaField  # noqa

SESSION_ENV_KEY = 'V3IO_SESSION'

_known_protocols = {'grpc', 'http', 'https'}


def Client(address='', data_url='', container='', path='', user='',
           password='', token='', session_id='', frame_factory=pd.DataFrame,
           concat=pd.concat, persist_connection=False):
    """Creates a new Frames client object
    NOTE: User authentication must be done using any of the following methods:
    setting the `token` parameter or the V3IO_ACCESS_KEY environment variable
    to a valid access key; setting the `user` and `password` parameters or the
    V3IO_USERNAME and V3IO_PASSWORD environment variables to a valid username
    and a matching password.

    Parameters
    ----------
    address (Required) : str
        Address of the Frames service (framesd). Use the grpc:// prefix for
        gRPC (default; recommended) or the http:// prefix for HTTP; for local
        execution on an Iguazio Data Science Platform ("the platform"), use
        `framesd:8081` (gRPC; recommended) or `framesd:8080` (HTTP)
    data_url (Optional): str
        Web-API base URL for accessing the backend data; default: the base URL
        configured for the Frames service; for the platform backends, this is
        typically the HTTPS URL of the web-APIs service of the parent tenant
    container : str
        Container name (session info)
    path : str
        DEPRECATED
    user (Optional): str
        Username of a user with permissions to access the backend data; cannot
        be used with `token`
    password (Required when `user` is set): str
        Password for the user configured in the `user` parameter; cannot be
        used with `token`
    token (Optional): str
        Token (access key) that allows access to the backend data; cannot be
        used with `user` or `password`
    session_id : str
        Session ID; currently, unused
    frame_factory (Optional) : class
        DataFrame factory; currently, pandas DataFrame (default)
    concat (Optional): function
        Function for concatenating DataFrames; default: pandas concat
    persist_connection: bool
        Whether the underlying connection should persist between requests.
        This only effect http client today. grpc client persists channels.
        Use True where the same client is rarely instantiated but requests
        are made often. When True is used, due to the nature of the
        underlying clients, rapid instantiation of the client
        may cause failures (e.g. HTTP NewConnectionError)

    Return Value
    ----------
    A new `Client` object
    """
    if not (address.startswith("grpc://") or address.startswith("https://") or address.startswith("http://")):
        address = "grpc://" + address
    protocol = urlparse(address).scheme or 'grpc'
    if protocol not in _known_protocols:
        raise ValueError('unknown protocol - {}'.format(protocol))
    if (user != "" or password != "") and token != "":
        raise ValueError('both basic username-password and '
                         'access-key authentication were provided')

    env = session_from_env()

    session = Session(
        url=data_url or env.url,
        container=container or env.container,
        path=path or env.path,
        user=user or env.user or environ.get('V3IO_USERNAME'),
        password=password or env.password or environ.get('V3IO_PASSWORD'),
        id=session_id or env.id,
    )

    if user == "" and password == "":
        session.token = token or env.token or \
                        environ.get('V3IO_ACCESS_KEY') or ''

    cls = gRPCClient if protocol == 'grpc' else HTTPClient
    return cls(address, session, persist_connection,
               frame_factory=frame_factory, concat=concat)


def session_from_env():
    """Load session from V3IO_SESSION environment variable (JSON encoded)"""
    data = environ.get(SESSION_ENV_KEY)
    if data is None:
        return Session()

    obj = json.loads(data)
    return Session(**obj)
