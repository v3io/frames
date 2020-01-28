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
from os import environ
from urllib.parse import urlparse

import pandas as pd

from .errors import *  # noqa
from .frames_pb2 import TableSchema as Schema, SchemaKey, FAIL, IGNORE, Session  # noqa
from .grpc import Client as gRPCClient  # noqa
from .http import Client as HTTPClient  # noqa
from .pbutils import SchemaField  # noqa

SESSION_ENV_KEY = 'V3IO_SESSION'

_known_protocols = {'grpc', 'http', 'https'}


def Client(address='', data_url='', container='', path='', user='',
           password='', token='', session_id='', frame_factory=pd.DataFrame,
           concat=pd.concat):
    """Creates a new Frames client object

    Parameters
    ----------
    address : str
        Address of the Frames service (framesd). Use the grpc:// prefix for
        gRPC (default; recommended) or the http:// prefix for HTTP.
        Use `framesd:8081` (gRPC; recommended) or `framesd:8080` for local
        execution on an Iguazio Data Science Platform ("the platform").
    data_url : str
        Base URL for accessing the backend data
    container : str
        Container name (session info)
    path : str
        DEPRECATED
    user : str
        The username of a platform user with permissions to access the backend
        data; can't be used with `token`
    password : str
        A platform password for the user configured in the `user` parameter;
        required when `user` is set; can't be used with `token`
    token : str
        A valid platform access key that allows access to the backend data;
        can't be used with `user` and `password`
    session_id : str
        Session ID
    frame_factory : class
        DataFrame factory; currently, pandas DataFrame (default)
    concat : function
        Function for concatenating DataFrames; default: pandas concat

    Return Value
    ----------
    A new `Client` object
    """
    if not (address.startswith("grpc://") or
            address.startswith("https://") or
            address.startswith("http://")):
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
    return cls(address, session, frame_factory=frame_factory, concat=concat)


def session_from_env():
    """Load session from V3IO_SESSION environment variable (JSON encoded)"""
    data = environ.get(SESSION_ENV_KEY)
    if data is None:
        return Session()

    obj = json.loads(data)
    return Session(**obj)
