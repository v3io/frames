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

from datetime import datetime
from os import environ

import numpy as np
import pandas as pd

from google.protobuf.message import Message
import google.protobuf.pyext._message as message

from . import frames_pb2 as fpb

pb_list_types = (
    message.RepeatedCompositeContainer,
    message.RepeatedScalarContainer,
)


def pb_value(v):
    """Convert Python type to frames_pb2.Value"""
    if v is None:
        return None

    if isinstance(v, (bool, np.bool_)):
        return fpb.Value(bval=v)

    if isinstance(v, (np.inexact, float)):
        return fpb.Value(fval=v)

    if isinstance(v, (int, np.integer)):
        return fpb.Value(ival=v)

    if isinstance(v, str):
        return fpb.Value(sval=v)

    if isinstance(v, (datetime, pd.Timestamp)):
        # Convert to epoch nano
        v = pd.Timestamp(v).to_datetime64().astype(np.int64)
        return fpb.Value(tval=v)

    raise TypeError('unsupported Value type - {}'.format(type(v)))


def pb_map(d):
    """Convert map values to frames_pb2.Value"""
    return None if d is None else {k: pb_value(v) for k, v in d.items()}


def SchemaField(name=None, doc=None, default=None, type=None, properties=None):
    """A schema field"""
    # We return a frames_pb2.SchemaField from Python types
    return fpb.SchemaField(
            name=name,
            doc=doc,
            default=pb_value(default),
            type=type,
            properties=pb_map(properties),
        )


def Session(url='', container='', path='', user='', password='', token=''):
    """Return a new session.

    Will populate missing values from environment. Environment variables have
    V3IO_ prefix (e.g. V3IO_URL)

    Parameters
    ----------
    url : str
        Backend URL
    container : str
        Container name
    path : str
        Path in container
    user : str
        Login user
    password : str
        Login password
    token : str
        Login token

    Returns:
        A session object
    """
    return fpb.Session(
        url=url or environ.get('V3IO_URL', ''),
        container=container or environ.get('V3IO_CONTAINER', ''),
        path=path or environ.get('V3IO_PATH', ''),
        user=user or environ.get('V3IO_USER', ''),
        password=password or environ.get('V3IO_PASSWORD', ''),
        token=token or environ.get('V3IO_TOKEN', ''),
    )


def pb2py(obj):
    """Convert protobuf object to Python object"""
    if isinstance(obj, fpb.Value):
        return getattr(obj, obj.WhichOneof('value'))

    if isinstance(obj, Message):
        return {
            field.name: pb2py(value) for field, value in obj.ListFields()
        }

    if isinstance(obj, pb_list_types):
        return [pb2py(v) for v in obj]

    if isinstance(obj, message.MessageMapContainer):
        return {
            key: pb2py(value) for key, value in obj.items()
        }

    return obj
