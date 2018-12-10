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

from os import environ
import warnings

from google.protobuf.message import Message
import google.protobuf.pyext._message as message
import pandas as pd
import numpy as np
import pytz

from . import frames_pb2 as fpb
from .dtypes import dtype_of, dtypes
from .errors import MessageError, WriteError

_ts = pd.Series(pd.Timestamp(0))
_time_dt = _ts.dtype
_time_tz_dt = _ts.dt.tz_localize(pytz.UTC).dtype
pb_list_types = (
    message.RepeatedCompositeContainer,
    message.RepeatedScalarContainer,
)


def pb_value(v):
    """Convert Python type to frames_pb2.Value"""
    if v is None:
        return None

    dtype = dtype_of(v)
    kw = {dtype.val_key: v}
    return fpb.Value(**kw)


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
        url=url,
        container=container,
        path=path,
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


def msg2df(frame):
    cols = {col.name: col2series(col) for col in frame.columns}
    df = pd.DataFrame(cols)

    indices = [col2series(idx) for idx in frame.indices]
    if len(indices) == 1:
        df.index = indices[0]
    elif len(indices) > 1:
        df.index = pd.MultiIndex.from_arrays(indices)

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        df.labels = pb2py(frame.labels)
    return df


def col2series(col):
    for dtype in dtypes:
        if col.dtype == dtype.dtype:
            data = getattr(col, dtype.col_key)
            break
    else:
        raise MessageError('unknown dtype - {}'.format(col.dtype))

    if col.dtype == fpb.TIME:
        data = pd.to_datetime(data)

    series = pd.Series(data, name=col.name)
    if col.kind == col.LABEL:
        series = series.reindex(pd.RangeIndex(col.size), method='pad')

    return series


def df2msg(df, labels=None):
    indices = None
    if should_encode_index(df):
        if hasattr(df.index, 'levels'):
            by_name = df.index.get_level_values
            names = df.index.names
            serieses = (by_name(name).to_series() for name in names)
            indices = [series2col(s) for s in serieses]
        else:
            indices = [series2col(df.index.to_series())]

    return fpb.Frame(
        columns=[series2col(df[name]) for name in df.columns],
        indices=indices,
        labels=pb_map(labels),
    )


def series2col(s):
    kw = {
        'name': s.name or '',
        'kind': fpb.Column.SLICE,
    }

    if is_int_dtype(s.dtype):
        kw['dtype'] = fpb.INTEGER
        kw['ints'] = s
    elif is_float_dtype(s.dtype):
        kw['dtype'] = fpb.FLOAT
        kw['floats'] = s
    elif s.dtype == np.object:  # Pandas dtype for str is object
        kw['strings'] = s
        kw['dtype'] = fpb.STRING
    elif s.dtype == np.bool:
        kw['bools'] = s
        kw['dtype'] = fpb.BOOLEAN
    elif is_time_dtype(s.dtype):
        if s.dt.tz:
            s = s.dt.tz_localize(pytz.UTC)
        kw['times'] = s.astype(np.int64)
        kw['dtype'] = fpb.TIME
    else:
        raise WriteError(
            '{} - unsupported type - {}'.format(s.name, s.dtype))

    return fpb.Column(**kw)


def should_encode_index(df):
    if df.index.name:
        return True

    return not isinstance(df.index, pd.RangeIndex)


# We can't use a set since hash(np.int64) != hash(pd.Series([1]).dtype)
def is_int_dtype(dtype):
    return \
        dtype == np.int64 or \
        dtype == np.int32 or \
        dtype == np.int16 or \
        dtype == np.int8 or \
        dtype == np.int


def is_float_dtype(dtype):
    return \
        dtype == np.float64 or \
        dtype == np.float32 or \
        dtype == np.float16 or \
        dtype == np.float


def is_time_dtype(dtype):
    return dtype == _time_dt or dtype == _time_tz_dt
