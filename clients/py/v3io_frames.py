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

"""Stream data from Nuclio into pandas DataFrame"""

import struct
import warnings
from collections import namedtuple
from datetime import datetime
from itertools import chain, count
from os import environ
from operator import attrgetter

import msgpack
import numpy as np
import pandas as pd
import requests

__version__ = '0.1.1'


class Error(Exception):
    """v3io_frames Exception"""


class BadRequest(Exception):
    """An error in query"""


class MessageError(Error):
    """An error in message"""


class CreateError(Error):
    """An error in table creation"""


class DeleteError(Error):
    """An error in table deletion"""


class DType:
    write_slice_key = None


class IntDType(DType):
    dtype = '[]int'
    slice_key = 'ints'
    pd_dtype = np.int64
    py_types = (np.integer, int)
    to_py = int

    @staticmethod
    def to_pylist(col):
        return col.tolist()


class FloatDType(DType):
    dtype = '[]float64'
    slice_key = 'floats'
    pd_dtype = np.float64
    py_types = (np.inexact, float)
    to_py = float

    @staticmethod
    def to_pylist(col):
        return col.fillna(0.0).tolist()


def identity(val):
    return val


class StringDType(DType):
    dtype = '[]string'
    slice_key = 'strings'
    pd_dtype = str
    py_types = str
    to_py = identity

    @staticmethod
    def to_pylist(col):
        return col.fillna('').tolist()


class TimeDType(DType):
    dtype = '[]time.Time'
    slice_key = 'times'
    pd_dtype = 'timedelta64[ns]'
    py_types = pd.Timestamp
    to_py = attrgetter('value')
    write_slice_key = 'ns_times'

    @staticmethod
    def to_pylist(col):
        return col.fillna(pd.Timestamp.min).values.tolist()


class BoolDType(DType):
    dtype = '[]bool'
    slice_key = 'bools'
    pd_dtype = bool
    py_types = (bool, np.bool_)
    to_py = bool  # TODO: Is this what we want?

    @staticmethod
    def to_pylist(col):
        return col.fillna(False).tolist()


dtypes = {dtype.dtype: dtype for dtype in DType.__subclasses__()}
Schema = namedtuple('Schema', 'type namespace name doc aliases fields key')
SchemaField = namedtuple('SchemaField', 'name doc default type properties')


class Client(object):
    """Client is a nuclio stream client"""

    def __init__(self, url='', api_key=''):
        """
        Parameters
        ----------
        url : string
            Server URL (if empty will use V3IO_URL environment variable)
        api_key : string
            API key (if empty will use V3IO_API_KEY environment variable)
        """
        self.url = url or environ.get('FRAMESD_URL')
        if not self.url:
            raise ValueError('missing URL')

        self.api_key = api_key or environ.get('V3IO_API_KEY')

    def read(self, backend='', query='', table='', columns=None, filter='',
             group_by='', limit=0, data_format='', row_layout=False,
             max_in_message=0, marker='', **kw):
        """Run a query in nuclio

        Parameters
        ----------
        backend : str
            Backend name
        query : str
            Query in SQL format
        table : str
            Table to query (can't be used with query)
        columns : []str
            List of columns to pass (can't be used with query)
        filter : str
            Query filter (can't be used with query)
        group_by : str
            Query group by (can't be used with query)
        limit: int
            Maximal number of rows to return
        data_format : str
            Data format
        row_layout : bool
            Weather to use row layout (vs the default column layout)
        max_in_message : int
            Maximal number of rows per message
        marker : str
            Query marker (can't be used with query)
        **kw
            Extra parameter for specific backends

        Returns:
            A pandas DataFrame iterator. Each DataFrame will have "labels"
            attribute.
        """
        request = {
            'backend': backend,
            'table': table,
            'query': query,
            'table': table,
            'columns': columns,
            'filter': filter,
            'group_by': group_by,
            'limit': limit,
            'data_format': data_format,
            'row_layout': row_layout,
            'max_in_message': max_in_message,
            'marker': marker,
        }

        request.update(kw)
        convert_go_times(kw, ('start', 'end'))

        self._validate_read_request(request)
        url = self.url + '/read'
        resp = requests.post(
            url, json=request, headers=self._headers(json=True), stream=True)
        if not resp.ok:
            raise Error('cannot call API - {}'.format(resp.text))

        return self._iter_dfs(resp.raw)

    def write(self, backend, table, dfs, labels=None, max_in_message=0):
        """Write to table

        Parameters
        ----------
        backend : str
            Backend name
        table : str
            Table to write to
        dfs : iterable of DataFrame or a single data frame
            Frames to write
        labels : dict
            Labels for current write
        max_in_message : int
            Maximal number of rows in a message

        Returns:
            Write result
        """

        if isinstance(dfs, pd.DataFrame):
            dfs = [dfs]

        request = msgpack.packb({
            'backend': backend,
            'table': table,
            'labels': labels,
            'more': True,
        })

        url = self.url + '/write'
        headers = self._headers()
        headers['Content-Encoding'] = 'chunked'
        data = chain([request], self._iter_chunks(dfs, labels, max_in_message))
        resp = requests.post(url, headers=headers, data=data)

        if not resp.ok:
            raise Error('cannot call API - {}'.format(resp.text))

        return resp.json()

    def create(self, backend, table, attrs=None, schema=None):
        """Create a table

        Parameters
        ----------
        backend : str
            Backend name
        table : str
            Table to create
        attrs : dict
            Table attributes
        schema: Schema or None
            Table schema

        Raises
        ------
        CreateError:
            On request error or backend error
        """
        if not backend:
            raise CreateError('empty backend')

        if not table:
            raise CreateError('empty table')

        schema = None if schema is None else encode_schema(schema)

        request = {
            'backend': backend,
            'table': table,
            'attributes': attrs,
            'schema': schema,
        }

        url = self.url + '/create'
        headers = self._headers()
        resp = requests.post(url, headers=headers, json=request)
        if not resp.ok:
            raise CreateError(resp.reason)

    def delete(self, backend, table, filter='', force=False, start='', end=''):
        """Delete a table

        Parameters
        ----------
        backend : str
            Backend name
        table : str
            Table to create
        filter : str
            Filter for selective delete
        force : bool
            Force deletion
        start : string
            Delete since start (TSDB/Stream)
        end : string
            Delete up to end (TSDB/Stream)

        Raises
        ------
        DeleteError
            On request error or backend error
        """

        if not backend:
            raise DeleteError('empty backend')

        if not table:
            raise DeleteError('empty table')

        request = {
            'backend': backend,
            'table': table,
            'filter': filter,
            'force': force,
            'start': start,
            'end': end,
        }

        convert_go_times(request, ('start', 'end'))

        url = self.url + '/delete'
        headers = self._headers()
        # TODO: Make it DELETE ?
        resp = requests.post(url, headers=headers, json=request)
        if not resp.ok:
            raise CreateError(resp.reason)

    def _headers(self, json=False):
        headers = {}
        if json:
            headers['Content-Type'] = 'application/json'

        if self.api_key:
            headers['Authorization'] = self.api_key

        return headers

    def _validate_read_request(self, request):
        if not (request.get('table') or request.get('query')):
            raise BadRequest('missing data')

        # TODO: More validation

    def _iter_dfs(self, resp):
        unpacker = msgpack.Unpacker(resp, ext_hook=ext_hook, raw=False)

        for msg in unpacker:
            yield self._msg2df(msg)

    def _msg2df(self, msg):
        # message format:
        #   columns: list of column message
        #   indices: list of column message
        #   labels: dict
        icols = enumerate(msg['columns'])
        columns = [self._handle_col_msg(i, col) for i, col in icols]
        df = pd.concat(columns, axis=1)

        idxs = enumerate(msg.get('indices', []))
        indices = [self._handle_col_msg(i, col) for i, col in idxs]

        if len(indices) == 1:
            df.index = indices[0]
        elif len(indices) > 1:
            df.index = pd.MultiIndex.from_arrays(indices)

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            df.labels = msg.get('labels', {})

        return df

    def _handle_col_msg(self, i, col):
        val = col.get('slice')
        if val:
            return self._handle_slice_col(val)

        val = col.get('label')
        if val:
            return self._handle_label_col(val)

        raise MessageError('{}: empty column message'.format(i))

    def _handle_slice_col(self, col):
        for dtype in dtypes.values():
            data = col.get(dtype.slice_key)
            if data is not None:
                return pd.Series(data, name=col['name'])

        return self._new_empty_col(col['name'], col['dtype'])

    def _handle_label_col(self, col):
        size = col['size']
        if size == 0:
            return self._new_empty_col(col['name'], col['dtype'])

        codes = np.zeros(size)
        cat = pd.Categorical.from_codes(codes, categories=[col['value']])
        return pd.Series(cat, name=col['name'])

    def _new_empty_col(self, name, msg_dtype):
        # Empty column
        dtype = dtypes.get(msg_dtype)
        if not dtype:
            raise MessageError(
                '{}: unknown data type: {}'.format(name, msg_dtype))
        return pd.Series(name=name, dtype=dtype.pd_dtype)

    def _encode_df(self, df, labels=None):
        msg = {
            'columns': [self._encode_col(df[name]) for name in df.columns],
        }

        if labels:
            msg['labels'] = labels

        if self._should_encode_index(df):
            if hasattr(df.index, 'levels'):
                by_name = df.index.get_level_values
                names = df.index.names
                serieses = (by_name(name).to_series() for name in names)
                msg['indices'] = [self._encode_col(s) for s in serieses]
            else:
                msg['indices'] = [self._encode_col(df.index.to_series())]

        return msgpack.packb(msg, strict_types=True)

    def _encode_col(self, col, name='', can_label=True):
        dtype = dtype_of(col.iloc[0])
        data = {
            'name': name or col.name,
            'dtype': dtype.dtype,
        }

        key = dtype.write_slice_key or dtype.slice_key
        data[key] = dtype.to_pylist(col)
        return {'slice': data}

    def _index_name(self, df):
        """Find a name for index column that does not collide with column
        names"""
        candidates = chain(
            [df.index.name, 'index'],
            ('index_{}'.format(i) for i in count()),
        )

        names = set(df.columns)
        for name in candidates:
            if name not in names:
                return name

    def _should_encode_index(self, df):
        if df.index.name:
            return True

        return not isinstance(df.index, pd.RangeIndex)

    def _iter_chunks(self, dfs, labels, max_in_message):
        for df in dfs:
            for cdf in self._chunk_df(df, max_in_message):
                yield self._encode_df(cdf, labels)

    def _chunk_df(self, df, size):
        size = size if size else len(df)

        i = 0
        while i < len(df):
            yield df[i:i+size]
            i += size


def datetime_fromnsec(sec, nsec):
    """Create datetime object from seconds and nanoseconds"""
    return datetime.fromtimestamp(sec).replace(microsecond=nsec//1000)


def unpack_time(value):
    """Unpack time packed by Go"""
    # See https://github.com/vmihailenco/msgpack/blob/master/time.go
    if len(value) == 4:
        sec, = struct.unpack('>L', value)
        return datetime.fromtimestamp(sec)

    if len(value) == 8:
        sec, = struct.unpack('>Q', value)
        nsec = sec >> 34
        sec &= 0x00000003ffffffff
        return datetime_fromnsec(sec, nsec)

    if len(value) == 12:
        nsec, = struct.unpack('>L', value[:4])
        sec, = struct.unpack('>Q', value[4:])
        return datetime_fromnsec(sec, nsec)

    raise ValueError(f'unknown time message length: {len(value)}')


def ext_hook(code, value):
    if code == -1:
        return unpack_time(value)

    raise ValueError(f'unknown ext code - {code}')


def dtype_of(val):
    # bool is subclass of int
    if type(val) in BoolDType.py_types:
        return BoolDType

    for dtype in dtypes.values():
        if isinstance(val, dtype.py_types):
            return dtype

    raise TypeError(f'unknown type - {val!r}')


def to_py(val):
    return dtype_of(val).to_py(val)


def encode_schema(schema):
    obj = schema._asdict()
    if obj['fields']:
        obj['fields'] = [field._asdict() for field in obj['fields']]
    return obj


def format_go_time(dt):
    """Format datetime in Go's time.RFC3339Nano format which looks like
    2018-10-04T15:08:53.229364634+03:00

    If dt tzinfo is None, UTC will be used
    """
    prefix = dt.strftime('%Y-%m-%dT%H:%M:%S')
    nsec = dt.microsecond * 1000
    tz = dt.strftime('%z') or '+0000'
    return '{}.{}{}:{}'.format(prefix, nsec, tz[:3], tz[3:5])


def convert_go_times(d, keys):
    """Convert datetime to Go's time format. This will change d *in place*"""
    for key in keys:
        value = d.get(key)
        if isinstance(value, datetime):
            # Go's RFC3339Nano 2018-10-04T15:08:53.229364634+03:00
            d[key] = format_go_time(value)
