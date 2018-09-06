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

from itertools import chain, count
from os import environ
import datetime
import struct

import msgpack
import numpy as np
import pandas as pd
import requests


__version__ = '0.1.0'


class Error(Exception):
    """v3io_frames Exception"""


class BadRequest(Exception):
    """An error in query"""


class MessageError(Error):
    """An error in message"""


class Client(object):
    """Client is a nuclio stream client"""

    def __init__(self, url, api_key=''):
        """
        Parameters
        ----------
        url : string
            Server URL
        api_key : string
            API key
        """
        self.url = url or environ.get('V3IO_URL')
        if not self.url:
            raise ValueError('missing URL')

        self.api_key = api_key or environ.get('V3IO_API_KEY')

    def read(self, backend='', query='', table='', columns=None, filter='',
             group_by='', limit=0, data_format='', row_layout=False,
             max_in_message=0, marker='', extra=None):
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
        extra : object
            Extra parameter for specific backends

        Returns:
            A pandas DataFrame iterator
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
            'extra': extra,
        }
        self._validate_read_request(request)
        url = self.url + '/read'
        resp = requests.post(
            url, json=request, headers=self._headers(json=True), stream=True)
        if not resp.ok:
            raise Error('cannot call API - {}'.format(resp.text))

        return self._iter_dfs(resp.raw)

    def write(self, backend, table, dfs, max_in_message=0):
        """Write to table

        Parameters
        ----------
        backend : str
            Backend name
        table : str
            Table to write to
        dfs : iterable of DataFrame
            Frames to write
        max_in_message : int
            Maximal number of rows in a message

        Returns:
            Write result
        """

        params = {
            'backend': backend,
            'table': table,
        }

        url = self.url + '/write'
        headers = self._headers()
        headers['Content-Encoding'] = 'chunked'
        chunks = self._iter_chunks(dfs, max_in_message)
        resp = requests.post(
            url, headers=self._headers(), params=params, data=chunks)

        if not resp.ok:
            raise Error('cannot call API - {}'.format(resp.text))

        return resp.json()

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
        #   columns: list of column names
        #   slice_cols: dict of name -> column
        #   label_cols: dict of name -> column

        df_data = {}
        for name in msg['columns']:
            col = msg['slice_cols'].get(name)
            if col is not None:
                df_data[name] = self._handle_slice_col(col)
                continue

            col = msg['label_cols'].get(name)
            if col is not None:
                df_data[name] = self._handle_label_col(col)
                continue

            raise MessageError('no data for column {!r}'.format(name))

        if not df_data:
            return None

        df = pd.DataFrame(df_data)

        name = msg.get('index_name')
        if name:
            if name not in df.columns:
                err = 'index ({!r}) not in columns'.format(name)
                raise MessageError(err)

            df.index = df[name]

        return df

    def _handle_slice_col(self, col):
        for field in ['ints', 'floats', 'strings', 'times']:
            data = col.get(field)
            if data is not None:
                return pd.Series(data, name=col['name'])

        raise MessageError('column without data')

    def _handle_label_col(self, col):
        codes = np.zeros(col['size'])
        return pd.Categorial.from_codes(codes, categories=[col['name']])

    def _encode_df(self, df):
        msg = {
            'columns': [],
            'index_name': '',
            'slice_cols': {},
            'label_cols': {},
        }

        for name in df.columns:
            msg['columns'].append(name)
            col = df[name]

            data = self._encode_col(col, dtype_of(col.iloc[0]))
            if 'size' in data:  # label column
                msg['label_cols'][name] = data
            else:
                msg['slice_cols'][name] = data

        if self._should_encode_index(df):
            name = self._index_name(df)
            dtype = dtype_of(df.index[0])
            data = self._encode_col(df.index, dtype, name, can_label=False)
            msg['slice_cols'][name] = data
            msg['index_name'] = name

        return msgpack.packb(msg, strict_types=True)

    def _encode_col(self, col, dtype, name='', can_label=True):
        data = {
            'name': name or col.name,
            'dtype': dtype,
        }

        # Same repeating value, encode as label column
        if can_label and col.nunique() == 1:
            data['value'] = to_py(col.iloc[0])
            data['size'] = len(col)
            return data

        key, values = to_pylist(col)
        data[key] = values

        return data

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

    def _iter_chunks(self, dfs, max_in_message):
        for df in dfs:
            for cdf in self._chunk_df(df, max_in_message):
                yield self._encode_df(df)

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
    if isinstance(val, np.integer):
        return '[]int'
    elif isinstance(val, np.inexact):
        return '[]float'
    elif isinstance(val, str):
        return '[]string'
    elif isinstance(val, pd.Timestamp):
        return '[]time.Time'

    raise TypeError(f'unknown type - {val!r}')


def to_py(val):
    if isinstance(val, np.integer):
        return int(val)
    elif isinstance(val, np.inexact):
        return float(val)
    elif isinstance(val, str):
        return val
    elif isinstance(val, pd.Timestamp):
        return val.value

    raise TypeError('unsupported type - {}'.format(type(val)))


def to_pylist(col):
    if issubclass(col.dtype.type, (np.integer, int)):
        return 'ints', col.tolist()
    elif issubclass(col.dtype.type, (np.inexact, float)):
        return 'floats', col.tolist()
    elif isinstance(col.iloc[0], str):
        return 'strings', col.tolist()
    elif isinstance(col.iloc[0], pd.Timestamp):
        return 'ns_times', col.values.tolist()  # Convert to nanoseconds

    raise TypeError('unknown type - {}'.format(col.dtype))
