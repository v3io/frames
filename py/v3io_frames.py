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

import datetime
import json
import struct
import sys

import msgpack
import numpy as np
import pandas as pd

if sys.version_info[:2] < (3, 0):
    from urllib2 import urlopen, Request
else:
    from urllib.request import Request, urlopen

__version__ = '0.1.0'


class Error(Exception):
    """v3io_frames Exception"""


class BadQueryError(Exception):
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
        self.url = url
        self.api_key = api_key

    def read(self, typ='', data_format='', row_layout=False, query='',
             table='', columns=None, filter='', group_by='', limit=0,
             max_in_message=0, marker='', extra=None):
        """Run a query in nuclio

        Parameters
        ----------
        typ : str
            Request type
        data_format : str
            Data format
        row_layout : bool
            Weather to use row layout (vs the default column layout)
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
        max_in_message : int
            Maximal number of rows per message
        marker : str
            Query marker (can't be used with query)
        extra : object
            Extra parameter for specific backends

        Returns:
            A pandas DataFrame iterator
        """
        query_obj = {
            'type': typ,
            'data_format': data_format,
            'row_layout': row_layout,
            'query': query,
            'table': table,
            'columns': columns,
            'filter': filter,
            'group_by': group_by,
            'limit': limit,
            'max_in_message': max_in_message,
            'marker': marker,
            'extra': extra,
        }
        self._validate_query(query_obj)
        resp = self._call_v3io(query_obj)
        return self._iter_dfs(resp)

    def _validate_query(self, query):
        """Valites query

        Parameters
        ----------
        query : dict
            Query to send

        Raises
        ------
        BadQueryError
            If query is malformed
        """
        return  # TODO

    def _call_v3io(self, query):

        headers = {
            'Authorization': self.api_key,
        }

        request = Request(
            self.url,
            data=json.dumps(query).encode('utf-8'),
            headers=headers,
        )

        return urlopen(request)

    def _iter_dfs(self, resp):
        unpacker = msgpack.Unpacker(resp, ext_hook=ext_hook, raw=False)

        for msg in unpacker:
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
                continue

            yield pd.DataFrame(df_data)

    def _handle_slice_col(self, col):
        for field in ['ints', 'floats', 'strings', 'times']:
            data = col.get(field)
            if data is not None:
                return pd.Series(data, name=col['name'])

        raise MessageError('column without data')

    def _handle_label_col(self, col):
        codes = np.zeros(col['size'])
        return pd.Categorial.from_codes(codes, categories=[col['name']])


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
