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
import pandas as pd

if sys.version_info[:2] < (3, 0):
    from urllib2 import urlopen, Request
else:
    from urllib.request import Request, urlopen

__version__ = '0.1.0'


class Error(Exception):
    """v3io_frames Exception"""


class MessageError(Error):
    """An error in message"""


class Client(object):
    """Client is a nuclio stream client"""

    def __init__(self, url, api_key='', orient='rows'):
        """
        Args:
            url: API url
            api_key: API key
            orient: Result orientations (rows or columns)
        """
        if orient not in ('rows', 'columns'):
            raise ValueError('unknown orient - {!r}'.format(orient))

        self.url = url
        self.api_key = api_key
        self.orient = orient

    def query(self, query):
        """Run a query in nuclio

        Args:
            query: Query to run

        Returns:
            A pandas DataFrame iterator
        """
        resp = self._call_v3io(query)
        return self._iter_dfs(resp)

    def _call_v3io(self, query):
        query_obj = {
            'query': query,
            'orient': self.orient,
            'limit': 100,
            'columns': ['first', 'last'],
        }

        headers = {
            'Authorization': self.api_key,
        }

        request = Request(
            self.url,
            data=json.dumps(query_obj).encode('utf-8'),
            headers=headers,
        )

        return urlopen(request)

    def _iter_dfs(self, resp):
        unpacker = msgpack.Unpacker(resp, ext_hook=ext_hook, raw=False)

        labels = {}
        names = None

        for msg in unpacker:
            # We keep last names & lables, allowing server to send only once
            labels = msg.get('labels', labels)
            names = msg.get('names', names)

            df = None
            cols = msg.get('columns')
            rows = msg.get('rows')
            if rows and cols:  # TODO: Should we raise here
                raise MessageError('both rows and columns returned')

            if cols:
                df = pd.DataFrame(cols)

            if rows:
                df = pd.DataFrame(rows, columns=names)

            if df is None:
                continue

            # TODO: What to do when label is already a column?
            for key, value in labels.items():
                df[key] = value

            yield df


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
