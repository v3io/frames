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

import json
import requests
import struct
from base64 import b64decode
from datetime import datetime
from functools import partial, wraps
from itertools import chain
from requests.exceptions import RequestException
from urllib3.exceptions import HTTPError

from . import frames_pb2 as fpb
from .client import ClientBase, RawFrame
from .errors import (CreateError, DeleteError, Error, ExecuteError, ReadError,
                     WriteError)
from .frames_pb2 import Frame
from .pbutils import df2msg, msg2df, pb2py
from .pdutils import concat_dfs, should_reorder_columns

header_fmt = '<q'
header_fmt_size = struct.calcsize(header_fmt)


def connection_error(error_cls):
    """Re-raise v3f Exceptions from connection errors"""

    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kw):
            try:
                return fn(*args, **kw)
            except (RequestException, HTTPError) as err:
                raise error_cls(str(err))

        return wrapper

    return decorator


class Client(ClientBase):
    """Client is a nuclio stream HTTP client"""

    def _fix_address(self, address):
        if '://' not in address:
            return 'http://{}'.format(address)

        return address

    @connection_error(ReadError)
    def _read(self, backend, table, query, columns, filter, group_by, limit,
              data_format, row_layout, max_in_message, marker, iterator, get_raw, **kw):
        request = {
            'session': pb2py(self.session),
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
            'message_limit': max_in_message,
            'marker': marker,
        }

        convert_go_times(kw, ('start', 'end'))
        request.update(kw)

        url = self._url_for('read')
        resp = requests.post(
            url, verify=False, json=request, headers=self._headers(json=True),
            stream=True)
        if not resp.ok:
            raise Error('cannot call API - {}'.format(resp.text))

        do_reorder = should_reorder_columns(backend, query, columns)
        dfs = self._iter_dfs(resp.raw, columns, get_raw, do_reorder=do_reorder)

        if not iterator and not get_raw:
            return concat_dfs(dfs, self.frame_factory, self.concat)
        return dfs

    @connection_error(WriteError)
    def _write(self, request, dfs, labels, index_cols):
        url = self._url_for('write')
        headers = self._headers()
        headers['Content-Encoding'] = 'chunked'

        request = self._encode_msg(request)
        enc = self._encode_msg
        frames = (enc(df2msg(df, labels, index_cols)) for df in dfs)
        data = chain([request], frames)

        resp = requests.post(url, verify=False, headers=headers, data=data)

        if not resp.ok:
            raise Error('cannot call API - {}'.format(resp.text))

        return resp.json()

    @connection_error(CreateError)
    def _create(self, backend, table, attrs, schema, if_exists):
        request = {
            'session': pb2py(self.session),
            'backend': backend,
            'table': table,
            'attribute_map': attrs,
            'schema': pb2py(schema),
            'if_exists': if_exists,
        }

        url = self._url_for('create')
        headers = self._headers()
        resp = requests.post(url, verify=False, headers=headers, json=request)
        if not resp.ok:
            raise CreateError(resp.text)

    @connection_error(DeleteError)
    def _delete(self, backend, table, filter, start, end, if_missing):
        request = {
            'session': pb2py(self.session),
            'backend': backend,
            'table': table,
            'filter': filter,
            'start': start,
            'end': end,
            'if_missing': if_missing,
        }

        convert_go_times(request, ('start', 'end'))

        url = self._url_for('delete')
        headers = self._headers()
        # TODO: Make it DELETE ?
        resp = requests.post(url, verify=False, headers=headers, json=request)
        if not resp.ok:
            raise CreateError(resp.text)

    @connection_error(ExecuteError)
    def _execute(self, backend, table, command, args, expression):
        request = {
            'session': pb2py(self.session),
            'backend': backend,
            'table': table,
            'command': command,
            'args': args or {},
            'expression': expression,
        }

        url = self._url_for('exec')
        headers = self._headers()
        resp = requests.post(url, verify=False, headers=headers, json=request)
        if not resp.ok:
            raise ExecuteError(resp.text)

        try:
            out = resp.json()
        except json.JSONDecodeError as err:
            raise ExecuteError(str(err))

        frame = out.get('frame')
        if not frame:
            return

        msg = Frame.FromString(b64decode(frame))
        return msg2df(msg, self.frame_factory)

    def _url_for(self, action):
        return self.address + '/' + action

    def _headers(self, json=False):
        headers = {'Accept-Encoding': ''}
        if json:
            headers['Content-Type'] = 'application/json'

        return headers

    def _iter_dfs(self, resp, columns, get_raw, do_reorder=True):
        for msg in iter(partial(self._read_msg, resp), None):
            if msg.error:
                raise ReadError(msg.error)
            if get_raw:
                yield RawFrame(msg)
            else:
                yield msg2df(msg, self.frame_factory, columns, do_reorder)

    def _read_msg(self, resp):
        data = resp.read(header_fmt_size)
        if not data:
            return None

        if len(data) != header_fmt_size:
            raise ReadError('chopped header')

        size = struct.unpack(header_fmt, data)[0]
        data = resp.read(size)
        if len(data) != size:
            raise ReadError('chopped frame body')

        return fpb.Frame.FromString(data)

    def _encode_msg(self, msg):
        data = msg.SerializeToString()
        size = len(data)
        return struct.pack(header_fmt, size) + data


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
