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

import pandas as pd
from os import environ

from . import frames_pb2 as fpb
from .errors import (CreateError, DeleteError, ExecuteError, ReadError,
                     WriteError)

FAIL = fpb.FAIL


class ClientBase:
    def __init__(self, address, session, frame_factory=pd.DataFrame,
                 concat=pd.concat):
        """Creates a new Frames client object

        Parameters
        ----------
        address : str
            Address of the Frames service (framesd)
        session : Session
            Session object
        frame_factory : class
            DataFrame factory; currently, pandas and cuDF are supported
        concat : function
            Function for concatenating DataFrames; default: pandas concat

        Return Value
        ----------
        A new `Client` object
        """
        address = address or environ.get('V3IO_FRAMESD')
        if not address:
            raise ValueError('empty address')
        self.address = self._fix_address(address)
        self.session = session
        self.frame_factory = frame_factory
        self.concat = concat

    def read(self, backend='', table='', query='', columns=None, filter='',
             group_by='', limit=0, data_format='', row_layout=False,
             max_in_message=0, marker='', iterator=False, **kw):
        """Reads data from a table or stream (run a data query)

        Common Parameters
        ----------
        backend : str
            Backend name ('kv', 'tsdb', 'stream')
        table : str
            Table to query; ignored when `query` references a specific table
        query : str
            Query string, in SQL format
        columns : []str
            List of item attributes (columns) to return;
            can't be used with `query`
        filter : str
            Query filter; can't be used with `query`
        group_by : str
            A group-by query string; can't be used with `query`
        limit: int
            Maximum number of rows to return
        data_format : str
            Data format
        row_layout : bool
            True to use a row layout; False (default) to use a column layout
            [Not supported in this version]
        max_in_message : int
            Maximum number of rows per message
        marker : str
            Query marker; can't be used with the `query` parameter
        iterator : bool
            True - return a DataFrames iterator;
            False (default) - return a single DataFrame
        **kw
            Extra parameter for specific backends

        Return Value
        ----------
            - When `iterator` is False (default) - returns a single DataFrame.
            - When `iterator` is True - returns a DataFrames iterator.
            The returned DataFrames include a "labels" DataFrame attribute with
            backend-specific data, if applicable.
        """
        if not backend:
            raise ReadError('no backend')
        if not (table or query):
            raise ReadError('missing data')
        # TODO: More validation

        if max_in_message > 0:
            iterator = True

        return self._read(
            backend, table, query, columns, filter,
            group_by, limit, data_format, row_layout,
            max_in_message, marker, iterator, **kw)

    def write(self, backend, table, dfs, expression='', condition='',
              labels=None, max_in_message=0, index_cols=None,
              partition_keys=None):
        """Writes data to a table or stream

        Parameters
        ----------
        backend : str
            Backend name
        table : str
            Table to write to
        dfs : a single DataFrame, a DataFrames list, or a DataFrames iterator
            DataFrames to write
        expression : str
            A platform update expression that determines the update logic for
            all items in the DataFrame [Not supported in this version]
        condition : str
            A platform condition expression that defines a condition for
            performing the write operation
        labels : dict
            Dictionary of labels; currently, used only with the "tsdb" backend
        max_in_message : int
            Maximum number of rows to send per message
        index_cols : []str
            List of column names to be used as the index columns for the write
            operation; by default, the DataFrame's index columns are used
        partition_keys : []str
            Partition keys [Not supported in this version]

        Return Value
        ----------
            Write result
        """
        self._validate_request(backend, table, WriteError)
        if isinstance(dfs, pd.DataFrame):
            dfs = [dfs]

        if max_in_message:
            dfs = self._iter_chunks(dfs, max_in_message)

        request = self._encode_write(
            backend, table, expression, condition, partition_keys)
        return self._write(request, dfs, labels, index_cols)

    def create(self, backend, table, attrs=None, schema=None, if_exists=FAIL):
        """Creates a new TSDB table or a stream

        Parameters
        ----------
        backend : str
            Backend name
        table : str
            Table to create
        attrs : dict
            A dictionary of backend-specific parameters (arguments)
        schema: Schema or None
            Table schema
        if_exists : int
            One of IGNORE or FAIL

        Raises
        ------
        CreateError:
            On request error or backend error
        """
        self._validate_request(backend, table, CreateError)
        return self._create(backend, table, attrs, schema, if_exists)

    def delete(self, backend, table, filter='', start='', end='',
               if_missing=FAIL):
        """Deletes a table or stream or specific table items

        Parameters
        ----------
        backend : str
            Backend name
        table : str
            Table to create
        filter : str
            Filter for selective delete
        start : string
             (`tsdb` backend only) Start (minimum) metric-sample time for the
             delete operation, as a string containing an RFC 3339 time, a Unix
             timestamp in milliseconds, a relative time (`"now"` or
             `"now-[0-9]+[mhd]"`, where `m` = minutes, `h` = hours, and `'d'` =
             days), or 0 for the earliest time; the default is <end time> - 1h
        end : string
             (`tsdb` backend only) End (maximum) metric-sample time for the
             delete operation, as a string containing an RFC 3339 time, a Unix
             timestamp in milliseconds, a relative time (`"now"` or
             `"now-[0-9]+[mhd]"`, where `m` = minutes, `h` = hours, and `'d'` =
             days), or 0 for the earliest time; the default is "now"
        if_missing : int
            One of IGNORE or FAIL

        Raises
        ------
        DeleteError
            On request error or backend error
        """
        self._validate_request(backend, table, DeleteError)
        return self._delete(backend, table, filter, start, end, if_missing)

    def execute(self, backend, table, command='', args=None, expression=''):
        """Executes a backend-specific command on a table or stream

        Parameters
        ----------
        backend : str
            Backend name
        table : str
            Table to create
        command : str
            Command to execute
        args : dict
            A dictionary of command-specific parameters (arguments)
        expression : str
            Command expression

        Raises
        ------
        ExecuteError
            On request error or backend error
        """
        self._validate_request(backend, table, ExecuteError)
        return self._execute(backend, table, command, args, expression)

    def _fix_address(self, address):
        return address

    def _encode_session(self, session):
        return session

    def _encode_write(self, backend, table, expression, condition,
                      partition_keys):
        # TODO: InitialData?
        return fpb.InitialWriteRequest(
            session=self.session,
            backend=backend,
            table=table,
            expression=expression,
            condition=condition,
            partition_keys=partition_keys,
        )

    def _validate_request(self, backend, table, err_cls):
        if not backend:
            raise err_cls('empty backend')

        if not table:
            raise err_cls('empty table')

    def _iter_chunks(self, dfs, size):
        for df in dfs:
            size = size if size else len(df)
            i = 0
            while i < len(df):
                yield df[i:i + size]
                i += size
