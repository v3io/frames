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

import pandas as pd

from . import frames_pb2 as fpb
from .errors import (CreateError, DeleteError, ExecuteError, ReadError,
                     WriteError)

FAIL = fpb.FAIL


class ClientBase:
    def __init__(self, address, session):
        """Create new client

        Parameters
        ----------
        address : str
            framesd server address
        session : Session
            Session object
        """
        address = address or environ.get('V3IO_FRAMES_ADDR')
        if not address:
            raise ValueError('empty address')
        self.address = self._fix_address(address)
        self.session = session

    def read(self, backend='', table='', query='', columns=None, filter='',
             group_by='', limit=0, data_format='', row_layout=False,
             max_in_message=0, marker='', iterator=False, **kw):
        """Run a query in nuclio

        Parameters
        ----------
        backend : str
            Backend name
        table : str
            Table to query (can't be used with query)
        query : str
            Query in SQL format
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
        iterator : bool
            Return iterator of DataFrames or (if False) just one DataFrame
        **kw
            Extra parameter for specific backends

        Returns:
            A pandas DataFrame iterator. Each DataFrame will have "labels"
            attribute. If `iterator` is False will return a single DataFrame.
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

    def write(self, backend, table, dfs, expression='', labels=None,
              max_in_message=0):
        """Write to table

        Parameters
        ----------
        backend : str
            Backend name
        table : str
            Table to write to
        dfs : iterable of DataFrame or a single data frame
            Frames to write
        experssion : str
            Write expression
        labels : dict
            Set of lables
        max_in_message : int
            Maximal number of rows to send per message

        Returns:
            Write result
        """
        self._validate_request(backend, table, WriteError)
        if isinstance(dfs, pd.DataFrame):
            dfs = [dfs]

        if max_in_message:
            dfs = self._iter_chunks(dfs, max_in_message)

        request = self._encode_write(backend, table, expression)
        return self._write(request, dfs, labels)

    def create(self, backend, table, attrs=None, schema=None, if_exists=FAIL):
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
        """Delete a table

        Parameters
        ----------
        backend : str
            Backend name
        table : str
            Table to create
        filter : str
            Filter for selective delete
        start : string
            Delete since start (TSDB/Stream)
        end : string
            Delete up to end (TSDB/Stream)
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
        """Execute a command

        Parameters
        ----------
        backend : str
            Backend name
        table : str
            Table to create
        command : str
            Command to execute
        args : dict
            Command arguments
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

    def _encode_write(self, backend, table, expression):
        # TODO: InitialData?
        return fpb.InitialWriteRequest(
            session=self.session,
            backend=backend,
            table=table,
            expression=expression,
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
                yield df[i:i+size]
                i += size
