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


class RawFrame:
    def __init__(self, proto_frame):
        self.raw_frame = proto_frame

    def names(self):
        """Returns all column names of the frame

        Return Value
        ----------
            Column names
        """
        column_names = []
        for col in self.raw_frame.columns:
            column_names.append(col.name)
        return column_names

    def column(self, column_name):
        """Returns a DataFrame column by column name

        Parameters
        ----------
        column_name : str
            Column name to get

        Return Value
        ----------
            Column object by the requested name
        """
        for col in self.raw_frame.columns:
            if col.name == column_name:
                return col

        raise ReadError('no column named {}'.format(column_name))

    def column_data(self, column_name):
        """Returns column data by column name

       Parameters
       ----------
       column_name : str
           Column name to get

       Return Value
       ----------
           List of values that represent the requested column's data
       """
        for col in self.raw_frame.columns:
            if col.name == column_name:
                if col.dtype == fpb.INTEGER:
                    return col.ints
                elif col.dtype == fpb.FLOAT:
                    return col.floats
                elif col.dtype == fpb.STRING:
                    return col.strings
                elif col.dtype == fpb.TIME:
                    return col.times
                elif col.dtype == fpb.BOOLEAN:
                    return col.bools
                else:
                    raise ReadError('{} - unsupported type - {}'.format(column_name, col.dtype))

        raise ReadError('No column named "{}"'.format(column_name))

    def labels(self):
        """Returns a DataFrame's labels

       Return Value
       ----------
           ('tsdb' and 'stream' backends only) The labels of the current DF
       """
        return self.raw_frame.labels

    def indices(self):
        """Returns a DataFrame's indices

       Return Value
       ----------
           List of column objects representing the indices of the DataFrame
       """
        return self.raw_frame.indices

    def columns(self):
        """Returns all DataFrame columns

       Return Value
       ----------
           List of column objects representing the columns of the frame
       """
        return self.raw_frame.columns

    def __len__(self):
        """Returns a DataFrame's length (number of rows)

       Return Value
       ----------
           Number of rows in the DataFrame
       """
        if len(self.raw_frame.columns) > 0:
            col = self.raw_frame.columns[0]
            if col.dtype == fpb.INTEGER:
                return len(col.ints)
            elif col.dtype == fpb.FLOAT:
                return len(col.floats)
            elif col.dtype == fpb.STRING:
                return len(col.strings)
            elif col.dtype == fpb.TIME:
                return len(col.times)
            elif col.dtype == fpb.BOOLEAN:
                return len(col.bools)
        return 0

    def is_null(self, index, column_name):
        """Indicates whether a specific DataFrame cell is null or not

       Parameters
       ----------
       index (Required) : str
           Row index of the desired cell
       column_name (Required) : str
           Column name of the desired cell

       Return Value
       ----------
           True - the cell is null
           False - the cell has a value
       """
        if len(self.raw_frame.null_values) == 0:
            return False

        for null_column in self.raw_frame.null_values[index].nullColumns:
            if null_column.key == column_name:
                return True
        return False


class ClientBase:
    def __init__(self, address, session, persist_connection=False,
                 frame_factory=pd.DataFrame, concat=pd.concat):
        """Creates a new Frames client object

        Parameters
        ----------
        address (Required) : str
            Address of the Frames service (framesd)
        session (Optional) : object
            Session object
        frame_factory (Optional) : class
            DataFrame factory; currently, pandas (default) or cuDF
        concat (Optional) : function
        persist_connection:
            Boolean, whether to persist underlying client connection
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
        self._persist_connection = persist_connection
        self.frame_factory = frame_factory
        self.concat = concat

    def read(self, backend, table='', query='', columns=None, filter='',
             group_by='', limit=0, data_format='', row_layout=False,
             max_rows_in_msg=0, marker='', iterator=False, get_raw=False, **kw):
        """Reads data from a data collection (runs a data query)

        Common Parameters
        ----------
        backend (Required) : str
            Backend name - 'nosql'/'kv' | 'tsdb' | 'stream' | 'csv' (for tests)
        table : str
            Path to the collection to query; ignored when the path is set in
            the `query` parameter (currently supported for the 'tsdb' backend)
            and required otherwise
        query (Optional) : str
            ('tsdb' backend only) SQL query string
        columns (Optional) : []str
            ('nosql'/'kv' and 'tsdb' backends only) List of item attributes
            (columns) to return (default - all);
            cannot be used with `query`
        filter (Optional) : str
            ('nosql'/'kv' and 'tsdb' backends only) Query filter as a filter
            expression; cannot be used with `query`
        group_by (Optional) : str
            ('tsdb' backend only) A group-by query string; cannot be used with
            `query`
        limit (Optional): int
            Maximum number of rows to return
            [Not supported in this version]
        data_format (Optional) : str
            Data format
            [Not supported in this version]
        row_layout (Optional) : bool
            True to use a row layout; False (default) to use a column layout
            [Not supported in this version]
         max_rows_in_msg : int
            Maximum number of rows to read in each message (read chunk size)
        marker (Optional) : str
            Query marker; cannot be used with `query`
            [Not supported in this version]
        iterator (Optional) : bool
            True - return a DataFrames iterator;
            False (default except for get_raw=True) - return a single DataFrame
        get_raw (Optional) : bool
            True to return the data in raw format instead as pandas DataFrames
            [For internal use]
        **kw
            Variable-length list of additional keyword (named) arguments

        Return Value
        ----------
            - When `iterator` is False (default) - returns a single DataFrame.
            - When `iterator` is True - returns a DataFrames iterator.
        """
        if not backend:
            raise ReadError('no backend')
        if not (table or query):
            raise ReadError('missing data')

        return self._read(
            self._alias_backends(backend), table, query, columns, filter,
            group_by, limit, data_format, row_layout,
            max_rows_in_msg, marker, iterator, get_raw, **kw)

    def write(self, backend, table, dfs, expression='', condition='',
              labels=None, max_rows_in_msg=0, index_cols=None,
              save_mode='', partition_keys=None):
        """Writes data to a data collection

        Parameters
        ----------
        backend (Required) : str
            Backend name - 'nosql'/'kv' | 'tsdb' | 'stream' | 'csv' (for tests)
        table (Required) : str
            Path to the collection to write
        dfs (Required) : a single DataFrame (DF), a DF list, or a DF iterator
            One or more DataFrames containing the data to write.
            For the 'nosql'/'kv' backend - the DF must contain a single index
            column for the item's primary-key attribute (= the item name).
            For the 'tsdb' backend - the DF must contain one or more non-index
            columns for the sample metrics and a single time index column for
            the sample time, and can optionally contain additional string index
            columns for metric labels that apply to the current DataFrame row.
        expression (Optional) : str
            An update expression that determines the update logic for all items
            in the DataFrame
            [Not supported in this version]
        condition (Optional) : str
            A condition expression that defines a condition for performing the
            write operation
        labels (Optional) : dict (`{<label>: <value>[, <label>: <value>,...]}`)
            ('tsdb' backend only) A dictionary of sample labels of type
            string that apply to all the DataFrame rows
         max_rows_in_msg (Optional) : int
            Maximum number of rows to write in each message (write chunk size)
        index_cols (Optional) : []str
            List of column names to be used as the index columns for the write
            operation; by default, the DataFrame's index columns are used
        save_mode (Optional) : str
            ('nosql'/'kv' backend only) Save mode - 'createNewItemsOnly'
            (default) | 'overwriteTable' | 'updateItem', 'overwriteItem',
            'errorIfTableExists'
        partition_keys (Optional) : []str
            List of column names to partition the table by.

        Return Value
        ----------
            Write result
        """
        self._validate_request(backend, table, WriteError)
        if isinstance(dfs, pd.DataFrame):
            dfs = [dfs]

        canonical_backend_name = self._alias_backends(backend)

        if not save_mode and canonical_backend_name == 'kv':
            save_mode = 'createNewItemsOnly'

        if max_rows_in_msg:
            dfs = self._iter_chunks(dfs, max_rows_in_msg)

        request = self._encode_write(canonical_backend_name, table, expression, condition, save_mode, partition_keys)
        return self._write(request, dfs, labels, index_cols)

    def create(self, backend, table, schema=None, if_exists=FAIL, **kw):
        """Creates a new data collection
        Note: This method isn't applicable to the 'nosql'/'kv' backend, because
        NoSQL tables are created automatically on the first write.

        Parameters
        ----------
        backend (Required) : str
            Backend name - 'nosql'/'kv' | 'tsdb' | 'stream' | 'csv' (for tests)
        table (Required) : str
            Table to create
        schema (Optional) : Backend-specific data schema or None
            Table schema; used for testing purposes with the 'csv' backend
        if_exists (Optional) : int (frames_pb2 pb.ErrorOptions)
            Determines the behavior when the specified collection already
            exists - `FAIL` (default) to raise an error or `IGNORE` to ignore
        **kw (Optional; required for the 'tsdb' backend)
            Variable-length list of additional keyword (named) arguments

        Raises
        ------
        CreateError:
            On request error or backend error
        """
        self._validate_request(backend, table, CreateError)
        return self._create(self._alias_backends(backend), table, schema, if_exists, **kw)

    def delete(self, backend, table, filter='', start='', end='',
               if_missing=FAIL, metrics=None):
        """Deletes a table or stream or specific table items

        Parameters
        ----------
        backend (Required) : str
            Backend name - 'nosql'/'kv' | 'tsdb' | 'stream' | 'csv' (for tests)
        table (Required) : str
            Path to the collection to delete or from which to delete items
        filter (Optional) : str
            ('nosql'/'kv' backend only) A filter expression that identifies
            specific items to delete; for example, 'age<18'
        start (Optional) : str
             ('tsdb' backend only) Start (minimum) data sample time for the
             delete operation, as a string containing an RFC 3339 time, a Unix
             timestamp in milliseconds, a relative time (`'now'` or
             `'now-[0-9]+[mhd]'`, where `m` = minutes, `h` = hours, and `'d'` =
             days), or 0 for the earliest time; the default is an empty string
             for when `end` is also not set - to delete the entire table - and
             `0` when `end` is set
        end (Optional) : str
             ('tsdb' backend only) End (maximum) data sample time for the
             delete operation, as a string containing an RFC 3339 time, a Unix
             timestamp in milliseconds, a relative time (`'now'` or
             `'now-[0-9]+[mhd]'`, where `m` = minutes, `h` = hours, and `'d'` =
             days), or 0 for the earliest time; the default is an empty string
             for when `start` is also not set - to delete the entire table -
             and `0` when `start` is set
        if_missing (Optional) : int (frames_pb2 pb.ErrorOptions)
            Determines the behavior when the specified collection doesn't
            exist - `FAIL` (default) to raise an error or `IGNORE` to ignore
        metrics : []str
             (`tsdb` backend only) List of specific metric names to delete.

        Raises
        ------
        DeleteError
            On request error or backend error
        """
        self._validate_request(backend, table, DeleteError)
        return self._delete(backend, table, filter, start, end,
                            if_missing, metrics)

    def execute(self, backend, table, command='', args=None):
        """Executes a backend-specific command on a data collection

        Parameters
        ----------
        backend (Required) : str
            Backend name - 'nosql'/'kv' | 'tsdb' | 'stream' | 'csv' (for tests)
        table (Required) : str
            Path to the collection on which to execute the specified command
        command (Required) : str
            Command to execute (backend-specific) -
            - For the 'nosql'/'kv' backend -
              - 'inferSchema'/'infer' - infer the table schema
              - 'update' - update a table item
                [Not supported in this version]
            - For the 'stream' backend -
              - 'put' - add a record to a stream shard
        args : dict
            A dictionary of command-specific parameters (arguments)

        Raises
        ------
        ExecuteError
            On request error or backend error
        """
        self._validate_request(backend, table, ExecuteError)
        return self._execute(self._alias_backends(backend), table, command, args, expression=None)

    def history(self, backend='', container='', table='', user='', action='', min_start_time='', max_start_time='', min_duration=0, max_duration=0):
        """Returns usage history logs for frames service

        Parameters
        ----------
        backend (Optional) : str
            Filter by Backend name - 'nosql'/'kv' | 'tsdb' | 'stream' | 'csv' (for tests)
        container (Optional) : str
            Filter by associated v3io container
        table (Optional) : str
            Filter by associated table
        user (Optional) : str
            Filter by the user that ran the command
        action (Optional) : str
            Filter logs by action type - supported actions: 'create, delete, execute, ingest, query'
        min_start_time (Optional): string
            Start log time to query, as a string containing an RFC 3339 time, a Unix
             timestamp in milliseconds, a relative time (`'now'` or
             `'now-[0-9]+[mhd]'`, where `m` = minutes, `h` = hours, and `'d'` =
             days), or 0 for the earliest time; the default is 0, to get all logs.
         max_start_time (Optional): string
            End log time to query, as a string containing an RFC 3339 time, a Unix
             timestamp in milliseconds, a relative time (`'now'` or
             `'now-[0-9]+[mhd]'`, where `m` = minutes, `h` = hours, and `'d'` =
             days); the default is 'now', to get all logs.
         min_duration (Optional): int
            Minimum desired action duration time in milliseconds.
         max_duration (Optional): int
            Maximum desired action duration time in milliseconds.

        Raises
        ------
        HistoryError
            On request error
        """
        df = self._history(self._alias_backends(backend), container, table, user, action, min_start_time, max_start_time, min_duration, max_duration)

        if not df.empty:
            df.sort_values('StartTime', inplace=True, ignore_index=True)
        return df

    def _fix_address(self, address):
        return address

    def _encode_session(self, session):
        return session

    def _encode_write(self, backend, table, expression, condition, save_mode,
                      partition_keys):
        # TODO: InitialData?
        return fpb.InitialWriteRequest(
            session=self.session,
            backend=backend,
            table=table,
            expression=expression,
            condition=condition,
            partition_keys=partition_keys,
            save_mode=save_mode,
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

    def _alias_backends(self, backend):
        if backend == "nosql":
            return "kv"
        else:
            return backend
