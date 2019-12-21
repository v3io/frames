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
        """Get column by column name

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
        """Get column's data by column name

       Parameters
       ----------
       column_name : str
           Column name to get

       Return Value
       ----------
           List of values which represents the requested column's data
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

        raise ReadError('no column named {}'.format(column_name))

    def labels(self):
        """Get frame's labels

       Return Value
       ----------
           labels of the current frame. Only applicable to TSDB and Stream backends
       """
        return self.raw_frame.labels

    def indices(self):
        """Get frame's indices


       Return Value
       ----------
           List of column objects representing the indices of the frame
       """
        return self.raw_frame.indices

    def columns(self):
        """Get all frame's columns

       Return Value
       ----------
           List of column objects representing the columns of the frame
       """
        return self.raw_frame.columns

    def __len__(self):
        """Get the length of the frame

       Return Value
       ----------
           Number of rows in the frame
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
        """Indicates whether a specific cell is null or not.

       Parameters
       ----------
       index : str
           Row index of the desired cell
       column_name : str
           Column name of the desired cell

       Return Value
       ----------
           True - the current cell is null
           False - the current cell has a value
       """
        if len(self.raw_frame.null_values) == 0:
            return False

        for null_column in self.raw_frame.null_values[index].nullColumns:
            if null_column.key == column_name:
                return True
        return False


class ClientBase:
    def __init__(self, address, session, frame_factory=pd.DataFrame,
                 concat=pd.concat):
        """Creates a new Frames base client object (for internal use)

        Parameters
        ----------
        address (Required) : str
            Address of the Frames service (framesd)
        session (Optional) : object
            Session object
        frame_factory (Optional) : class
            DataFrame factory; currently, pandas (default) or cuDF
        concat (Optional) : function
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
             max_in_message=0, marker='', iterator=False, get_raw=False, **kw):
        """Reads data from a table or stream (run a data query)

        Common Parameters
        ----------
        backend : str
            Backend name ('kv', 'tsdb', 'stream')
        table : str
            Table or stream to query; ignored when the table path is set in
            the `query` parameter (for the 'tsdb' backend)
        query : str
            ('tsdb' backend only) SQL query string
        columns : []str
            List of item attributes (columns) to return (default - all);
            cannot be used with `query`
        filter : str
            Query filter as a platform filter expression; cannot be used with
            `query`
        group_by : str
            ('tsdb' backend only) A group-by query string; cannot be used with
            `query`
        limit: int
            Maximum number of rows to return
            [Not supported in this version]
        data_format : str
            Data format
            [Not supported in this version]
        row_layout : bool
            True to use a row layout; False (default) to use a column layout
            [Not supported in this version]
        max_in_message : int
            Maximum number of rows to read in each message (read chunk size)
        marker : str
            Query marker; cannot be used with `query`
            [Not supported in this version]
        iterator : bool
            True - return a DataFrames iterator;
            False (default) - return a single DataFrame
        get_raw : bool
            False (default) - return Pandas Dataframe
            True - return a raw data object rather then Pandas data frame.
            This will boost performance at the expense of Pandas convenience.
            Note: this mode will always return an iterator of frames.
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
        # TODO: More validation

        if max_in_message > 0:
            iterator = True

        return self._read(
            backend, table, query, columns, filter,
            group_by, limit, data_format, row_layout,
            max_in_message, marker, iterator, get_raw, **kw)

    def write(self, backend, table, dfs, expression='', condition='',
              labels=None, max_in_message=0, index_cols=None,
              save_mode='errorIfTableExists', partition_keys=None):
        """Writes data to a table or stream

        Parameters
        ----------
        backend : str
            Backend name
        table : str
            Table to write to
        dfs : a single DataFrame, a DataFrames list, or a DataFrames iterator
            One or more DataFrames containing the data to write.
            For the 'kv' backend - the DF must contain a single index column
            for the item's primary-key attribute (= the item name).
            For the 'tsdb' backend - the DF must contain one or more non-index
            columns for the sample metrics and a single time index column for
            the sample time, and can optionally contain additional string index
            columns for metric labels that apply to the current DataFrame row.
        expression : str
            A platform update expression that determines the update logic for
            all items in the DataFrame
            [Not supported in this version]
        condition : str
            A platform condition expression that defines a condition for
            performing the write operation
        labels : dict (`{<label>: <value>[, <label>: <value>, ...]}`)
            ('tsdb' backend only) A dictionary of sample labels of type
            string that apply to all the DataFrame rows
        max_in_message : int
            Maximum number of rows to write in each message (write chunk size)
        index_cols : []str
            List of column names to be used as the index columns for the write
            operation; by default, the DataFrame's index columns are used
        save_mode : str
            Save mode.
            Optional values: errorIfTableExists (default), overwriteTable,
             updateItem, overwriteItem, createNewItemsOnly
        partition_keys : []str
            Partition keys
            [Not supported in this version]

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
            backend, table, expression, condition, save_mode, partition_keys)
        return self._write(request, dfs, labels, index_cols)

    def create(self, backend, table, attrs=None, schema=None, if_exists=FAIL):
        """Creates a new TSDB table or stream

        Parameters
        ----------
        backend (Required) : str
            Backend name
        table (Required) : str
            Table to create
        attrs (Required for the 'tsdb' backend; optional otherwise : dict
            A dictionary of backend-specific parameters (arguments)
        schema (Optional) : Schema or None
            Table schema; used for testing purposes with the 'csv' backend
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
            Table or stream to delete or from which to delete specific items
        filter : str
            ('kv' backend only) A filter expression that identifies specific
            items to delete; for example, 'age<18'
        start : str
             ('tsdb' backend only) Start (minimum) data sample time for the
             delete operation, as a string containing an RFC 3339 time, a Unix
             timestamp in milliseconds, a relative time (`'now'` or
             `'now-[0-9]+[mhd]'`, where `m` = minutes, `h` = hours, and `'d'` =
             days), or 0 for the earliest time; the default is an empty string
             for when `end` is also not set - to delete the entire table - and
             `0` when `end` is set
        end : str
             ('tsdb' backend only) End (maximum) data sample time for the
             delete operation, as a string containing an RFC 3339 time, a Unix
             timestamp in milliseconds, a relative time (`'now'` or
             `'now-[0-9]+[mhd]'`, where `m` = minutes, `h` = hours, and `'d'` =
             days), or 0 for the earliest time; the default is an empty string
             for when `start` is also not set - to delete the entire table -
             and `0` when `start` is set
        if_missing : int (frames_pb2 pb.ErrorOptions)
            Determines the behavior when the specified table or stream doesn't
            exist - `FAIL` (default) to raise an error or `IGNORE` to ignore

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
