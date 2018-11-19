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

import warnings
from datetime import datetime
from functools import wraps

import grpc
import numpy as np
import pandas as pd
import pytz

from . import frames_pb2 as fpb  # noqa
from . import frames_pb2_grpc as fgrpc  # noqa
from .errors import (
    CreateError, DeleteError, MessageError, ReadError, WriteError,
    ExecuteError)
from .http import format_go_time
from .pbutils import pb2py, pb_map

IGNORE, FAIL = fpb.IGNORE, fpb.FAIL
_ts = pd.Series(pd.Timestamp(0))
_time_dt = _ts.dtype
_time_tz_dt = _ts.dt.tz_localize(pytz.UTC).dtype


def grpc_raise(err_cls):
    """Re-raise a different type of exception from grpc.RpcError"""
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kw):
            try:
                return fn(*args, **kw)
            except grpc.RpcError as gerr:
                err = err_cls('error in {}: {}'.format(fn.__name__, gerr))
                err.cause = gerr
                raise err
        return wrapper
    return decorator


class Client:
    def __init__(self, address, session):
        """Create new client

        Parameters
        ----------
        address : str
            framesd server address
        session : Session
            Session object
        """
        self.address = address
        self.session = session

    @grpc_raise(ReadError)
    def read(self, backend='', table='', query='', columns=None, filter='',
             group_by='', limit=0, data_format='', row_layout=False,
             max_in_message=0, marker='', **kw):
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
        **kw
            Extra parameter for specific backends

        Returns:
            A pandas DataFrame iterator. Each DataFrame will have "labels"
            attribute.
        """
        # TODO: Create channel once?
        with grpc.insecure_channel(self.address) as channel:
            stub = fgrpc.FramesStub(channel)
            request = fpb.ReadRequest(
                session=self.session,
                backend=backend,
                query=query,
                table=table,
                columns=columns,
                filter=filter,
                group_by=group_by,
                message_limit=max_in_message,
                limit=limit,
                row_layout=row_layout,
                marker=marker,
                **kw
            )
            for frame in stub.Read(request):
                yield frame2df(frame)

    @grpc_raise(WriteError)
    def write(self, backend, table, dfs, expression='', labels=None):
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

        Returns:
            Write result
        """

        if isinstance(dfs, pd.DataFrame):
            dfs = [dfs]

        with grpc.insecure_channel(self.address) as channel:
            stub = fgrpc.FramesStub(channel)
            request = fpb.InitialWriteRequest(
                session=self.session,
                backend=backend,
                table=table,
                expression=expression,
            )
            stub.Write(write_stream(request, dfs))

    @grpc_raise(CreateError)
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
        attrs = pb_map(attrs)
        with grpc.insecure_channel(self.address) as channel:
            stub = fgrpc.FramesStub(channel)
            request = fpb.CreateRequest(
                session=self.session,
                backend=backend,
                table=table,
                attribute_map=attrs,
                schema=schema,
                if_exists=if_exists,
            )
            stub.Create(request)

    @grpc_raise(DeleteError)
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
        start, end = time2str(start), time2str(end)
        with grpc.insecure_channel(self.address) as channel:
            stub = fgrpc.FramesStub(channel)
            request = fpb.DeleteRequest(
                session=self.session,
                backend=backend,
                table=table,
                filter=filter,
                start=start,
                end=end,
                if_missing=if_missing,
            )
            stub.Delete(request)

    @grpc_raise(ExecuteError)
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
        args = pb_map(args)
        with grpc.insecure_channel(self.address) as channel:
            stub = fgrpc.FramesStub(channel)
            request = fpb.ExecRequest(
                session=self.session,
                backend=backend,
                table=table,
                command=command,
                args=args,
                expression=expression,
            )
            stub.Exec(request)


def frame2df(frame):
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
    if col.dtype == fpb.BOOLEAN:
        data = pd.Series(col.bools, dtype=np.bool)
    elif col.dtype == fpb.FLOAT:
        data = pd.Series(col.floats, dtype=np.float64)
    elif col.dtype == fpb.INTEGER:
        data = pd.Series(col.ints, dtype=np.int64)
    elif col.dtype == fpb.STRING:
        data = pd.Series(col.strings)
    elif col.dtype == fpb.TIME:
        data = pd.Series(col.times).astype('datetime64[ns]')
    else:
        raise MessageError('unknown dtype - {}'.format(col.dtype))

    if col.kind == col.LABEL:
        data = data.reindex(pd.RangeIndex(col.size), method='pad')

    return data


def write_stream(request, dfs):
    yield fpb.WriteRequest(request=request)
    for df in dfs:
        yield fpb.WriteRequest(frame=df2msg(df))


def df2msg(df):
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
    )


def series2col(s):
    col = fpb.Column(
        name=s.name or '',
        kind=fpb.Column.SLICE,
    )

    if is_int_dtype(s.dtype):
        col.ints.extend(s)
        col.dtype = fpb.INTEGER
    elif is_float_dtype(s.dtype):
        col.floats.extend(s)
        col.dtype = fpb.FLOAT
    elif s.dtype == np.object:  # Pandas dtype for str is object
        col.strings.extend(s)
        col.dtype = fpb.STRING
    elif s.dtype == np.bool:
        col.bools.extend(s)
        col.dtype = fpb.BOOLEAN
    elif is_time_dtype(s.dtype):
        if s.dt.tz:
            s = s.dt.tz_localize(pytz.UTC)
        col.times.extend(s.astype(np.int64))
        col.dtype = fpb.TIME
    else:
        raise WriteError(
            '{} - unsupported type - {}'.format(s.name, s.dtype))

    return col


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


def time2str(ts):
    if not isinstance(ts, (datetime, pd.Timestamp)):
        return ts

    return format_go_time(ts)


def is_exists_error(err):
    return 'A TSDB table already exists' in str(err)
