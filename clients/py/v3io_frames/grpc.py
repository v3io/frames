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
from datetime import datetime
from functools import wraps

import pandas as pd

import grpc

from . import frames_pb2 as fpb
from . import frames_pb2_grpc as fgrpc
from .client import ClientBase
from .errors import (CreateError, DeleteError, ExecuteError, ReadError,
                     WriteError)
from .http import format_go_time
from .pbutils import df2msg, msg2df, pb_map
from .pdutils import concat_dfs, should_reorder_columns

try:
    import pyarrow as pa
    from pyarrow import plasma

    has_arrow = True
except ImportError:
    has_arrow = False


IGNORE, FAIL = fpb.IGNORE, fpb.FAIL
_scheme_prefix = 'grpc://'
GRPC_MESSAGE_SIZE = 128 * (1 << 20)  # 128MB
channel_options = [
    ('grpc.max_send_message_length', GRPC_MESSAGE_SIZE),
    ('grpc.max_receive_message_length', GRPC_MESSAGE_SIZE),
]


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


class Client(ClientBase):
    def _fix_address(self, address):
        if address.startswith(_scheme_prefix):
            return address[len(_scheme_prefix):]
        return address

    @grpc_raise(ReadError)
    def do_read(self, backend, table, query, columns, filter, group_by, limit,
                data_format, row_layout, max_in_message, marker, **kw):
        # TODO: Create channel once?
        with new_channel(self.address) as channel:
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
            do_reorder = should_reorder_columns(backend, query, columns)
            for frame in stub.Read(request):
                yield msg2df(frame, self.frame_factory,
                             columns, do_reorder=do_reorder)

    # We need to write "read" since once you have a yield in a function
    # (do_read) it'll always return a generator
    @grpc_raise(WriteError)
    def _read(self, backend, table, query, columns, filter, group_by, limit,
              data_format, row_layout, max_in_message, marker, iterator, **kw):
        dfs = self.do_read(
            backend, table, query, columns, filter, group_by, limit,
            data_format, row_layout, max_in_message, marker, **kw)
        if not iterator:
            return concat_dfs(dfs, self.frame_factory, self.concat)
        return dfs

    @grpc_raise(WriteError)
    def _write(self, request, dfs, labels, index_cols):
        with new_channel(self.address) as channel:
            stub = fgrpc.FramesStub(channel)
            stub.Write(write_stream(request, dfs, labels, index_cols))

    @grpc_raise(CreateError)
    def _create(self, backend, table, attrs, schema, if_exists):
        attrs = pb_map(attrs)
        with new_channel(self.address) as channel:
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
    def _delete(self, backend, table, filter, start, end, if_missing):
        start, end = time2str(start), time2str(end)
        with new_channel(self.address) as channel:
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
    def _execute(self, backend, table, command, args, expression):
        args = pb_map(args)
        with new_channel(self.address) as channel:
            stub = fgrpc.FramesStub(channel)
            request = fpb.ExecRequest(
                session=self.session,
                backend=backend,
                table=table,
                command=command,
                args=args,
                expression=expression,
            )
            resp = stub.Exec(request)
            if resp.frame:
                return msg2df(resp.frame, self.frame_factory)

    @grpc_raise(ReadError)
    def _read_shm(self, req):
        if not has_arrow:
            raise ReadError('pyarrow not found')

        with new_channel(self.address) as channel:
            stub = fgrpc.FramesStub(channel)
            resp = stub.ShmRead(req)

        if resp.error:
            raise ReadError(resp.error)

        client = plasma.connect(resp.db_path)
        oid = plasma.ObjectID(resp.object_id)
        buf, = client.get_buffers([oid])
        reader = pa.RecordBatchStreamReader(buf)
        record_batch = reader.read_next_batch()

        if self.frame_factory is pd.DataFrame:
            df = record_batch.to_pandas()
        else:  # cudf
            table = pa.Table.from_batches([record_batch])
            df = self.frame_factory.from_arrow(table)

        client.delete([oid])

        if req.index_columns:
            df = df.set_index(req.index_columns)

        return df

    @grpc_raise(WriteError)
    def _write_shm(self, req, db_path, df, obj_id=None):
        obj_id, index_cols = plasma_write_df(db_path, df, obj_id)

        wreq = fpb.ShmWriteRequest(
            db_path=db_path,
            object_id=obj_id.binary(),
            keep_object=False,  # TODO: In API?
            index_columns=index_cols,
            request=req,
        )
        with new_channel(self.address) as channel:
            stub = fgrpc.FramesStub(channel)
            return stub.ShmWrite(wreq)


def plasma_write_df(db_path, df, obj_id=None):
    if obj_id is None:
        obj_id = plasma.ObjectID.from_random()
    elif isinstance(obj_id, bytes):
        obj_id = plasma.ObjectID(obj_id)
    elif isinstance(obj_id, str):
        obj_id = plasma.ObjectID(obj_id.encode('ascii'))

    client = plasma.connect(db_path)
    record_batch = pa.RecordBatch.from_pandas(df)

    # Create the Plasma object from the PyArrow RecordBatch. Most of the work
    # here is done to determine the size of buffer to request from the object
    # store.
    mock_sink = pa.MockOutputStream()
    stream_writer = pa.RecordBatchStreamWriter(mock_sink, record_batch.schema)
    stream_writer.write_batch(record_batch)
    stream_writer.close()
    data_size = mock_sink.size()
    buf = client.create(obj_id, data_size)

    stream = pa.FixedSizeBufferWriter(buf)
    stream_writer = pa.RecordBatchStreamWriter(stream, record_batch.schema)
    stream_writer.write_batch(record_batch)
    stream_writer.close()
    client.seal(obj_id)

    meta = json.loads(record_batch.schema.metadata[b'pandas'])
    return obj_id, meta['index_columns']


def plasma_read_df(db_path, obj_id, index_columns=None):
    if isinstance(obj_id, bytes):
        obj_id = plasma.ObjectID(obj_id)
    elif isinstance(obj_id, str):
        obj_id = plasma.ObjectID(obj_id.encode('ascii'))

    client = plasma.connect(db_path)
    data, = client.get_buffers([obj_id])
    buf = pa.BufferReader(data)
    reader = pa.RecordBatchStreamReader(buf)
    record_batch = reader.read_next_batch()
    df = record_batch.to_pandas()
    if index_columns:
        if len(index_columns) == 1:
            df.index = df.pop(index_columns[0])
        else:
            raise ReadError('MultiIndex not supported')  # FIXME
    return df


def new_channel(address):
    return grpc.insecure_channel(address, options=channel_options)


def write_stream(request, dfs, labels, index_cols):
    yield fpb.WriteRequest(request=request)
    for df in dfs:
        yield fpb.WriteRequest(frame=df2msg(df, labels, index_cols))


def time2str(ts):
    if not isinstance(ts, (datetime, pd.Timestamp)):
        return ts

    return format_go_time(ts)
