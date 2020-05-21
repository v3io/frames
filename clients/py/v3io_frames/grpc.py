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
import abc
import math
import time
import typing
from datetime import datetime
from functools import wraps

import pandas as pd

import grpc
from . import frames_pb2 as fpb  # noqa
from . import frames_pb2_grpc as fgrpc  # noqa
from .client import ClientBase, RawFrame
from .errors import (CreateError, DeleteError, ExecuteError, ReadError,
                     WriteError, HistoryError)
from .http import format_go_time
from .pbutils import msg2df, pb_map, df2msg
from .pdutils import concat_dfs, should_reorder_columns

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
    def __init__(self, *args, **kwargs):

        # before parent initializer calls _fix_address()
        self._scheme_prefix = 'grpc://'

        super(Client, self).__init__(*args, **kwargs)

        # default channel options
        self._channel_options = [
            ('grpc.max_send_message_length', GRPC_MESSAGE_SIZE),
            ('grpc.max_receive_message_length', GRPC_MESSAGE_SIZE),
        ]

        self._channel = None

        # create interceptors
        self._interceptors = (
            RetryOnRpcErrorClientInterceptor(
                max_attempts=4,
                sleeping_policy=ExponentialBackoff(init_backoff_ms=100,
                                                   max_backoff_ms=1600,
                                                   multiplier=2),
                status_for_retry=(grpc.StatusCode.UNAVAILABLE,),
            ),
        )

        # create the session object, persist it between requests
        self._open_new_channel()

    def __del__(self):
        self._channel.close()

    def _fix_address(self, address):
        if address.startswith(self._scheme_prefix):
            return address[len(self._scheme_prefix):]
        return address

    def _open_new_channel(self):
        self._channel = grpc.intercept_channel(
            grpc.insecure_channel(self.address,
                                  options=self._channel_options),
            *self._interceptors)

    @grpc_raise(ReadError)
    def do_read(self, backend, table, query, columns, filter, group_by, limit,
                data_format, row_layout, max_in_message, marker, get_raw, **kw):
        stub = fgrpc.FramesStub(self._channel)
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
            if get_raw:
                yield RawFrame(frame)
            else:
                yield msg2df(frame, self.frame_factory,
                             columns, do_reorder=do_reorder)

    # We need to write "read" since once you have a yield in a function
    # (do_read) it'll always return a generator
    @grpc_raise(WriteError)
    def _read(self, backend, table, query, columns, filter, group_by, limit,
              data_format, row_layout, max_in_message, marker, iterator, get_raw, **kw):
        dfs = self.do_read(
            backend, table, query, columns, filter, group_by, limit,
            data_format, row_layout, max_in_message, marker, get_raw, **kw)
        if not iterator and not get_raw:
            multi_index = kw.get('multi_index', False)
            return concat_dfs(dfs, backend, self.frame_factory, self.concat, multi_index)
        return dfs

    @grpc_raise(WriteError)
    def _write(self, request, dfs, labels, index_cols):
        stub = fgrpc.FramesStub(self._channel)
        frames = []
        for df in dfs:
            sub_dfs = self._split_df(df)
            for sub_df in sub_dfs:
                frames.append(df2msg(sub_df, labels, index_cols))
        stub.Write(write_stream(request, frames))

    def _split_df(self, df):
        memory_usage = df.memory_usage(deep=True).sum()
        # protobuf will take up more space - take a large margin
        memory_usage = memory_usage * 2
        num_dfs = 1 + int(memory_usage / 128 / 1024 / 1024)
        if num_dfs == 1:
            return [df]
        dfs = []
        sub_df_length = math.ceil(len(df) / num_dfs)
        for i in range(num_dfs):
            sub_df = df[i * sub_df_length:(i + 1) * sub_df_length]
            dfs.append(sub_df)
        return dfs

    @grpc_raise(CreateError)
    def _create(self, backend, table, schema, if_exists, **kw):
        stub = fgrpc.FramesStub(self._channel)
        request = fpb.CreateRequest(
            session=self.session,
            backend=backend,
            table=table,
            schema=schema,
            if_exists=if_exists,
            **kw
        )
        stub.Create(request)

    @grpc_raise(DeleteError)
    def _delete(self, backend, table, filter, start, end, if_missing, metrics):
        start, end = time2str(start), time2str(end)
        stub = fgrpc.FramesStub(self._channel)
        request = fpb.DeleteRequest(
            session=self.session,
            backend=backend,
            table=table,
            filter=filter,
            start=start,
            end=end,
            if_missing=if_missing,
            metrics=metrics,
        )
        stub.Delete(request)

    @grpc_raise(ExecuteError)
    def _execute(self, backend, table, command, args, expression):
        args = pb_map(args)
        stub = fgrpc.FramesStub(self._channel)
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

    @grpc_raise(HistoryError)
    def _history(self, backend, container, table, user, action, min_start_time, max_start_time, min_duration, max_duration):
        dfs = self.do_history(backend, container, table, user, action, min_start_time, max_start_time, min_duration, max_duration)
        return concat_dfs(dfs, "")

    @grpc_raise(ReadError)
    def do_history(self, backend, container, table, user, action, min_start_time, max_start_time, min_duration, max_duration):
        stub = fgrpc.FramesStub(self._channel)
        request = fpb.HistoryRequest(
            session=self.session,
            backend=backend,
            table=table,
            user=user,
            action=action,
            min_start_time=min_start_time,
            max_start_time=max_start_time,
            container=container,
            min_duration=min_duration,
            max_duration=max_duration
        )
        for frame in stub.History(request):
            yield msg2df(frame, self.frame_factory)


def write_stream(request, frames):
    yield fpb.WriteRequest(request=request)
    for frame in frames:
        yield fpb.WriteRequest(frame=frame)


def time2str(ts):
    if not isinstance(ts, (datetime, pd.Timestamp)):
        return ts

    return format_go_time(ts)


class SleepingPolicy(abc.ABC):
    @abc.abstractmethod
    def sleep(self, attempt):
        """
        How long to sleep in milliseconds.
        :param attempt: the number of attempt (starting from zero)
        """
        assert attempt >= 0


class ExponentialBackoff(SleepingPolicy):
    def __init__(self,
                 init_backoff_ms: int,
                 max_backoff_ms: int,
                 multiplier: int = 2):
        """
        inputs in ms
        """
        self._init_backoff = init_backoff_ms
        self._max_backoff = max_backoff_ms
        self._multiplier = multiplier

    def sleep(self, attempt: int):
        sleep_time_ms = min(
            self._init_backoff * self._multiplier ** attempt,
            self._max_backoff
        )
        time.sleep(sleep_time_ms / 1000)


class RetryOnRpcErrorClientInterceptor(
    grpc.UnaryUnaryClientInterceptor,
    grpc.StreamUnaryClientInterceptor
):
    def __init__(self,
                 max_attempts: int,
                 sleeping_policy: SleepingPolicy,
                 status_for_retry: typing.Tuple[grpc.StatusCode] = None):
        self._max_attempts = max_attempts
        self._sleeping_policy = sleeping_policy
        self._retry_statuses = status_for_retry

    def _intercept_call(self, continuation, client_call_details,
                        request_or_iterator):

        for attempt in range(self._max_attempts):
            response = continuation(client_call_details,
                                    request_or_iterator)

            if isinstance(response, grpc.RpcError):

                # Return if it was last attempt
                if attempt == (self._max_attempts - 1):
                    return response

                # If status code is not in retryable status codes
                if self._retry_statuses \
                        and hasattr(response, 'code') \
                        and response.code() \
                        not in self._retry_statuses:
                    return response

                self._sleeping_policy.sleep(attempt)
            else:
                return response

    def intercept_unary_unary(self, continuation, client_call_details,
                              request):
        return self._intercept_call(continuation, client_call_details,
                                    request)

    def intercept_stream_unary(
            self, continuation, client_call_details, request_iterator
    ):
        return self._intercept_call(continuation, client_call_details,
                                    request_iterator)
