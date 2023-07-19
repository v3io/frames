# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from . import frames_pb2 as frames__pb2


class FramesStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Read = channel.unary_stream(
                '/pb.Frames/Read',
                request_serializer=frames__pb2.ReadRequest.SerializeToString,
                response_deserializer=frames__pb2.Frame.FromString,
                )
        self.Write = channel.stream_unary(
                '/pb.Frames/Write',
                request_serializer=frames__pb2.WriteRequest.SerializeToString,
                response_deserializer=frames__pb2.WriteRespose.FromString,
                )
        self.Create = channel.unary_unary(
                '/pb.Frames/Create',
                request_serializer=frames__pb2.CreateRequest.SerializeToString,
                response_deserializer=frames__pb2.CreateResponse.FromString,
                )
        self.Delete = channel.unary_unary(
                '/pb.Frames/Delete',
                request_serializer=frames__pb2.DeleteRequest.SerializeToString,
                response_deserializer=frames__pb2.DeleteResponse.FromString,
                )
        self.Exec = channel.unary_unary(
                '/pb.Frames/Exec',
                request_serializer=frames__pb2.ExecRequest.SerializeToString,
                response_deserializer=frames__pb2.ExecResponse.FromString,
                )
        self.History = channel.unary_stream(
                '/pb.Frames/History',
                request_serializer=frames__pb2.HistoryRequest.SerializeToString,
                response_deserializer=frames__pb2.Frame.FromString,
                )
        self.Version = channel.unary_unary(
                '/pb.Frames/Version',
                request_serializer=frames__pb2.VersionRequest.SerializeToString,
                response_deserializer=frames__pb2.VersionResponse.FromString,
                )


class FramesServicer(object):
    """Missing associated documentation comment in .proto file."""

    def Read(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Write(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Create(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Delete(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Exec(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def History(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Version(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_FramesServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'Read': grpc.unary_stream_rpc_method_handler(
                    servicer.Read,
                    request_deserializer=frames__pb2.ReadRequest.FromString,
                    response_serializer=frames__pb2.Frame.SerializeToString,
            ),
            'Write': grpc.stream_unary_rpc_method_handler(
                    servicer.Write,
                    request_deserializer=frames__pb2.WriteRequest.FromString,
                    response_serializer=frames__pb2.WriteRespose.SerializeToString,
            ),
            'Create': grpc.unary_unary_rpc_method_handler(
                    servicer.Create,
                    request_deserializer=frames__pb2.CreateRequest.FromString,
                    response_serializer=frames__pb2.CreateResponse.SerializeToString,
            ),
            'Delete': grpc.unary_unary_rpc_method_handler(
                    servicer.Delete,
                    request_deserializer=frames__pb2.DeleteRequest.FromString,
                    response_serializer=frames__pb2.DeleteResponse.SerializeToString,
            ),
            'Exec': grpc.unary_unary_rpc_method_handler(
                    servicer.Exec,
                    request_deserializer=frames__pb2.ExecRequest.FromString,
                    response_serializer=frames__pb2.ExecResponse.SerializeToString,
            ),
            'History': grpc.unary_stream_rpc_method_handler(
                    servicer.History,
                    request_deserializer=frames__pb2.HistoryRequest.FromString,
                    response_serializer=frames__pb2.Frame.SerializeToString,
            ),
            'Version': grpc.unary_unary_rpc_method_handler(
                    servicer.Version,
                    request_deserializer=frames__pb2.VersionRequest.FromString,
                    response_serializer=frames__pb2.VersionResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'pb.Frames', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class Frames(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def Read(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/pb.Frames/Read',
            frames__pb2.ReadRequest.SerializeToString,
            frames__pb2.Frame.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Write(request_iterator,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.stream_unary(request_iterator, target, '/pb.Frames/Write',
            frames__pb2.WriteRequest.SerializeToString,
            frames__pb2.WriteRespose.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Create(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/pb.Frames/Create',
            frames__pb2.CreateRequest.SerializeToString,
            frames__pb2.CreateResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Delete(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/pb.Frames/Delete',
            frames__pb2.DeleteRequest.SerializeToString,
            frames__pb2.DeleteResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Exec(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/pb.Frames/Exec',
            frames__pb2.ExecRequest.SerializeToString,
            frames__pb2.ExecResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def History(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/pb.Frames/History',
            frames__pb2.HistoryRequest.SerializeToString,
            frames__pb2.Frame.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Version(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/pb.Frames/Version',
            frames__pb2.VersionRequest.SerializeToString,
            frames__pb2.VersionResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
