# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
import grpc

from . import frames_pb2 as frames__pb2


class FramesStub(object):
  # missing associated documentation comment in .proto file
  pass

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


class FramesServicer(object):
  # missing associated documentation comment in .proto file
  pass

  def Read(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def Write(self, request_iterator, context):
    # missing associated documentation comment in .proto file
    pass
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
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'pb.Frames', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))