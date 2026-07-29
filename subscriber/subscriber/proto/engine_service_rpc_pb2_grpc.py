"""Hand-maintained gRPC stubs.

DO NOT regenerate via grpc_tools.protoc; see AGENTS.md for the wire-schema
maintenance rules (protobuf 3.20.3 compatibility must be preserved).
"""
import grpc
import warnings

from subscriber.proto import engine_service_rpc_pb2 as subscriber_dot_proto_dot_engine__service__rpc__pb2

GRPC_GENERATED_VERSION = '1.68.1'
GRPC_VERSION = grpc.__version__
_version_not_supported = False

try:
    from grpc._utilities import first_version_is_lower
    _version_not_supported = first_version_is_lower(GRPC_VERSION, GRPC_GENERATED_VERSION)
except ImportError:
    _version_not_supported = True

if _version_not_supported:
    raise RuntimeError(
        f'The grpc package installed is at version {GRPC_VERSION},'
        + f' but the generated code in subscriber/proto/engine_service_rpc_pb2_grpc.py depends on'
        + f' grpcio>={GRPC_GENERATED_VERSION}.'
        + f' Please upgrade your grpc module to grpcio>={GRPC_GENERATED_VERSION}'
        + f' or downgrade your generated code using grpcio-tools<={GRPC_VERSION}.'
    )


class RpcServiceStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.GetKvCacheDescriptor = channel.unary_unary(
                '/RpcService/GetKvCacheDescriptor',
                request_serializer=subscriber_dot_proto_dot_engine__service__rpc__pb2.KvCacheDescriptorRequestPB.SerializeToString,
                response_deserializer=subscriber_dot_proto_dot_engine__service__rpc__pb2.KvCacheDescriptorPB.FromString,
                _registered_method=True)
        self.GetWorkerStatus = channel.unary_unary(
                '/RpcService/GetWorkerStatus',
                request_serializer=subscriber_dot_proto_dot_engine__service__rpc__pb2.StatusVersionPB.SerializeToString,
                response_deserializer=subscriber_dot_proto_dot_engine__service__rpc__pb2.WorkerStatusPB.FromString,
                _registered_method=True)
        self.GetAllKvCacheBlocks = channel.unary_unary(
                '/RpcService/GetAllKvCacheBlocks',
                request_serializer=subscriber_dot_proto_dot_engine__service__rpc__pb2.KvCacheBlocksRequestPB.SerializeToString,
                response_deserializer=subscriber_dot_proto_dot_engine__service__rpc__pb2.KvCacheBlockListPB.FromString,
                _registered_method=True)


class RpcServiceServicer(object):
    """Missing associated documentation comment in .proto file."""

    def GetKvCacheDescriptor(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetWorkerStatus(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetAllKvCacheBlocks(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_RpcServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'GetKvCacheDescriptor': grpc.unary_unary_rpc_method_handler(
                    servicer.GetKvCacheDescriptor,
                    request_deserializer=subscriber_dot_proto_dot_engine__service__rpc__pb2.KvCacheDescriptorRequestPB.FromString,
                    response_serializer=subscriber_dot_proto_dot_engine__service__rpc__pb2.KvCacheDescriptorPB.SerializeToString,
            ),
            'GetWorkerStatus': grpc.unary_unary_rpc_method_handler(
                    servicer.GetWorkerStatus,
                    request_deserializer=subscriber_dot_proto_dot_engine__service__rpc__pb2.StatusVersionPB.FromString,
                    response_serializer=subscriber_dot_proto_dot_engine__service__rpc__pb2.WorkerStatusPB.SerializeToString,
            ),
            'GetAllKvCacheBlocks': grpc.unary_unary_rpc_method_handler(
                    servicer.GetAllKvCacheBlocks,
                    request_deserializer=subscriber_dot_proto_dot_engine__service__rpc__pb2.KvCacheBlocksRequestPB.FromString,
                    response_serializer=subscriber_dot_proto_dot_engine__service__rpc__pb2.KvCacheBlockListPB.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'RpcService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))
    server.add_registered_method_handlers('RpcService', rpc_method_handlers)


 # This class is part of an EXPERIMENTAL API.
class RpcService(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def GetKvCacheDescriptor(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/RpcService/GetKvCacheDescriptor',
            subscriber_dot_proto_dot_engine__service__rpc__pb2.KvCacheDescriptorRequestPB.SerializeToString,
            subscriber_dot_proto_dot_engine__service__rpc__pb2.KvCacheDescriptorPB.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def GetWorkerStatus(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/RpcService/GetWorkerStatus',
            subscriber_dot_proto_dot_engine__service__rpc__pb2.StatusVersionPB.SerializeToString,
            subscriber_dot_proto_dot_engine__service__rpc__pb2.WorkerStatusPB.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def GetAllKvCacheBlocks(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/RpcService/GetAllKvCacheBlocks',
            subscriber_dot_proto_dot_engine__service__rpc__pb2.KvCacheBlocksRequestPB.SerializeToString,
            subscriber_dot_proto_dot_engine__service__rpc__pb2.KvCacheBlockListPB.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)
