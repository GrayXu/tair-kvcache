# -*- coding: utf-8 -*-
"""Hand-written protobuf definitions for engine_service_rpc.proto.

Compatible with protobuf>=3.20.3 (no runtime_version / _builder dependency).
Mirrors the codegen output for: KvCacheGroupDescriptorPB, KvCacheDescriptorPB,
KvCacheDescriptorRequestPB, StatusVersionPB, WorkerStatusPB,
KvCacheBlockListPB, KvCacheBlocksRequestPB,
and the KvCacheDescriptorErrorCode enum.
"""

from google.protobuf import descriptor_pb2 as _descriptor_pb2
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import message_factory as _message_factory
from google.protobuf import symbol_database as _symbol_database

_sym_db = _symbol_database.Default()


def _add_field(
    message: _descriptor_pb2.DescriptorProto,
    name: str,
    number: int,
    label: int,
    field_type: int,
    type_name: str = "",
    oneof_index: int | None = None,
) -> None:
    field = message.field.add()
    field.name = name
    field.number = number
    field.label = label
    field.type = field_type
    if type_name:
        field.type_name = type_name
    if oneof_index is not None:
        field.oneof_index = oneof_index


def _build_file_descriptor():
    file_proto = _descriptor_pb2.FileDescriptorProto()
    file_proto.name = "subscriber/proto/engine_service_rpc.proto"
    file_proto.syntax = "proto3"

    # enum KvCacheDescriptorErrorCode
    error_code = file_proto.enum_type.add()
    error_code.name = "KvCacheDescriptorErrorCode"
    error_code.value.add(name="KV_CACHE_DESCRIPTOR_OK", number=0)
    error_code.value.add(name="KV_CACHE_DESCRIPTOR_UNAVAILABLE", number=1)

    # message KvCacheGroupDescriptorPB
    group_meta = file_proto.message_type.add()
    group_meta.name = "KvCacheGroupDescriptorPB"
    _add_field(group_meta, "group_idx", 1, 1, 5)  # int32
    _add_field(group_meta, "kind", 2, 1, 9)  # string
    _add_field(group_meta, "block_size", 3, 1, 5)  # int32
    _add_field(group_meta, "sliding_window", 4, 1, 5)  # int32

    # message KvCacheDescriptorPB
    descriptor = file_proto.message_type.add()
    descriptor.name = "KvCacheDescriptorPB"
    _add_field(descriptor, "items", 1, 3, 11, ".KvCacheGroupDescriptorPB")
    _add_field(descriptor, "settings", 2, 1, 11, ".EngineCacheSettingsPB")
    _add_field(
        descriptor, "err_code", 3, 1,
        _descriptor_pb2.FieldDescriptorProto.TYPE_ENUM,
        ".KvCacheDescriptorErrorCode",
    )
    _add_field(descriptor, "err_msg", 4, 1, 9)  # string

    # message EngineCacheSettingsPB
    engine_settings = file_proto.message_type.add()
    engine_settings.name = "EngineCacheSettingsPB"
    _add_field(engine_settings, "use_eagle_pop", 1, 1, 8)  # bool
    _add_field(engine_settings, "mamba_cache_mode", 2, 1, 9)  # string

    # message KvCacheDescriptorRequestPB (empty)
    file_proto.message_type.add().name = "KvCacheDescriptorRequestPB"

    # message StatusVersionPB
    status_version = file_proto.message_type.add()
    status_version.name = "StatusVersionPB"
    _add_field(status_version, "latest_cache_version", 1, 1, 3)  # int64
    _add_field(status_version, "latest_finished_version", 2, 1, 3)  # int64

    # message WorkerStatusPB
    worker_status = file_proto.message_type.add()
    worker_status.name = "WorkerStatusPB"
    _add_field(worker_status, "status_version", 12, 1, 3)  # int64
    _add_field(worker_status, "alive", 13, 1, 8)  # bool
    _add_field(worker_status, "latest_finished_version", 15, 1, 3)  # int64

    # message KvCacheBlockListPB
    block_list = file_proto.message_type.add()
    block_list.name = "KvCacheBlockListPB"
    _add_field(block_list, "raw_snapshot", 1, 1, 12)  # bytes
    _add_field(block_list, "snapshot_version", 2, 1, 3)  # int64
    _add_field(block_list, "block_size", 3, 1, 5)  # int32

    # message KvCacheBlocksRequestPB (empty)
    file_proto.message_type.add().name = "KvCacheBlocksRequestPB"

    # service RpcService
    service = file_proto.service.add()
    service.name = "RpcService"
    method = service.method.add()
    method.name = "GetKvCacheDescriptor"
    method.input_type = ".KvCacheDescriptorRequestPB"
    method.output_type = ".KvCacheDescriptorPB"
    method2 = service.method.add()
    method2.name = "GetWorkerStatus"
    method2.input_type = ".StatusVersionPB"
    method2.output_type = ".WorkerStatusPB"
    method3 = service.method.add()
    method3.name = "GetAllKvCacheBlocks"
    method3.input_type = ".KvCacheBlocksRequestPB"
    method3.output_type = ".KvCacheBlockListPB"

    pool = _descriptor_pool.Default()
    try:
        return pool.AddSerializedFile(file_proto.SerializeToString())
    except Exception:
        return pool.FindFileByName(file_proto.name)


DESCRIPTOR = _build_file_descriptor()


def _message_class(name: str):
    descriptor = DESCRIPTOR.message_types_by_name[name]
    get_message_class = getattr(_message_factory, "GetMessageClass", None)
    if get_message_class is not None:
        return get_message_class(descriptor)
    factory = _message_factory.MessageFactory()
    return factory.GetPrototype(descriptor)


KvCacheGroupDescriptorPB = _message_class("KvCacheGroupDescriptorPB")
KvCacheDescriptorPB = _message_class("KvCacheDescriptorPB")
EngineCacheSettingsPB = _message_class("EngineCacheSettingsPB")
KvCacheDescriptorRequestPB = _message_class("KvCacheDescriptorRequestPB")
StatusVersionPB = _message_class("StatusVersionPB")
WorkerStatusPB = _message_class("WorkerStatusPB")
KvCacheBlockListPB = _message_class("KvCacheBlockListPB")
KvCacheBlocksRequestPB = _message_class("KvCacheBlocksRequestPB")

KvCacheDescriptorErrorCode = _enum_type_wrapper.EnumTypeWrapper(
    DESCRIPTOR.enum_types_by_name["KvCacheDescriptorErrorCode"]
)
KV_CACHE_DESCRIPTOR_OK = KvCacheDescriptorErrorCode.KV_CACHE_DESCRIPTOR_OK
KV_CACHE_DESCRIPTOR_UNAVAILABLE = (
    KvCacheDescriptorErrorCode.KV_CACHE_DESCRIPTOR_UNAVAILABLE
)

_sym_db.RegisterMessage(KvCacheGroupDescriptorPB)
_sym_db.RegisterMessage(KvCacheDescriptorPB)
_sym_db.RegisterMessage(EngineCacheSettingsPB)
_sym_db.RegisterMessage(KvCacheDescriptorRequestPB)
_sym_db.RegisterMessage(StatusVersionPB)
_sym_db.RegisterMessage(WorkerStatusPB)
_sym_db.RegisterMessage(KvCacheBlockListPB)
_sym_db.RegisterMessage(KvCacheBlocksRequestPB)
