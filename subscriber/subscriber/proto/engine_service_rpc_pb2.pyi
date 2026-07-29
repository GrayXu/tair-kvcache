from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class KvCacheDescriptorErrorCode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    KV_CACHE_DESCRIPTOR_OK: _ClassVar[KvCacheDescriptorErrorCode]
    KV_CACHE_DESCRIPTOR_UNAVAILABLE: _ClassVar[KvCacheDescriptorErrorCode]
KV_CACHE_DESCRIPTOR_OK: KvCacheDescriptorErrorCode
KV_CACHE_DESCRIPTOR_UNAVAILABLE: KvCacheDescriptorErrorCode

class KvCacheGroupDescriptorPB(_message.Message):
    __slots__ = ("group_idx", "kind", "block_size", "sliding_window")
    GROUP_IDX_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    BLOCK_SIZE_FIELD_NUMBER: _ClassVar[int]
    SLIDING_WINDOW_FIELD_NUMBER: _ClassVar[int]
    group_idx: int
    kind: str
    block_size: int
    sliding_window: int
    def __init__(self, group_idx: _Optional[int] = ..., kind: _Optional[str] = ..., block_size: _Optional[int] = ..., sliding_window: _Optional[int] = ...) -> None: ...

class EngineCacheSettingsPB(_message.Message):
    __slots__ = ("use_eagle_pop", "mamba_cache_mode")
    USE_EAGLE_POP_FIELD_NUMBER: _ClassVar[int]
    MAMBA_CACHE_MODE_FIELD_NUMBER: _ClassVar[int]
    use_eagle_pop: bool
    mamba_cache_mode: str
    def __init__(self, use_eagle_pop: bool = ..., mamba_cache_mode: _Optional[str] = ...) -> None: ...

class KvCacheDescriptorPB(_message.Message):
    __slots__ = ("items", "settings", "err_code", "err_msg")
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    SETTINGS_FIELD_NUMBER: _ClassVar[int]
    ERR_CODE_FIELD_NUMBER: _ClassVar[int]
    ERR_MSG_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[KvCacheGroupDescriptorPB]
    settings: EngineCacheSettingsPB
    err_code: KvCacheDescriptorErrorCode
    err_msg: str
    def __init__(self, items: _Optional[_Iterable[_Union[KvCacheGroupDescriptorPB, _Mapping]]] = ..., settings: _Optional[_Union[EngineCacheSettingsPB, _Mapping]] = ..., err_code: _Optional[_Union[KvCacheDescriptorErrorCode, str]] = ..., err_msg: _Optional[str] = ...) -> None: ...

class KvCacheDescriptorRequestPB(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class StatusVersionPB(_message.Message):
    __slots__ = ("latest_cache_version", "latest_finished_version")
    LATEST_CACHE_VERSION_FIELD_NUMBER: _ClassVar[int]
    LATEST_FINISHED_VERSION_FIELD_NUMBER: _ClassVar[int]
    latest_cache_version: int
    latest_finished_version: int
    def __init__(self, latest_cache_version: _Optional[int] = ..., latest_finished_version: _Optional[int] = ...) -> None: ...

class WorkerStatusPB(_message.Message):
    __slots__ = ("status_version", "alive", "latest_finished_version")
    STATUS_VERSION_FIELD_NUMBER: _ClassVar[int]
    ALIVE_FIELD_NUMBER: _ClassVar[int]
    LATEST_FINISHED_VERSION_FIELD_NUMBER: _ClassVar[int]
    status_version: int
    alive: bool
    latest_finished_version: int
    def __init__(self, status_version: _Optional[int] = ..., alive: _Optional[bool] = ..., latest_finished_version: _Optional[int] = ...) -> None: ...

class KvCacheBlockListPB(_message.Message):
    __slots__ = ("raw_snapshot", "snapshot_version", "block_size")
    RAW_SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_VERSION_FIELD_NUMBER: _ClassVar[int]
    BLOCK_SIZE_FIELD_NUMBER: _ClassVar[int]
    raw_snapshot: bytes
    snapshot_version: int
    block_size: int
    def __init__(self, raw_snapshot: _Optional[bytes] = ..., snapshot_version: _Optional[int] = ..., block_size: _Optional[int] = ...) -> None: ...

class KvCacheBlocksRequestPB(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
