from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Direction(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DIRECTION_UNSPECIFIED: _ClassVar[Direction]
    DOWNSTREAM: _ClassVar[Direction]
    UPSTREAM: _ClassVar[Direction]
DIRECTION_UNSPECIFIED: Direction
DOWNSTREAM: Direction
UPSTREAM: Direction

class Empty(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class PublicKey(_message.Message):
    __slots__ = ("agreement", "signing")
    AGREEMENT_FIELD_NUMBER: _ClassVar[int]
    SIGNING_FIELD_NUMBER: _ClassVar[int]
    agreement: bytes
    signing: bytes
    def __init__(self, agreement: _Optional[bytes] = ..., signing: _Optional[bytes] = ...) -> None: ...

class PublicKeys(_message.Message):
    __slots__ = ("keys",)
    KEYS_FIELD_NUMBER: _ClassVar[int]
    keys: _containers.RepeatedCompositeFieldContainer[PublicKey]
    def __init__(self, keys: _Optional[_Iterable[_Union[PublicKey, _Mapping]]] = ...) -> None: ...

class TraceRequest(_message.Message):
    __slots__ = ("token", "direction", "hops_remaining", "visited")
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    DIRECTION_FIELD_NUMBER: _ClassVar[int]
    HOPS_REMAINING_FIELD_NUMBER: _ClassVar[int]
    VISITED_FIELD_NUMBER: _ClassVar[int]
    token: bytes
    direction: Direction
    hops_remaining: int
    visited: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, token: _Optional[bytes] = ..., direction: _Optional[_Union[Direction, str]] = ..., hops_remaining: _Optional[int] = ..., visited: _Optional[_Iterable[bytes]] = ...) -> None: ...

class LinkClaim(_message.Message):
    __slots__ = ("signer", "in_from", "in_token", "out_to", "out_token", "cut", "signature")
    SIGNER_FIELD_NUMBER: _ClassVar[int]
    IN_FROM_FIELD_NUMBER: _ClassVar[int]
    IN_TOKEN_FIELD_NUMBER: _ClassVar[int]
    OUT_TO_FIELD_NUMBER: _ClassVar[int]
    OUT_TOKEN_FIELD_NUMBER: _ClassVar[int]
    CUT_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    signer: str
    in_from: str
    in_token: bytes
    out_to: str
    out_token: bytes
    cut: bool
    signature: bytes
    def __init__(self, signer: _Optional[str] = ..., in_from: _Optional[str] = ..., in_token: _Optional[bytes] = ..., out_to: _Optional[str] = ..., out_token: _Optional[bytes] = ..., cut: _Optional[bool] = ..., signature: _Optional[bytes] = ...) -> None: ...

class TraceReply(_message.Message):
    __slots__ = ("claims",)
    CLAIMS_FIELD_NUMBER: _ClassVar[int]
    claims: _containers.RepeatedCompositeFieldContainer[LinkClaim]
    def __init__(self, claims: _Optional[_Iterable[_Union[LinkClaim, _Mapping]]] = ...) -> None: ...

class Chain(_message.Message):
    __slots__ = ("claims",)
    CLAIMS_FIELD_NUMBER: _ClassVar[int]
    claims: _containers.RepeatedCompositeFieldContainer[LinkClaim]
    def __init__(self, claims: _Optional[_Iterable[_Union[LinkClaim, _Mapping]]] = ...) -> None: ...

class Ack(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
