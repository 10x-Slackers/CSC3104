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

class State(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    STATE_UNSPECIFIED: _ClassVar[State]
    CONFIRMED: _ClassVar[State]
    NO_MATCH: _ClassVar[State]
    PARTIAL: _ClassVar[State]
    PENDING: _ClassVar[State]
    TIMED_OUT: _ClassVar[State]
    INVALID_CLAIM: _ClassVar[State]
DIRECTION_UNSPECIFIED: Direction
DOWNSTREAM: Direction
UPSTREAM: Direction
STATE_UNSPECIFIED: State
CONFIRMED: State
NO_MATCH: State
PARTIAL: State
PENDING: State
TIMED_OUT: State
INVALID_CLAIM: State

class TokenRef(_message.Message):
    __slots__ = ("pair", "token")
    PAIR_FIELD_NUMBER: _ClassVar[int]
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    pair: str
    token: str
    def __init__(self, pair: _Optional[str] = ..., token: _Optional[str] = ...) -> None: ...

class TraceRequest(_message.Message):
    __slots__ = ("query_id", "sender", "key_version", "ref", "direction", "hops_remaining", "deadline", "visited")
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    SENDER_FIELD_NUMBER: _ClassVar[int]
    KEY_VERSION_FIELD_NUMBER: _ClassVar[int]
    REF_FIELD_NUMBER: _ClassVar[int]
    DIRECTION_FIELD_NUMBER: _ClassVar[int]
    HOPS_REMAINING_FIELD_NUMBER: _ClassVar[int]
    DEADLINE_FIELD_NUMBER: _ClassVar[int]
    VISITED_FIELD_NUMBER: _ClassVar[int]
    query_id: str
    sender: str
    key_version: str
    ref: TokenRef
    direction: Direction
    hops_remaining: int
    deadline: str
    visited: _containers.RepeatedCompositeFieldContainer[TokenRef]
    def __init__(self, query_id: _Optional[str] = ..., sender: _Optional[str] = ..., key_version: _Optional[str] = ..., ref: _Optional[_Union[TokenRef, _Mapping]] = ..., direction: _Optional[_Union[Direction, str]] = ..., hops_remaining: _Optional[int] = ..., deadline: _Optional[str] = ..., visited: _Optional[_Iterable[_Union[TokenRef, _Mapping]]] = ...) -> None: ...

class LinkClaim(_message.Message):
    __slots__ = ("signer", "recipient", "query_id", "key_version", "in_ref", "out_ref", "signature")
    SIGNER_FIELD_NUMBER: _ClassVar[int]
    RECIPIENT_FIELD_NUMBER: _ClassVar[int]
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_VERSION_FIELD_NUMBER: _ClassVar[int]
    IN_REF_FIELD_NUMBER: _ClassVar[int]
    OUT_REF_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    signer: str
    recipient: str
    query_id: str
    key_version: str
    in_ref: TokenRef
    out_ref: TokenRef
    signature: bytes
    def __init__(self, signer: _Optional[str] = ..., recipient: _Optional[str] = ..., query_id: _Optional[str] = ..., key_version: _Optional[str] = ..., in_ref: _Optional[_Union[TokenRef, _Mapping]] = ..., out_ref: _Optional[_Union[TokenRef, _Mapping]] = ..., signature: _Optional[bytes] = ...) -> None: ...

class TraceReply(_message.Message):
    __slots__ = ("state", "claims")
    STATE_FIELD_NUMBER: _ClassVar[int]
    CLAIMS_FIELD_NUMBER: _ClassVar[int]
    state: State
    claims: _containers.RepeatedCompositeFieldContainer[LinkClaim]
    def __init__(self, state: _Optional[_Union[State, str]] = ..., claims: _Optional[_Iterable[_Union[LinkClaim, _Mapping]]] = ...) -> None: ...

class ChainResult(_message.Message):
    __slots__ = ("chain_digest", "sender", "state", "claims")
    CHAIN_DIGEST_FIELD_NUMBER: _ClassVar[int]
    SENDER_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    CLAIMS_FIELD_NUMBER: _ClassVar[int]
    chain_digest: str
    sender: str
    state: State
    claims: _containers.RepeatedCompositeFieldContainer[LinkClaim]
    def __init__(self, chain_digest: _Optional[str] = ..., sender: _Optional[str] = ..., state: _Optional[_Union[State, str]] = ..., claims: _Optional[_Iterable[_Union[LinkClaim, _Mapping]]] = ...) -> None: ...

class Ack(_message.Message):
    __slots__ = ("chain_digest",)
    CHAIN_DIGEST_FIELD_NUMBER: _ClassVar[int]
    chain_digest: str
    def __init__(self, chain_digest: _Optional[str] = ...) -> None: ...
