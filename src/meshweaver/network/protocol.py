from __future__ import annotations

import json
import math
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

PROTOCOL_VERSION = 1
MAX_PACKET_SIZE = 65507


class MessageType(str, Enum):
    """Supported MeshWeaver message types."""

    PING = "PING"
    PONG = "PONG"
    ERROR = "ERROR"


class ErrorCode(str, Enum):
    """Stable error codes for protocol validation failures."""

    INVALID_ENCODING = "INVALID_ENCODING"
    INVALID_JSON = "INVALID_JSON"
    PACKET_TOO_LARGE = "PACKET_TOO_LARGE"
    MISSING_FIELD = "MISSING_FIELD"
    INVALID_FIELD_TYPE = "INVALID_FIELD_TYPE"
    UNSUPPORTED_VERSION = "UNSUPPORTED_VERSION"
    UNKNOWN_MESSAGE_TYPE = "UNKNOWN_MESSAGE_TYPE"
    INVALID_PAYLOAD = "INVALID_PAYLOAD"


class ProtocolValidationError(ValueError):
    """Raised when a network message is invalid."""

    def __init__(
        self,
        code: ErrorCode | str,
        reason: str | None = None,
        ref_id: str | None = None,
    ) -> None:
        # Keep the original one-argument exception usage compatible.
        if reason is None:
            reason = str(code)
            code = ErrorCode.INVALID_PAYLOAD
        self.code = code.value if isinstance(code, ErrorCode) else str(code)
        self.reason = reason
        self.ref_id = ref_id
        super().__init__(f"[{self.code}] {self.reason}")


SUPPORTED_MESSAGE_TYPES = {item.value for item in MessageType}


@dataclass
class Message:
    """Represents one MeshWeaver network message."""

    type: MessageType | str
    sender_id: str
    payload: dict[str, Any] = field(default_factory=dict)
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = field(default_factory=time.time)
    version: int = PROTOCOL_VERSION

    def __post_init__(self) -> None:
        if isinstance(self.type, str) and not isinstance(self.type, MessageType):
            try:
                self.type = MessageType(self.type)
            except ValueError as exc:
                raise ProtocolValidationError(
                    ErrorCode.UNKNOWN_MESSAGE_TYPE,
                    f"Unsupported message type: {self.type!r}",
                    self.message_id if isinstance(self.message_id, str) else None,
                ) from exc
        validate_message(self)

    def to_dict(self) -> dict[str, Any]:
        """Convert the message into a JSON-compatible dictionary."""
        return {
            "version": self.version,
            "type": self.type.value
            if isinstance(self.type, MessageType)
            else self.type,
            "message_id": self.message_id,
            "sender_id": self.sender_id,
            "timestamp": self.timestamp,
            "payload": self.payload,
        }


def validate_message(message: Message) -> None:
    """Validate the structure and values of a MeshWeaver message."""
    if not isinstance(message.version, int) or isinstance(message.version, bool):
        raise ProtocolValidationError(
            ErrorCode.INVALID_FIELD_TYPE, "version must be an integer"
        )
    if message.version != PROTOCOL_VERSION:
        raise ProtocolValidationError(
            ErrorCode.UNSUPPORTED_VERSION,
            f"Unsupported protocol version: {message.version}",
            message.message_id,
        )
    message_type = (
        message.type.value if isinstance(message.type, MessageType) else message.type
    )
    if not isinstance(message_type, str) or message_type not in SUPPORTED_MESSAGE_TYPES:
        raise ProtocolValidationError(
            ErrorCode.UNKNOWN_MESSAGE_TYPE,
            f"Unsupported message type: {message_type!r}",
        )
    if not isinstance(message.sender_id, str) or not message.sender_id.strip():
        raise ProtocolValidationError(
            ErrorCode.INVALID_FIELD_TYPE, "sender_id must be a non-empty string"
        )
    if not isinstance(message.message_id, str) or not message.message_id.strip():
        raise ProtocolValidationError(
            ErrorCode.INVALID_FIELD_TYPE, "message_id must be a non-empty string"
        )
    if (
        not isinstance(message.timestamp, (int, float))
        or isinstance(message.timestamp, bool)
        or not math.isfinite(message.timestamp)
    ):
        raise ProtocolValidationError(
            ErrorCode.INVALID_FIELD_TYPE, "timestamp must be a finite number"
        )
    if not isinstance(message.payload, dict):
        raise ProtocolValidationError(
            ErrorCode.INVALID_PAYLOAD, "payload must be a dictionary"
        )


def encode_message(message: Message) -> bytes:
    """Validate a message and encode it as UTF-8 JSON bytes."""
    validate_message(message)
    try:
        encoded = json.dumps(
            message.to_dict(),
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ProtocolValidationError(
            ErrorCode.INVALID_PAYLOAD, f"Could not encode message: {exc}"
        ) from exc
    if len(encoded) > MAX_PACKET_SIZE:
        raise ProtocolValidationError(
            ErrorCode.PACKET_TOO_LARGE, "Message exceeds maximum UDP packet size"
        )
    return encoded


def decode_message(data: bytes) -> Message:
    """Decode UTF-8 JSON bytes and validate the resulting message."""
    if not isinstance(data, bytes):
        raise ProtocolValidationError(
            ErrorCode.INVALID_ENCODING, "Incoming data must be bytes"
        )
    if len(data) > MAX_PACKET_SIZE:
        raise ProtocolValidationError(
            ErrorCode.PACKET_TOO_LARGE, "Incoming packet is too large"
        )
    try:
        decoded = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ProtocolValidationError(
            ErrorCode.INVALID_ENCODING, "Message is not valid UTF-8"
        ) from exc
    try:
        content = json.loads(decoded)
    except json.JSONDecodeError as exc:
        raise ProtocolValidationError(
            ErrorCode.INVALID_JSON, "Message is not valid JSON"
        ) from exc
    if not isinstance(content, dict):
        raise ProtocolValidationError(
            ErrorCode.INVALID_JSON, "Message must be a JSON object"
        )

    required_fields = {
        "version",
        "type",
        "message_id",
        "sender_id",
        "timestamp",
        "payload",
    }
    missing_fields = required_fields - content.keys()
    if missing_fields:
        raise ProtocolValidationError(
            ErrorCode.MISSING_FIELD,
            f"Missing fields: {', '.join(sorted(missing_fields))}",
        )
    if not isinstance(content["type"], str):
        raise ProtocolValidationError(
            ErrorCode.INVALID_FIELD_TYPE, "type must be a string"
        )

    return Message(
        version=content["version"],
        type=content["type"],
        message_id=content["message_id"],
        sender_id=content["sender_id"],
        timestamp=content["timestamp"],
        payload=content["payload"],
    )
