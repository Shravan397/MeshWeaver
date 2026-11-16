from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

PROTOCOL_VERSION = 1
MAX_PACKET_SIZE = 65507  # Maximum safe UDP payload size in bytes


class MessageType(str, Enum):
    """Supported protocol message types."""

    PING = "PING"
    PONG = "PONG"
    ERROR = "ERROR"


class ErrorCode(str, Enum):
    """Standardized protocol error codes."""

    INVALID_ENCODING = "INVALID_ENCODING"
    INVALID_JSON = "INVALID_JSON"
    PACKET_TOO_LARGE = "PACKET_TOO_LARGE"
    MISSING_FIELD = "MISSING_FIELD"
    INVALID_FIELD_TYPE = "INVALID_FIELD_TYPE"
    UNSUPPORTED_VERSION = "UNSUPPORTED_VERSION"
    UNKNOWN_MESSAGE_TYPE = "UNKNOWN_MESSAGE_TYPE"
    INVALID_PAYLOAD = "INVALID_PAYLOAD"


class ProtocolValidationError(Exception):
    """Raised when a message fails structural or semantic validation."""

    def __init__(
        self,
        code: ErrorCode | str,
        reason: str,
        ref_id: str | None = None,
    ) -> None:
        self.code = code.value if isinstance(code, ErrorCode) else str(code)
        self.reason = reason
        self.ref_id = ref_id
        super().__init__(f"[{self.code}] {self.reason}")


@dataclass(slots=True)
class Message:
    """Represents a validated MeshWeaver protocol packet."""

    type: MessageType
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
                    ref_id=self.message_id if isinstance(self.message_id, str) else None,
                ) from exc
        validate_message(self)

    def to_dict(self) -> dict[str, Any]:
        """Convert the message into a JSON-serializable dictionary."""
        return {
            "version": self.version,
            "type": self.type.value,
            "message_id": self.message_id,
            "sender_id": self.sender_id,
            "timestamp": self.timestamp,
            "payload": self.payload,
        }

