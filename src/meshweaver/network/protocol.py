
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


PROTOCOL_VERSION = 1
MAX_PACKET_SIZE = 65507

SUPPORTED_MESSAGE_TYPES = {"PING", "PONG", "ERROR"}


class ProtocolValidationError(ValueError):
    """Raised when a network message is invalid."""


@dataclass
class Message:
    """Represents one MeshWeaver network message."""

    type: str
    sender_id: str
    payload: dict[str, Any] = field(default_factory=dict)
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = field(default_factory=time.time)
    version: int = PROTOCOL_VERSION

    def to_dict(self) -> dict[str, Any]:
        """Convert the message into a JSON-compatible dictionary."""
        return {
            "version": self.version,
            "type": self.type,
            "message_id": self.message_id,
            "sender_id": self.sender_id,
            "timestamp": self.timestamp,
            "payload": self.payload,
        }


def validate_message(message: Message) -> None:
    """Check that a message contains valid fields."""

    if message.version != PROTOCOL_VERSION:
        raise ProtocolValidationError("Unsupported protocol version")

    if message.type not in SUPPORTED_MESSAGE_TYPES:
        raise ProtocolValidationError("Unknown message type")

    if not isinstance(message.sender_id, str) or not message.sender_id.strip():
        raise ProtocolValidationError("sender_id must be a non-empty string")

    if not isinstance(message.message_id, str) or not message.message_id:
        raise ProtocolValidationError("message_id must be a non-empty string")

    if (
        not isinstance(message.timestamp, (int, float))
        or isinstance(message.timestamp, bool)
    ):
        raise ProtocolValidationError("timestamp must be a number")

    if not isinstance(message.payload, dict):
        raise ProtocolValidationError("payload must be a dictionary")


def encode_message(message: Message) -> bytes:
    """Validate a message and encode it as UTF-8 JSON bytes."""

    validate_message(message)

    try:
        encoded = json.dumps(
            message.to_dict(),
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ProtocolValidationError(
            f"Could not encode message: {exc}"
        ) from exc

    if len(encoded) > MAX_PACKET_SIZE:
        raise ProtocolValidationError("Message exceeds maximum UDP packet size")

    return encoded


def decode_message(data: bytes) -> Message:
    """Decode UTF-8 JSON bytes and validate the resulting message."""

    if not isinstance(data, bytes):
        raise ProtocolValidationError("Incoming data must be bytes")

    if len(data) > MAX_PACKET_SIZE:
        raise ProtocolValidationError("Incoming packet is too large")

    try:
        decoded = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ProtocolValidationError("Message is not valid UTF-8") from exc

    try:
        content = json.loads(decoded)
    except json.JSONDecodeError as exc:
        raise ProtocolValidationError("Message is not valid JSON") from exc

    if not isinstance(content, dict):
        raise ProtocolValidationError("Message must be a JSON object")

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
            f"Missing fields: {', '.join(sorted(missing_fields))}"
        )

    if not isinstance(content["version"], int) or isinstance(
        content["version"], bool
    ):
        raise ProtocolValidationError("version must be an integer")

    message = Message(
        version=content["version"],
        type=content["type"],
        message_id=content["message_id"],
        sender_id=content["sender_id"],
        timestamp=content["timestamp"],
        payload=content["payload"],
    )

    validate_message(message)
    return message
