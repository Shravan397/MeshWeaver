import json

import pytest

from meshweaver.network.protocol import (
    ErrorCode,
    Message,
    MessageType,
    ProtocolValidationError,
    decode_message,
    encode_message,
)


def test_message_round_trip_preserves_existing_wire_format():
    original = Message(
        type="PING", sender_id="node-a", payload={"target": "127.0.0.1:9000"}
    )
    encoded = encode_message(original)
    decoded = decode_message(encoded)

    assert decoded.type == "PING"
    assert decoded.sender_id == original.sender_id
    assert decoded.message_id == original.message_id
    assert decoded.payload == original.payload
    assert json.loads(encoded)["type"] == "PING"


def test_message_type_enum_is_supported():
    message = Message(type=MessageType.PONG, sender_id="node-b")
    assert message.to_dict()["type"] == "PONG"


def test_unknown_message_type_has_error_code():
    with pytest.raises(ProtocolValidationError) as exc:
        Message(type="UNKNOWN", sender_id="node-a")
    assert exc.value.code == ErrorCode.UNKNOWN_MESSAGE_TYPE.value


def test_decode_rejects_missing_fields():
    with pytest.raises(ProtocolValidationError) as exc:
        decode_message(b'{"type":"PING"}')
    assert exc.value.code == ErrorCode.MISSING_FIELD.value


def test_decode_rejects_invalid_json():
    with pytest.raises(ProtocolValidationError) as exc:
        decode_message(b"{")
    assert exc.value.code == ErrorCode.INVALID_JSON.value


def test_decode_rejects_invalid_utf8():
    with pytest.raises(ProtocolValidationError) as exc:
        decode_message(b"\xff")
    assert exc.value.code == ErrorCode.INVALID_ENCODING.value


def test_encode_rejects_oversized_payload():
    message = Message(type="PING", sender_id="node-a", payload={"data": "x" * 70000})
    with pytest.raises(ProtocolValidationError) as exc:
        encode_message(message)
    assert exc.value.code == ErrorCode.PACKET_TOO_LARGE.value
