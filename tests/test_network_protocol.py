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


def valid_resource_payload():
    return {
        "cpu_percent": 45.5,
        "ram_percent": 62.3,
        "ram_total_mb": 8192.0,
        "ram_available_mb": 3072.0,
        "timestamp": "2026-11-24T18:30:00+00:00",
    }


def test_message_round_trip_preserves_existing_wire_format():
    original = Message(
        type="PING",
        sender_id="node-a",
        payload={"target": "127.0.0.1:9000"},
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


def test_resource_update_message_type_is_supported():
    message = Message(
        type=MessageType.RESOURCE_UPDATE,
        sender_id="node-a",
        payload=valid_resource_payload(),
    )

    assert message.to_dict()["type"] == "RESOURCE_UPDATE"


def test_resource_update_message_round_trip():
    resource_payload = valid_resource_payload()

    original = Message(
        type=MessageType.RESOURCE_UPDATE,
        sender_id="node-a",
        payload=resource_payload,
    )

    encoded = encode_message(original)
    decoded = decode_message(encoded)

    assert decoded.type == MessageType.RESOURCE_UPDATE
    assert decoded.sender_id == "node-a"
    assert decoded.payload == resource_payload


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("cpu_percent", -1),
        ("cpu_percent", 101),
        ("ram_percent", -1),
        ("ram_percent", 101),
        ("ram_total_mb", 0),
        ("ram_total_mb", -100),
        ("ram_available_mb", -1),
        ("ram_available_mb", 9000),
        ("cpu_percent", float("nan")),
        ("ram_percent", float("inf")),
        ("ram_total_mb", True),
        ("ram_available_mb", "3072"),
    ],
)
def test_resource_update_rejects_invalid_numeric_values(field, value):
    payload = valid_resource_payload()
    payload[field] = value

    with pytest.raises(ProtocolValidationError) as exc:
        Message(
            type=MessageType.RESOURCE_UPDATE,
            sender_id="node-a",
            payload=payload,
        )

    assert exc.value.code == ErrorCode.INVALID_PAYLOAD.value


def test_resource_update_rejects_missing_field():
    payload = valid_resource_payload()
    del payload["cpu_percent"]

    with pytest.raises(ProtocolValidationError) as exc:
        Message(
            type=MessageType.RESOURCE_UPDATE,
            sender_id="node-a",
            payload=payload,
        )

    assert exc.value.code == ErrorCode.INVALID_PAYLOAD.value


@pytest.mark.parametrize(
    "timestamp",
    [
        "",
        "not-a-date",
        "2026-11-24T18:30:00",
        12345,
    ],
)
def test_resource_update_rejects_invalid_timestamp(timestamp):
    payload = valid_resource_payload()
    payload["timestamp"] = timestamp

    with pytest.raises(ProtocolValidationError) as exc:
        Message(
            type=MessageType.RESOURCE_UPDATE,
            sender_id="node-a",
            payload=payload,
        )

    assert exc.value.code == ErrorCode.INVALID_PAYLOAD.value


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
    message = Message(
        type="PING",
        sender_id="node-a",
        payload={"data": "x" * 70000},
    )

    with pytest.raises(ProtocolValidationError) as exc:
        encode_message(message)

    assert exc.value.code == ErrorCode.PACKET_TOO_LARGE.value
