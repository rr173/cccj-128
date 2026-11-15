"""本地协议帧模型与解析器。

输入为“项目定义记录”（普通 dict），例如::

    {"type": "CONNECT", "client_id": "sensor-01", "session_present": false}
    {"type": "PUBLISH", "session_id": "sensor-01", "topic": "a/b",
     "payload": "on", "qos": 1, "retain": false, "message_id": 1, "dup": false}
    {"type": "SUBSCRIBE", "session_id": "sensor-01",
     "message_id": 10, "filters": ["a/+", "x/#"]}

解析只负责结构与基本取值域；主题通配/长度等语义校验在接入层完成，
语义非法的帧会被送入隔离区而非抛给调用方。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

FRAME_TYPES = ("CONNECT", "PUBLISH", "SUBSCRIBE", "DISCONNECT")


class FrameFormatError(ValueError):
    """帧记录结构非法（缺字段、类型错误等），携带机器可读原因码。"""

    def __init__(self, reason: str, detail: str):
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True)
class ConnectFrame:
    client_id: str
    session_present: bool = False
    clean_session: bool = True
    keep_alive: int = 0
    raw: dict = field(repr=False, default_factory=dict)
    kind: str = "CONNECT"


@dataclass(frozen=True)
class PublishFrame:
    session_id: str
    topic: str
    payload: bytes
    qos: int
    retain: bool
    message_id: Optional[int]
    dup: bool
    raw: dict = field(repr=False, default_factory=dict)
    kind: str = "PUBLISH"


@dataclass(frozen=True)
class SubscribeFrame:
    session_id: str
    message_id: Optional[int]
    filters: tuple
    raw: dict = field(repr=False, default_factory=dict)
    kind: str = "SUBSCRIBE"


@dataclass(frozen=True)
class DisconnectFrame:
    session_id: str
    raw: dict = field(repr=False, default_factory=dict)
    kind: str = "DISCONNECT"


def _require_str(record: dict, key: str) -> str:
    value = record.get(key)
    if not isinstance(value, str) or value == "":
        raise FrameFormatError(
            "MISSING_OR_INVALID_FIELD", f"字段 {key!r} 必须是非空字符串"
        )
    return value


def _optional_message_id(record: dict) -> Optional[int]:
    value = record.get("message_id")
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise FrameFormatError(
            "MISSING_OR_INVALID_FIELD", "message_id 必须是整数或 null"
        )
    if not (0 <= value <= 0xFFFF):
        raise FrameFormatError("MESSAGE_ID_OUT_OF_RANGE", f"message_id={value} 超出 0..65535")
    return value


def _coerce_payload(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    raise FrameFormatError(
        "MISSING_OR_INVALID_FIELD",
        f"payload 必须是字符串/字节/null，实际类型 {type(value).__name__}",
    )


def _as_bool(record: dict, key: str, default: bool) -> bool:
    value = record.get(key, default)
    if not isinstance(value, bool):
        raise FrameFormatError("MISSING_OR_INVALID_FIELD", f"字段 {key!r} 必须是布尔值")
    return value


def parse_frame(record: Any) -> "ConnectFrame | PublishFrame | SubscribeFrame | DisconnectFrame":
    """把一条项目定义记录解析为强类型帧；结构非法时抛 :class:`FrameFormatError`。"""
    if not isinstance(record, dict):
        raise FrameFormatError("NOT_A_RECORD", f"帧必须是 dict，实际类型 {type(record).__name__}")
    frame_type = record.get("type")
    if not isinstance(frame_type, str):
        raise FrameFormatError("MISSING_TYPE", "缺少字符串类型字段 type")
    frame_type = frame_type.upper()
    if frame_type not in FRAME_TYPES:
        raise FrameFormatError("UNKNOWN_FRAME_TYPE", f"未知帧类型 {frame_type!r}")

    if frame_type == "CONNECT":
        client_id = _require_str(record, "client_id")
        keep_alive = record.get("keep_alive", 0)
        if isinstance(keep_alive, bool) or not isinstance(keep_alive, int) or keep_alive < 0:
            raise FrameFormatError("MISSING_OR_INVALID_FIELD", "keep_alive 必须是非负整数")
        return ConnectFrame(
            client_id=client_id,
            session_present=_as_bool(record, "session_present", False),
            clean_session=_as_bool(record, "clean_session", True),
            keep_alive=keep_alive,
            raw=record,
        )

    if frame_type == "PUBLISH":
        session_id = _require_str(record, "session_id")
        topic = record.get("topic")
        if not isinstance(topic, str):
            raise FrameFormatError("MISSING_OR_INVALID_FIELD", "字段 'topic' 必须是字符串")
        qos = record.get("qos", 0)
        if isinstance(qos, bool) or not isinstance(qos, int) or qos not in (0, 1, 2):
            raise FrameFormatError("INVALID_QOS", f"qos 必须是 0/1/2，实际 {qos!r}")
        message_id = _optional_message_id(record)
        if qos > 0 and message_id is None:
            raise FrameFormatError("MISSING_MESSAGE_ID", "QoS>0 的 PUBLISH 必须带 message_id")
        if qos == 0 and message_id == 0:
            raise FrameFormatError("MESSAGE_ID_OUT_OF_RANGE", "QoS0 不允许使用 message_id=0")
        return PublishFrame(
            session_id=session_id,
            topic=topic,
            payload=_coerce_payload(record.get("payload")),
            qos=qos,
            retain=_as_bool(record, "retain", False),
            message_id=message_id,
            dup=_as_bool(record, "dup", False),
            raw=record,
        )

    if frame_type == "SUBSCRIBE":
        session_id = _require_str(record, "session_id")
        filters = record.get("filters")
        if not isinstance(filters, list) or not filters:
            raise FrameFormatError("MISSING_OR_INVALID_FIELD", "filters 必须是非空列表")
        if not all(isinstance(f, str) for f in filters):
            raise FrameFormatError("MISSING_OR_INVALID_FIELD", "filters 中每一项必须是字符串")
        return SubscribeFrame(
            session_id=session_id,
            message_id=_optional_message_id(record),
            filters=tuple(filters),
            raw=record,
        )

    # DISCONNECT
    return DisconnectFrame(session_id=_require_str(record, "session_id"), raw=record)
