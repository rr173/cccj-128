"""导出文件读取接口：把 ``messages.jsonl`` 还原成可重新接入的 PUBLISH 记录。

读取前强制核验完成标记与交付文件 sha256（不完整/被截断的导出不会被
当作可用输入）。``payload_base64`` 在此还原为 ``bytes``；``to_ingest_record``
产出可直接交给现有 ``MqttIngress.ingest`` 的项目定义记录，保持
topic/qos/retain/dup/message_id 与原始 session_id（不新造认证）。
"""

from __future__ import annotations

import base64
import json
import os
from dataclasses import dataclass
from typing import Iterator, Optional

from .pb_errors import EXPORT_INCOMPLETE, ExportError
from .pb_export import MESSAGES_FILE
from .pb_inspect import verify_complete


@dataclass(frozen=True)
class ExportPublish:
    """导出文件中的一条 PUBLISH 记录（payload 已还原为字节）。"""

    event_seq: int
    session_id: str
    topic: str
    payload: bytes
    qos: int
    retain: bool
    dup: bool
    message_id: Optional[int]
    message_type: str

    def to_ingest_record(self) -> dict:
        """交给现有接入 API（``MqttIngress.ingest``）的项目定义记录。"""
        return {
            "type": "PUBLISH",
            "session_id": self.session_id,
            "topic": self.topic,
            "payload": self.payload,
            "qos": self.qos,
            "retain": self.retain,
            "dup": self.dup,
            "message_id": self.message_id,
        }


def iter_export(export_dir: str) -> Iterator[ExportPublish]:
    """流式读取导出目录；目录不完整时抛 EXPORT_INCOMPLETE。"""
    verify_complete(export_dir)
    path = os.path.join(export_dir, MESSAGES_FILE)
    with open(path, "r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ExportError(
                    EXPORT_INCOMPLETE, f"消息文件第 {lineno} 行 JSON 非法: {exc}"
                ) from None
            try:
                payload = base64.b64decode(obj["payload_base64"], validate=True)
            except Exception:
                raise ExportError(
                    EXPORT_INCOMPLETE, f"消息文件第 {lineno} 行 payload_base64 非法"
                ) from None
            yield ExportPublish(
                event_seq=int(obj["event_seq"]),
                session_id=obj["session_id"],
                topic=obj["topic"],
                payload=payload,
                qos=int(obj["qos"]),
                retain=bool(obj["retain"]),
                dup=bool(obj["dup"]),
                message_id=obj["message_id"],
                message_type=obj["message_type"],
            )
