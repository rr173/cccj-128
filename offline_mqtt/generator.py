"""本地协议帧生成器。

完全离线运行：按脚本顺序或可控随机方式产出项目定义记录（dict），
可生成合法帧、边界主题以及用于隔离测试的非法帧。
"""

from __future__ import annotations

import random
from typing import Any, Optional


def connect(client_id: str, *, session_present: bool = False,
            clean_session: bool = True, keep_alive: int = 60) -> dict:
    return {
        "type": "CONNECT",
        "client_id": client_id,
        "session_present": session_present,
        "clean_session": clean_session,
        "keep_alive": keep_alive,
    }


def disconnect(session_id: str) -> dict:
    return {"type": "DISCONNECT", "session_id": session_id}


def publish(session_id: str, topic: str, payload: Any = None, *,
            qos: int = 0, retain: bool = False,
            message_id: Optional[int] = None, dup: bool = False) -> dict:
    return {
        "type": "PUBLISH",
        "session_id": session_id,
        "topic": topic,
        "payload": payload,
        "qos": qos,
        "retain": retain,
        "message_id": message_id,
        "dup": dup,
    }


def retained_publish(session_id: str, topic: str, payload: Any, *,
                     qos: int = 1, message_id: Optional[int] = None) -> dict:
    return publish(session_id, topic, payload, qos=qos, retain=True,
                   message_id=message_id)


def retained_clear(session_id: str, topic: str, *,
                   message_id: Optional[int] = None) -> dict:
    """空载荷保留发布：清除该主题保留值。"""
    return publish(session_id, topic, "", qos=1, retain=True,
                   message_id=message_id)


def subscribe(session_id: str, filters, *, message_id: Optional[int] = None) -> dict:
    if isinstance(filters, str):
        filters = [filters]
    return {
        "type": "SUBSCRIBE",
        "session_id": session_id,
        "message_id": message_id,
        "filters": list(filters),
    }


# -- 非法帧生成（供隔离测试） -------------------------------------------------

def malformed(*, reason: str, **fields: Any) -> dict:
    """手工构造非法记录。reason 仅用于测试可读性，不写入帧。"""
    frame = dict(fields)
    frame["_expected_reason"] = reason
    return frame


class LocalFrameGenerator:
    """脚本式 + 随机式离线帧源。"""

    def __init__(self, seed: Optional[int] = None):
        self._rng = random.Random(seed)
        self._script: list[dict] = []
        self._msgid = 0

    def next_message_id(self) -> int:
        self._msgid += 1
        return self._msgid

    def add(self, frame: dict) -> "LocalFrameGenerator":
        self._script.append(frame)
        return self

    def script(self) -> list[dict]:
        return list(self._script)

    def generate(self) -> list[dict]:
        """按脚本顺序产出帧（拷贝，避免调用方修改污染脚本）。"""
        return [dict(f) for f in self._script]

    def random_burst(self, sessions: list[str], topics: list[str],
                     count: int, *, retain_ratio: float = 0.3,
                     invalid_ratio: float = 0.0) -> list[dict]:
        """生成一批随机 PUBLISH；invalid_ratio>0 时混入结构/语义非法帧。"""
        frames: list[dict] = []
        invalid_builders = (
            lambda: {"type": "PUBLISH", "session_id": "", "topic": "x"},
            lambda: {"type": "PUBLISH", "session_id": "s", "topic": "a/+",
                     "payload": "wildcard not allowed in publish", "qos": 0},
            lambda: {"type": "SUBSCRIBE", "session_id": "s", "filters": ["a#"]},
            lambda: {"type": "WHATEVER", "x": 1},
            lambda: {"type": "PUBLISH", "session_id": "s", "topic": "a",
                     "payload": "x", "qos": 5},
        )
        for _ in range(count):
            if self._rng.random() < invalid_ratio:
                frames.append(self._rng.choice(invalid_builders)())
                continue
            session = self._rng.choice(sessions)
            topic = self._rng.choice(topics)
            retain = self._rng.random() < retain_ratio
            qos = self._rng.choice((0, 1, 2))
            mid = None if qos == 0 else self.next_message_id()
            frames.append(publish(
                session, topic,
                f"p-{self._rng.randrange(100000)}",
                qos=qos, retain=retain, message_id=mid,
            ))
        return frames
