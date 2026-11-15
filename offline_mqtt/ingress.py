"""离线 MQTT 接入器外观。

线程模型：
- 进程内用一把 :class:`threading.RLock` 串行化所有写动作（与单写者语义一致）；
- 多进程/多接入器实例共享同一数据库文件时，由 SQLite 的 ``BEGIN IMMEDIATE``
  文件锁 + busy_timeout 保证互斥。

核心不变式（并发安全）：
订阅建立在单个立即写事务内完成——先取账本最大序号作为截断点 cutoff，再在同一
事务快照中读取保留表并写入订阅初始视图。任何并发 PUBLISH 要么整体先于该事务
提交（进初始保留视图，seq<=cutoff，不会再进实时流），要么整体后于它提交
（seq>cutoff，进实时流，保留值不会出现在初始视图），因此不漏、不重。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Optional

from .frames import (
    ConnectFrame,
    DisconnectFrame,
    FrameFormatError,
    PublishFrame,
    SubscribeFrame,
    parse_frame,
)
from .store import MqttStore
from .topic import (
    MAX_TOPIC_LENGTH,
    TopicError,
    topic_matches,
    validate_filter,
    validate_publish_topic,
)


@dataclass(frozen=True)
class IngestResult:
    """单条帧的处理结果。accepted=False 时 reason/quarantine_id 说明隔离原因。"""

    accepted: bool
    frame_type: str
    reason: Optional[str] = None
    detail: Optional[str] = None
    event_seq: Optional[int] = None
    subscription_id: Optional[int] = None
    quarantine_id: Optional[int] = None


@dataclass(frozen=True)
class DeliveredMessage:
    """订阅查询返回的一条消息。"""

    topic: str
    payload: bytes
    qos: int
    retain: bool          # True=作为保留值投递的初始视图；False=实时事件
    dup: bool
    message_id: Optional[int]
    session_id: str
    event_seq: Optional[int]
    retain_version: Optional[int]


@dataclass(frozen=True)
class PollResult:
    messages: list
    next_cursor: int
    retained_exhausted: bool


class MqttIngress:
    def __init__(self, db_path: str = ":memory:", *, max_topic_length: int = MAX_TOPIC_LENGTH,
                 busy_timeout_ms: int = 10000):
        self.max_topic_length = max_topic_length
        self._lock = threading.RLock()
        self.store = MqttStore(db_path, busy_timeout_ms=busy_timeout_ms)

    def close(self) -> None:
        self.store.close()

    def __enter__(self) -> "MqttIngress":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ------------------------------------------------------------- ingestion
    def ingest(self, record: Any) -> IngestResult:
        """处理一条项目定义记录；任何非法帧都进入隔离区而不抛出。"""
        with self._lock:
            try:
                frame = parse_frame(record)
            except FrameFormatError as exc:
                qid = self.store.quarantine(
                    reason=exc.reason, detail=exc.detail, raw=record,
                    frame_type=record.get("type") if isinstance(record, dict) else None,
                )
                return IngestResult(False, frame_type=record.get("type") if isinstance(record, dict) else "?",
                                    reason=exc.reason, detail=exc.detail, quarantine_id=qid)

            if isinstance(frame, ConnectFrame):
                return self._handle_connect(frame)
            if isinstance(frame, DisconnectFrame):
                return self._handle_disconnect(frame)
            if isinstance(frame, PublishFrame):
                return self._handle_publish(frame)
            if isinstance(frame, SubscribeFrame):
                return self._handle_subscribe(frame)
            # 理论不可达：解析器只产出上述四种帧
            raise RuntimeError(f"未支持的帧类型: {frame!r}")

    def ingest_many(self, records: list) -> list:
        return [self.ingest(r) for r in records]

    def _quarantine(self, frame, reason: str, detail: str) -> IngestResult:
        qid = self.store.quarantine(
            reason=reason, detail=detail, raw=frame.raw,
            frame_type=frame.kind,
            session_id=getattr(frame, "session_id", None),
            topic=getattr(frame, "topic", None),
        )
        return IngestResult(False, frame_type=frame.kind, reason=reason,
                            detail=detail, quarantine_id=qid)

    def _handle_connect(self, frame: ConnectFrame) -> IngestResult:
        # client_id 即会话身份；主题长度规则同样约束 client_id 中的异常长度场景由上层决定，
        # 这里只要求非空（解析器已保证）。
        with self.store.write() as conn:
            current = self.store.max_ledger_seq(conn)
            self.store.upsert_session(
                conn,
                session_id=frame.client_id,
                client_id=frame.client_id,
                session_present=frame.session_present,
                clean_session=frame.clean_session,
                keep_alive=frame.keep_alive,
                current_seq=current,
            )
        return IngestResult(True, frame_type=frame.kind)

    def _handle_disconnect(self, frame: DisconnectFrame) -> IngestResult:
        with self.store.write() as conn:
            current = self.store.max_ledger_seq(conn)
            self.store.mark_disconnected(conn, frame.session_id, current)
        return IngestResult(True, frame_type=frame.kind)

    def _handle_publish(self, frame: PublishFrame) -> IngestResult:
        # 语义校验：发布主题不允许通配符，且受长度限制。
        try:
            validate_publish_topic(frame.topic, self.max_topic_length)
        except TopicError as exc:
            return self._quarantine(frame, exc.reason, exc.detail)

        with self.store.write() as conn:
            # 账本与保留表在同一事务内更新：外部观察到的“到达次序”和
            # “当前保留值”始终来自同一个已提交状态。
            seq = self.store.insert_event(
                conn,
                session_id=frame.session_id,
                topic=frame.topic,
                payload=frame.payload,
                qos=frame.qos,
                retain=frame.retain,
                dup=frame.dup,
                message_id=frame.message_id,
            )
            if frame.retain:
                if frame.payload == b"":
                    # 空载荷保留发布：清除该主题的保留值（实时事件仍留账本）。
                    self.store.clear_retained(conn, frame.topic, seq)
                else:
                    self.store.upsert_retained(
                        conn,
                        topic=frame.topic,
                        payload=frame.payload,
                        qos=frame.qos,
                        source_session_id=frame.session_id,
                        message_id=frame.message_id,
                        event_seq=seq,
                    )
        return IngestResult(True, frame_type=frame.kind, event_seq=seq)

    def _handle_subscribe(self, frame: SubscribeFrame) -> IngestResult:
        # 先在事务外做过滤器语义校验（非法通配位置/超长 -> 隔离整帧）。
        validated: list[str] = []
        for flt in frame.filters:
            try:
                validated.append(validate_filter(flt, self.max_topic_length))
            except TopicError as exc:
                return self._quarantine(frame, exc.reason, exc.detail)

        with self.store.write() as conn:
            cutoff = self.store.max_ledger_seq(conn)
            # 同一事务快照：只可能看到 cutoff 之前已提交的保留状态。
            retained_snapshot = [
                row
                for row in self.store.all_retained(conn)
                if any(topic_matches(f, row["topic"]) for f in validated)
            ]
            sub_id = self.store.insert_subscription(
                conn,
                session_id=frame.session_id,
                message_id=frame.message_id,
                cutoff_seq=cutoff,
                filters=validated,
                retained_rows=retained_snapshot,
            )
        return IngestResult(True, frame_type=frame.kind, subscription_id=sub_id)

    # -------------------------------------------------------------- querying
    def poll(self, sub_id: int, limit: Optional[int] = None) -> PollResult:
        """按订阅拉取消息。

        首次调用先投递建立订阅时固化的保留值视图（每个匹配主题恰好一条），
        随后（及后续调用）按账本到达次序投递 cutoff 之后的实时事件。
        游标持久化在 ``delivery_cursor`` 中，进程重启后可继续。
        """
        with self._lock:
            sub = self.store.get_subscription(sub_id)
            if sub is None:
                raise KeyError(f"订阅不存在: sub_id={sub_id}")
            cursor = self.store.get_cursor(sub_id)
            if cursor is None:
                raise KeyError(f"订阅缺少投递游标: sub_id={sub_id}")

            messages: list[DeliveredMessage] = []
            retained_exhausted = True

            pending = self.store.pending_retained(sub_id)
            if pending:
                retained_exhausted = False
                delivered_topics: list[str] = []
                for row in pending:
                    messages.append(
                        DeliveredMessage(
                            topic=row["topic"],
                            payload=bytes(row["payload"]),
                            qos=row["qos"],
                            retain=True,
                            dup=False,
                            message_id=None,
                            session_id="",
                            event_seq=row["source_event_seq"],
                            retain_version=row["retain_version"],
                        )
                    )
                    delivered_topics.append(row["topic"])
                with self.store.write() as conn:
                    self.store.mark_retained_delivered(conn, sub_id, delivered_topics)
                return PollResult(messages, next_cursor=cursor,
                                  retained_exhausted=False)

            rows = self.store.live_events_after(cursor, limit)
            if rows:
                new_cursor = int(rows[-1]["seq"])
                with self.store.write() as conn:
                    self.store.advance_cursor(conn, sub_id, new_cursor)
                filters = self.store.get_filters(sub_id)
                for row in rows:
                    if not any(topic_matches(f, row["topic"]) for f in filters):
                        continue
                    messages.append(
                        DeliveredMessage(
                            topic=row["topic"],
                            payload=bytes(row["payload"]),
                            qos=row["qos"],
                            retain=False,
                            dup=bool(row["dup"]),
                            message_id=row["message_id"],
                            session_id=row["session_id"],
                            event_seq=int(row["seq"]),
                            retain_version=None,
                        )
                    )
                cursor = new_cursor

            return PollResult(messages, next_cursor=cursor,
                              retained_exhausted=retained_exhausted)

    def drain(self, sub_id: int) -> list:
        """一次性排空订阅当前可投递的全部消息（保留视图 + 全部实时事件）。"""
        out: list[DeliveredMessage] = []
        while True:
            result = self.poll(sub_id)
            out.extend(result.messages)
            if not result.messages and result.retained_exhausted:
                return out

    # ------------------------------------------------------------- inspection
    def get_retained(self, topic: str) -> Optional[dict]:
        row = self.store.get_retained(topic)
        if row is None:
            return None
        return dict(row)

    def list_retained(self) -> list[dict]:
        return [dict(r) for r in self.store.list_retained()]

    def list_events(self, limit: Optional[int] = None) -> list[dict]:
        return [dict(r) for r in self.store.list_events(limit)]

    def list_quarantine(self) -> list[dict]:
        return [dict(r) for r in self.store.list_quarantine()]

    def find_events_by_message_id(self, session_id: str, message_id: int) -> list[dict]:
        return [
            dict(r)
            for r in self.store.find_events_by_message_id(session_id, message_id)
        ]

    def get_session(self, session_id: str) -> Optional[dict]:
        row = self.store.get_session(session_id)
        return None if row is None else dict(row)

    def get_subscription(self, sub_id: int) -> Optional[dict]:
        row = self.store.get_subscription(sub_id)
        if row is None:
            return None
        data = dict(row)
        data["filters"] = self.store.get_filters(sub_id)
        data["cursor"] = self.store.get_cursor(sub_id)
        return data
