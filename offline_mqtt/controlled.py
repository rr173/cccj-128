"""受控订阅/读取入口：用范围准入保护真实订阅创建。

这是本次新增能力的**权限边界**。旧入口（``MqttIngress.ingest`` 直接消费
SUBSCRIBE 记录、``MqttIngress.poll`` 直接按订阅读取）保持原样、不缩小范围；
只有本模块的 :class:`ControlledIngress` 在创建订阅前强制执行整份输入校验与
逐过滤器范围准入，并在读取时把结果限定在采集命名空间内。

- 身份（``principal_id``）由调用方负责认证后**显式传入**，绝不从 SUBSCRIBE 记录
  的 session_id/client_id 推断，也不新建登录服务；
- 先验证整份输入（策略已在 :class:`~offline_mqtt.admission.AdmissionEngine` 构造
  时校验，这里再校验记录与过滤器列表），再逐个过滤器做范围判断；任何一项越权或
  输入无效都**不创建订阅**——不产生 subscriptions、subscription_retained、
  delivery_cursor 任何一行；
- 全部获准后，复用现有接入能力（同一条立即写事务、cutoff、初始 retained 视图、
  实时游标语义）建立真实订阅，返回实际 subscription_id 与逐过滤器准入依据；
- 受控读取只交付采集命名空间内的消息；库里其他用途的主题（大写、``$`` 前缀、
  空层等）即便底层 ``#`` 匹配也不会混入。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from .admission import (
    AdmissionEngine,
    AdmissionInputError,
    ScopeDecision,
)
from .frames import FrameFormatError, SubscribeFrame, parse_frame
from .ingress import DeliveredMessage, MqttIngress, PollResult
from .namespace import is_namespace_topic
from .topic import TopicError, topic_matches, validate_filter


@dataclass(frozen=True)
class FilterAdmission:
    """单个过滤器的准入依据（全部通过时用于重现“为什么允许”）。"""

    request_filter: str
    safe: bool
    detail: dict

    def to_dict(self) -> dict:
        out = {"request_filter": self.request_filter, "safe": self.safe}
        out.update(self.detail)
        return out


@dataclass(frozen=True)
class ControlledSubscribeResult:
    granted: bool
    principal_id: str
    subscription_id: Optional[int]
    filter_decisions: tuple
    errors: tuple
    policy_summary: dict

    def to_dict(self) -> dict:
        return {
            "granted": self.granted,
            "principal_id": self.principal_id,
            "subscription_id": self.subscription_id,
            "filter_admissions": [d.to_dict() for d in self.filter_decisions],
            "input_errors": list(self.errors),
            "policy": self.policy_summary,
        }


class ControlledIngress:
    """在现有 :class:`MqttIngress` 之上叠加范围准入的受控入口。"""

    def __init__(self, ingress: MqttIngress, engine: AdmissionEngine):
        self.ingress = ingress
        self.engine = engine

    # ----------------------------------------------------------- 受控订阅
    def controlled_subscribe(
        self,
        principal_id: str,
        subscribe_record: Any,
    ) -> ControlledSubscribeResult:
        """验证整份输入并逐过滤器准入；全通过才创建真实订阅。

        任何输入错误或越权都返回 ``granted=False`` 且保证没有任何订阅相关行落库。
        资源不足以 :class:`AdmissionResourceLimit` 抛出（区别于普通越权）。
        """
        policy_summary = self.engine.policy_summary()

        # 1) 记录结构校验（与旧入口同一解析器，但失败在这里返回错误而非隔离）。
        try:
            frame = parse_frame(subscribe_record)
        except FrameFormatError as exc:
            return self._deny(principal_id, [{
                "location": "subscribe_record", "reason": exc.reason, "detail": exc.detail,
            }], policy_summary)
        if not isinstance(frame, SubscribeFrame):
            return self._deny(principal_id, [{
                "location": "subscribe_record",
                "reason": "WRONG_FRAME_TYPE",
                "detail": f"受控订阅只接受 SUBSCRIBE 记录，实际类型 {frame.kind}",
            }], policy_summary)

        # 2) 过滤器基本合法性（旧入口语义校验），并显式拒绝重复过滤器。
        input_errors: list[dict] = []
        normalized: list[str] = []
        seen: set[str] = set()
        for index, flt in enumerate(frame.filters):
            try:
                validate_filter(flt, self.ingress.max_topic_length)
            except TopicError as exc:
                input_errors.append({
                    "location": f"filters[{index}]", "reason": exc.reason, "detail": exc.detail,
                })
                continue
            if flt in seen:
                input_errors.append({
                    "location": f"filters[{index}]",
                    "reason": "DUPLICATE_FILTER",
                    "detail": f"请求包含重复过滤器 {flt!r}",
                })
                continue
            seen.add(flt)
            normalized.append(flt)
        if input_errors:
            return self._deny(principal_id, input_errors, policy_summary)

        # 3) 逐过滤器做范围准入。全部判断先跑完，再决定是否建订阅。
        #    未知身份等准入输入错误在这里转成结构化拒绝，不向调用方抛出。
        decisions: list[ScopeDecision] = []
        try:
            for flt in normalized:
                # decide_scope 自身会做采集命名空间校验；非法过滤器属于输入错误。
                decisions.append(self.engine.decide_scope(principal_id, flt))
        except AdmissionInputError as exc:
            return self._deny(principal_id, exc.errors, policy_summary)

        if not all(d.safe for d in decisions):
            filter_admissions = tuple(
                FilterAdmission(d.request_filter, d.safe, d.to_dict()) for d in decisions
            )
            return ControlledSubscribeResult(
                granted=False,
                principal_id=principal_id,
                subscription_id=None,
                filter_decisions=filter_admissions,
                errors=(),
                policy_summary=policy_summary,
            )

        # 4) 全部获准：复用现有接入能力创建真实订阅（保留 retained 初始交付与
        #    实时游标语义不变）。
        result = self.ingress.ingest(subscribe_record)
        if not result.accepted:
            # 走到这里说明过滤器通过了 validate_filter 仍被旧入口隔离（理论不可达）。
            return self._deny(principal_id, [{
                "location": "subscribe_record",
                "reason": result.reason or "SUBSCRIBE_REJECTED",
                "detail": result.detail or "底层接入器拒绝了该 SUBSCRIBE",
            }], policy_summary)

        filter_admissions = tuple(
            FilterAdmission(d.request_filter, True, d.to_dict()) for d in decisions
        )
        return ControlledSubscribeResult(
            granted=True,
            principal_id=principal_id,
            subscription_id=result.subscription_id,
            filter_decisions=filter_admissions,
            errors=(),
            policy_summary=policy_summary,
        )

    # ----------------------------------------------------------- 受控读取
    def controlled_poll(self, sub_id: int, limit: Optional[int] = None) -> PollResult:
        """按订阅读取并过滤：只交付采集命名空间内的消息。

        游标仍由底层接入器按账本推进（不漏不重的交接语义不变）；命名空间外的
        消息被跳过、不投递给受控调用方。保留初始视图阶段也会把命名空间外的
        retained 行一并标记为已消费，避免它们反复出现导致受控读取无法进入实时阶段。
        """
        store = self.ingress.store
        sub = store.get_subscription(sub_id)
        if sub is None:
            raise KeyError(f"订阅不存在: sub_id={sub_id}")
        cursor = store.get_cursor(sub_id)
        if cursor is None:
            raise KeyError(f"订阅缺少投递游标: sub_id={sub_id}")

        # 阶段一：订阅建立时固化的初始保留视图。全部行都推进投递标记，
        # 只有命名空间内的行被实际交付。
        pending = store.pending_retained(sub_id)
        if pending:
            kept: list[DeliveredMessage] = []
            all_topics = []
            for row in pending:
                all_topics.append(row["topic"])
                if is_namespace_topic(row["topic"]):
                    kept.append(DeliveredMessage(
                        topic=row["topic"],
                        payload=bytes(row["payload"]),
                        qos=row["qos"],
                        retain=True,
                        dup=False,
                        message_id=None,
                        session_id="",
                        event_seq=row["source_event_seq"],
                        retain_version=row["retain_version"],
                    ))
            with store.write() as conn:
                store.mark_retained_delivered(conn, sub_id, all_topics)
            return PollResult(messages=kept, next_cursor=cursor, retained_exhausted=False)

        # 阶段二：cutoff 之后的实时事件。游标对账本推进（不漏不重），
        # 命名空间外的事件被过滤掉。
        rows = store.live_events_after(cursor, limit)
        messages: list[DeliveredMessage] = []
        if rows:
            new_cursor = int(rows[-1]["seq"])
            with store.write() as conn:
                store.advance_cursor(conn, sub_id, new_cursor)
            filters = store.get_filters(sub_id)
            for row in rows:
                if not any(topic_matches(f, row["topic"]) for f in filters):
                    continue
                if not is_namespace_topic(row["topic"]):
                    continue
                messages.append(DeliveredMessage(
                    topic=row["topic"],
                    payload=bytes(row["payload"]),
                    qos=row["qos"],
                    retain=False,
                    dup=bool(row["dup"]),
                    message_id=row["message_id"],
                    session_id=row["session_id"],
                    event_seq=int(row["seq"]),
                    retain_version=None,
                ))
            cursor = new_cursor
        return PollResult(messages, next_cursor=cursor, retained_exhausted=True)

    def controlled_drain(self, sub_id: int) -> list:
        out = []
        while True:
            result = self.controlled_poll(sub_id)
            out.extend(result.messages)
            if not result.messages and result.retained_exhausted:
                return out

    # ------------------------------------------------------------- 检查
    def namespace_filtered_topics(self, sub_id: int) -> dict:
        """展示某次受控读取会保留/剔除哪些主题（供演练与审计，不改游标）。"""
        filters = self.ingress.store.get_filters(sub_id)
        retained = self.ingress.store.pending_retained(sub_id)
        # 注意：这里只做静态展示，不推进任何投递状态。
        kept, dropped = [], []
        for row in retained:
            (kept if is_namespace_topic(row["topic"]) else dropped).append(row["topic"])
        return {"filters": filters, "kept_retained_topics": kept,
                "dropped_non_namespace_topics": dropped}

    @staticmethod
    def _deny(principal_id: str, errors: list[dict], policy_summary: dict) -> ControlledSubscribeResult:
        return ControlledSubscribeResult(
            granted=False,
            principal_id=principal_id,
            subscription_id=None,
            filter_decisions=(),
            errors=tuple(errors),
            policy_summary=policy_summary,
        )


__all__ = [
    "ControlledIngress",
    "ControlledSubscribeResult",
    "FilterAdmission",
    "AdmissionInputError",
]
