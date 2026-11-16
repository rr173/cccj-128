"""离线 MQTT 消息接入器。

不连接任何代理，只消费“项目定义记录”形式的协议帧（dict），
将实时消息写入事件账本、将保留值写入保留表，并提供按订阅的查询接口。
"""

from .ingress import MqttIngress
from .frames import (
    ConnectFrame,
    PublishFrame,
    SubscribeFrame,
    DisconnectFrame,
    parse_frame,
    FrameFormatError,
)
from .topic import (
    validate_publish_topic,
    validate_filter,
    topic_matches,
    TopicError,
    MAX_TOPIC_LENGTH,
)
from .store import MqttStore
from .namespace import (
    validate_namespace_topic,
    validate_namespace_filter,
    is_namespace_topic,
    MAX_TOPIC_BYTES,
)
from .policy import Policy, Rule, Principal, PolicyError, load_policy_file, parse_policy
from .admission import (
    AdmissionEngine,
    AdmissionError,
    AdmissionInputError,
    AdmissionResourceLimit,
    TopicDecision,
    ScopeDecision,
    Witness,
)
from .controlled import ControlledIngress, ControlledSubscribeResult, FilterAdmission
from .capture_reconcile import (
    reconcile,
    load_manifest,
    load_capture,
    write_report,
    load_report,
    summarize_report,
    explain_report,
    verify_report,
    verify_scenario,
    find_counterpart,
    ReconcileError,
    ReconcileInputError,
    ReconcileResourceLimit,
    ReportExistsError,
    ReportError,
    CONTRACT as RECONCILE_CONTRACT,
)

__all__ = [
    "MqttIngress",
    "MqttStore",
    "ConnectFrame",
    "PublishFrame",
    "SubscribeFrame",
    "DisconnectFrame",
    "parse_frame",
    "FrameFormatError",
    "validate_publish_topic",
    "validate_filter",
    "topic_matches",
    "TopicError",
    "MAX_TOPIC_LENGTH",
    # 采集范围准入（受控入口）
    "validate_namespace_topic",
    "validate_namespace_filter",
    "is_namespace_topic",
    "MAX_TOPIC_BYTES",
    "Policy",
    "Rule",
    "Principal",
    "PolicyError",
    "load_policy_file",
    "parse_policy",
    "AdmissionEngine",
    "AdmissionError",
    "AdmissionInputError",
    "AdmissionResourceLimit",
    "TopicDecision",
    "ScopeDecision",
    "Witness",
    "ControlledIngress",
    "ControlledSubscribeResult",
    "FilterAdmission",
    # 捕获链对账
    "reconcile",
    "load_manifest",
    "load_capture",
    "write_report",
    "load_report",
    "summarize_report",
    "explain_report",
    "verify_report",
    "verify_scenario",
    "find_counterpart",
    "ReconcileError",
    "ReconcileInputError",
    "ReconcileResourceLimit",
    "ReportExistsError",
    "ReportError",
    "RECONCILE_CONTRACT",
]
