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
]
