"""主题与主题过滤器校验、通配匹配。

规则（对齐 MQTT 3.1.1 4.7 节语义）：
- 主题按 ``/`` 分层；
- ``+`` 为单层通配，必须独占一整层（``a+`` 非法）；
- ``#`` 为多层通配，只能作为最后一层，且必须独占该层（``a#`` 非法），
  匹配零层或多层（``sport/#`` 可匹配 ``sport`` 本身）；
- 发布主题不允许出现任何通配符；
- 主题最大长度默认 65535 字节（MQTT 规范上限），可在接入器配置覆盖。
"""

from __future__ import annotations

MAX_TOPIC_LENGTH = 65535


class TopicError(ValueError):
    """主题或过滤器非法，携带机器可读的原因码。"""

    def __init__(self, reason: str, detail: str):
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


def _check_basic(topic: object, *, allow_wildcards: bool, max_length: int) -> str:
    if not isinstance(topic, str):
        raise TopicError("TOPIC_NOT_STRING", f"主题必须是字符串，实际类型 {type(topic).__name__}")
    if topic == "":
        raise TopicError("EMPTY_TOPIC", "空主题")
    if "\x00" in topic:
        raise TopicError("NUL_IN_TOPIC", "主题包含 NUL 字符")
    encoded = topic.encode("utf-8")
    if len(encoded) > max_length:
        raise TopicError(
            "TOPIC_TOO_LONG",
            f"主题长度 {len(encoded)} 字节超过上限 {max_length} 字节",
        )
    if not allow_wildcards and ("+" in topic or "#" in topic):
        raise TopicError("WILDCARD_IN_PUBLISH", f"发布主题不允许通配符: {topic!r}")
    return topic


def validate_publish_topic(topic: object, max_length: int = MAX_TOPIC_LENGTH) -> str:
    """校验发布主题：非空、无通配符、长度受限。返回原字符串。"""
    return _check_basic(topic, allow_wildcards=False, max_length=max_length)


def validate_filter(topic_filter: object, max_length: int = MAX_TOPIC_LENGTH) -> str:
    """校验订阅过滤器：通配符位置必须合法。"""
    topic = _check_basic(topic_filter, allow_wildcards=True, max_length=max_length)
    levels = topic.split("/")
    for index, level in enumerate(levels):
        if "#" in level:
            if level != "#" or index != len(levels) - 1:
                raise TopicError(
                    "ILLEGAL_MULTI_WILDCARD",
                    f"# 必须独占最后一层: {topic!r}",
                )
        if "+" in level and level != "+":
            raise TopicError(
                "ILLEGAL_SINGLE_WILDCARD",
                f"+ 必须独占一整层: {topic!r}",
            )
    return topic


def topic_matches(topic_filter: str, topic: str) -> bool:
    """判断具体主题是否匹配过滤器（两者均假定已通过校验）。"""
    filter_levels = topic_filter.split("/")
    topic_levels = topic.split("/")
    i = 0
    while i < len(filter_levels):
        fl = filter_levels[i]
        if fl == "#":
            # 多层通配：# 必为最后一层，匹配剩余任意层级（含零层）。
            return True
        if i >= len(topic_levels):
            return False
        if fl != "+" and fl != topic_levels[i]:
            return False
        i += 1
    return len(topic_levels) == len(filter_levels)
