"""采集命名空间的严格主题/过滤器校验。

这是本次新增“范围准入”能力使用的独立词汇表，**刻意窄于**旧入口
（:mod:`offline_mqtt.topic`）所支持的通用 MQTT 主题语法。旧的不受控入口
支持范围不缩小；受控入口只接受这里定义的采集命名空间：

- 每层只由小写 ASCII 字母、数字、下划线、连字符组成（``[a-z0-9_-]+``）且非空；
- 层间用 ``/`` 分隔；
- 完整具体主题按 UTF-8 编码不超过 :data:`MAX_TOPIC_BYTES` 字节；
- 授权/请求过滤器的每层可以是上述字面量或 ``+``，最后一层还可以是 ``#``；
  ``+`` 恰好占一层，``#`` 占零层或更多层（``plant/#`` 包含 ``plant``）；
- 不接受空层、系统主题（``$`` 前缀）、共享订阅前缀（``$share/...``）或其他
  通配写法（如 ``a+``、``a#``）。

具体主题与过滤器共用同一套层级字符规则，区别只在通配符是否允许出现。
"""

from __future__ import annotations

import re

from .topic import TopicError

MAX_TOPIC_BYTES = 65535

# 单层字面量：小写 ASCII 字母 / 数字 / 下划线 / 连字符，至少一个字符。
_LEVEL_RE = re.compile(r"^[a-z0-9_-]+$")


def _check_bytes(text: object) -> str:
    if not isinstance(text, str):
        raise TopicError(
            "TOPIC_NOT_STRING",
            f"主题必须是字符串，实际类型 {type(text).__name__}",
        )
    if text == "":
        raise TopicError("EMPTY_TOPIC", "空主题")
    if "\x00" in text:
        raise TopicError("NUL_IN_TOPIC", "主题包含 NUL 字符")
    encoded = text.encode("utf-8")
    if len(encoded) > MAX_TOPIC_BYTES:
        raise TopicError(
            "TOPIC_TOO_LONG",
            f"主题长度 {len(encoded)} 字节超过采集命名空间上限 "
            f"{MAX_TOPIC_BYTES} 字节",
        )
    return text


def _check_literal_level(level: str, location: str) -> None:
    """校验一个非通配层；location 用于在策略/请求里指出位置。"""
    if level == "":
        raise TopicError("EMPTY_LEVEL", f"{location} 为空层（不允许连续/首尾 '/'）")
    if level.startswith("$"):
        # 系统主题与共享订阅前缀（$share/...、$SYS/...）都在禁止之列。
        raise TopicError(
            "SYS_OR_SHARED_PREFIX",
            f"{location}={level!r} 以 '$' 开头：系统主题/共享订阅前缀不属于采集命名空间",
        )
    if not _LEVEL_RE.match(level):
        raise TopicError(
            "INVALID_LEVEL_CHAR",
            f"{location}={level!r} 含非法字符：采集命名空间每层只允许 "
            "[a-z0-9_-] 且非空",
        )


def validate_namespace_topic(topic: object) -> str:
    """校验采集命名空间内的**具体主题**（不允许任何通配符），返回原字符串。"""
    text = _check_bytes(topic)
    levels = text.split("/")
    for index, level in enumerate(levels):
        if level in ("+", "#"):
            raise TopicError(
                "WILDCARD_IN_PUBLISH",
                f"具体主题不允许通配符: levels[{index}]={level!r}",
            )
        _check_literal_level(level, f"levels[{index}]")
    return text


def validate_namespace_filter(topic_filter: object) -> str:
    """校验采集命名空间内的主题过滤器（仅 ``+`` 与末层 ``#`` 合法）。"""
    text = _check_bytes(topic_filter)
    levels = text.split("/")
    last = len(levels) - 1
    for index, level in enumerate(levels):
        if level == "+":
            continue
        if level == "#":
            if index != last:
                raise TopicError(
                    "ILLEGAL_MULTI_WILDCARD",
                    f"# 必须独占最后一层: levels[{index}] 后仍有层级",
                )
            continue
        if "+" in level or "#" in level:
            code = "ILLEGAL_MULTI_WILDCARD" if "#" in level else "ILLEGAL_SINGLE_WILDCARD"
            raise TopicError(
                code,
                f"通配符必须独占一整层: levels[{index}]={level!r}",
            )
        _check_literal_level(level, f"levels[{index}]")
    return text


def is_namespace_topic(topic: object) -> bool:
    """宽松判断：对象是否为合法采集命名空间具体主题（不抛异常）。

    受控读取用它把库中其他用途的主题（大写主题、``$`` 前缀、空层等）挡在
    受控结果之外。
    """
    try:
        validate_namespace_topic(topic)
    except TopicError:
        return False
    return True
