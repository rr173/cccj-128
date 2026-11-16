"""离线外发（按描述符删除字段）的错误模型。

所有失败都归为 :class:`ExportError`，携带机器可读 ``category`` 以及定位信息
（来源序号 ``event_seq``、字段号路径 ``field_path``、字节位置 ``byte_offset``）。
按合同，错误信息**不回显**正文或字段值，只给类别与位置。
"""

from __future__ import annotations

from typing import Optional, Tuple

# ---- 描述符 / 配置 / 来源 / 输出目标 -------------------------------------
DESCRIPTOR_ERROR = "descriptor_error"      # 描述符集非法或含不支持的特性
CONFIG_ERROR = "config_error"              # 配置文件非法（含删除路径非法）
LEDGER_ERROR = "ledger_error"              # 来源库打不开或结构不符
TARGET_EXISTS = "target_exists"            # 输出目录已存在
IO_ERROR = "io_error"                      # 实际写出失败
EXPORT_INCOMPLETE = "export_incomplete"    # 导出目录不完整（inspect/reader 用）

# ---- 线上字节核验（wire 层） ----------------------------------------------
UNKNOWN_FIELD = "unknown_field"            # 描述符不认识的字段号
INVALID_TAG = "invalid_tag"                # 字段号 0 / 越界 / 非法 wire type
INVALID_VARINT = "invalid_varint"          # varint 超过 64 位或过长
WRONG_WIRE_TYPE = "wrong_wire_type"        # 字段的 wire type 与描述符不符
TRUNCATED = "truncated"                    # varint/定长/长度界字段被截断
INVALID_LENGTH = "invalid_length"          # 长度前缀越出所在消息边界
INVALID_PACKED = "invalid_packed"          # packed 数值内容不合法
INVALID_UTF8 = "invalid_utf8"              # string 字段不是合法 UTF-8

# ---- 资源 ------------------------------------------------------------------
RESOURCE_LIMIT = "resource_limit"          # 字节数/嵌套深度/字段出现数超限

_INPUT_CATEGORIES = frozenset({
    DESCRIPTOR_ERROR, CONFIG_ERROR,
    UNKNOWN_FIELD, INVALID_TAG, INVALID_VARINT, WRONG_WIRE_TYPE,
    TRUNCATED, INVALID_LENGTH, INVALID_PACKED, INVALID_UTF8,
})

#: category -> CLI 退出码（0=成功；2=输入/正文非法；3=资源超限；4=目标已存在；
#: 5=来源/写出 IO 失败；6=导出目录不完整或核验不通过）。
CATEGORY_EXIT_CODES = {
    DESCRIPTOR_ERROR: 2,
    CONFIG_ERROR: 2,
    UNKNOWN_FIELD: 2,
    INVALID_TAG: 2,
    INVALID_VARINT: 2,
    WRONG_WIRE_TYPE: 2,
    TRUNCATED: 2,
    INVALID_LENGTH: 2,
    INVALID_PACKED: 2,
    INVALID_UTF8: 2,
    RESOURCE_LIMIT: 3,
    TARGET_EXISTS: 4,
    LEDGER_ERROR: 5,
    IO_ERROR: 5,
    EXPORT_INCOMPLETE: 6,
}


class ExportError(Exception):
    """外发失败。``field_path`` 是从根消息开始的字段号元组。"""

    def __init__(
        self,
        category: str,
        detail: str,
        *,
        event_seq: Optional[int] = None,
        field_path: Optional[Tuple[int, ...]] = None,
        byte_offset: Optional[int] = None,
        topic: Optional[str] = None,
    ):
        super().__init__(f"{category}: {detail}")
        self.category = category
        self.detail = detail
        self.event_seq = event_seq
        self.field_path = tuple(field_path) if field_path is not None else None
        self.byte_offset = byte_offset
        self.topic = topic

    @property
    def exit_code(self) -> int:
        return CATEGORY_EXIT_CODES.get(self.category, 1)

    def to_dict(self) -> dict:
        """机器可读、且不含正文/字段值的错误定位。"""
        out = {"category": self.category, "detail": self.detail}
        if self.event_seq is not None:
            out["event_seq"] = self.event_seq
        if self.topic is not None:
            out["topic"] = self.topic
        if self.field_path is not None:
            out["field_path"] = list(self.field_path)
        if self.byte_offset is not None:
            out["byte_offset"] = self.byte_offset
        return out
