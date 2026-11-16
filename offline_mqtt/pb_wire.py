"""Protobuf wire 层逐字段核验与按描述符计划的字节级删除。

字节保真合同：
- 未删除字段的原始 tag、数值编码（包括非规范但合法的 varint）、先后顺序与
  每次出现都逐字节保留；
- 命中删除的字段移除它的整段（tag + 长度前缀 + 内容）的**每一次线上出现**
  ——singular 字段先旧后新的多处出现、oneof 中后来被覆盖的成员、singular
  message 的多段出现，都按真实字段号逐段处理，不做解码器式合并；
- 只有内部发生删除的长度定界 message 才重算自己的长度前缀（规范 varint），
  未变化的容器连同原长度前缀逐字节不变；删除后变空的嵌套 message 仍保留
  父字段的存在（长度 0）；
- 任何一层出现描述符不认识的字段、非法 tag/varint/wire type/长度/packed/
  UTF-8，都拒绝本次外发，绝不静默放行未知数据。

本模块不做任何对象序列化；输出仅由输入的未删字节与重算的长度前缀拼成。
"""

from __future__ import annotations

from typing import Dict, List, NamedTuple, Optional, Tuple

from google.protobuf.descriptor import Descriptor, FieldDescriptor

from .pb_descriptor import EMPTY_PLAN, MAX_FIELD_NUMBER, PlanNode
from .pb_errors import (
    INVALID_LENGTH,
    INVALID_PACKED,
    INVALID_TAG,
    INVALID_UTF8,
    INVALID_VARINT,
    RESOURCE_LIMIT,
    TRUNCATED,
    UNKNOWN_FIELD,
    WRONG_WIRE_TYPE,
    ExportError,
)

# wire 类别（内部表示，与 protobuf wire type 区分开）
_KIND_VARINT = 0
_KIND_FIXED64 = 1
_KIND_LEN = 2
_KIND_FIXED32 = 5

_WIRE_OF_KIND = {
    _KIND_VARINT: 0,
    _KIND_FIXED64: 1,
    _KIND_LEN: 2,
    _KIND_FIXED32: 5,
}

_TYPE_TO_KIND = {
    FieldDescriptor.TYPE_INT32: _KIND_VARINT,
    FieldDescriptor.TYPE_INT64: _KIND_VARINT,
    FieldDescriptor.TYPE_UINT32: _KIND_VARINT,
    FieldDescriptor.TYPE_UINT64: _KIND_VARINT,
    FieldDescriptor.TYPE_SINT32: _KIND_VARINT,
    FieldDescriptor.TYPE_SINT64: _KIND_VARINT,
    FieldDescriptor.TYPE_BOOL: _KIND_VARINT,
    FieldDescriptor.TYPE_ENUM: _KIND_VARINT,
    FieldDescriptor.TYPE_FIXED64: _KIND_FIXED64,
    FieldDescriptor.TYPE_SFIXED64: _KIND_FIXED64,
    FieldDescriptor.TYPE_DOUBLE: _KIND_FIXED64,
    FieldDescriptor.TYPE_FIXED32: _KIND_FIXED32,
    FieldDescriptor.TYPE_SFIXED32: _KIND_FIXED32,
    FieldDescriptor.TYPE_FLOAT: _KIND_FIXED32,
    FieldDescriptor.TYPE_STRING: _KIND_LEN,
    FieldDescriptor.TYPE_BYTES: _KIND_LEN,
    FieldDescriptor.TYPE_MESSAGE: _KIND_LEN,
}

_PACKABLE_KINDS = (_KIND_VARINT, _KIND_FIXED32, _KIND_FIXED64)


class Limits(NamedTuple):
    """单条正文的资源上限（在分配或递归前检查）。"""

    max_bytes: int = 1 << 20        # 单条载荷最大字节数
    max_depth: int = 64             # message 嵌套最大深度
    max_occurrences: int = 1 << 16  # 单个 message 内字段出现数上限


DEFAULT_LIMITS = Limits()


class FieldSchema:
    __slots__ = ("number", "kind", "wire_types", "packed_kind", "sub", "is_string")

    def __init__(self, number, kind, wire_types, packed_kind, sub, is_string):
        self.number = number
        self.kind = kind
        self.wire_types = wire_types      # 该字段可接受的 wire type 集合
        self.packed_kind = packed_kind    # repeated 数值的 packed 内容类别，否则 None
        self.sub = sub                    # message 字段的子 schema，否则 None
        self.is_string = is_string


class MessageSchema:
    __slots__ = ("full_name", "fields")

    def __init__(self, full_name: str):
        self.full_name = full_name
        self.fields: Dict[int, FieldSchema] = {}


def build_schema(desc: Descriptor, cache: Optional[dict] = None) -> MessageSchema:
    """从描述符构建紧凑的 wire 校验 schema；递归消息通过占位缓存支持。"""
    if cache is None:
        cache = {}
    cached = cache.get(desc.full_name)
    if cached is not None:
        return cached
    schema = MessageSchema(desc.full_name)
    cache[desc.full_name] = schema  # 先占位，递归类型可回指
    for f in desc.fields:
        kind = _TYPE_TO_KIND[f.type]
        wire_types = frozenset({_WIRE_OF_KIND[kind]})
        packed_kind = None
        sub = None
        is_string = False
        if f.type == FieldDescriptor.TYPE_MESSAGE:
            sub = build_schema(f.message_type, cache)
        elif f.type == FieldDescriptor.TYPE_STRING:
            is_string = True
        if f.is_repeated and kind in _PACKABLE_KINDS:
            # repeated 可打包数值：packed 与 unpacked 两种表示都合法
            wire_types = frozenset({_WIRE_OF_KIND[kind], 2})
            packed_kind = kind
        schema.fields[f.number] = FieldSchema(
            f.number, kind, wire_types, packed_kind, sub, is_string
        )
    return schema


def canonical_varint(n: int) -> bytes:
    """规范（最短）varint 编码，只用于重算长度前缀。"""
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


class RewriteResult(NamedTuple):
    output: bytes                       # 输出字节（未变化时与输入同一对象）
    changed: bool
    intervals: List[Tuple[int, int]]    # 删除的原始半开字节区间（相对正文起点）


class _Ctx:
    __slots__ = ("data", "limits", "hits", "intervals", "event_seq")

    def __init__(self, data: bytes, limits: Limits, hits: Dict[Tuple[int, ...], int],
                 event_seq: Optional[int]):
        self.data = data
        self.limits = limits
        self.hits = hits
        self.intervals: List[Tuple[int, int]] = []
        self.event_seq = event_seq

    def fail(self, category: str, detail: str, field_path: Tuple[int, ...],
             offset: int) -> None:
        raise ExportError(
            category, detail,
            event_seq=self.event_seq, field_path=field_path, byte_offset=offset,
        )


def _read_varint(ctx: _Ctx, pos: int, end: int, path: Tuple[int, ...]) -> Tuple[int, int]:
    """读取一个 varint；截断/超 64 位明确报错。非规范但合法的编码原样接受。"""
    data = ctx.data
    result = 0
    shift = 0
    for i in range(10):
        if pos >= end:
            ctx.fail(TRUNCATED, "varint 在消息边界内被截断", path, pos)
        b = data[pos]
        pos += 1
        if i == 9 and b > 1:
            ctx.fail(INVALID_VARINT, "varint 超过 64 位", path, pos - 10)
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, pos
        shift += 7
    # 不可达：第 10 字节必含续位（>1），上面已拒绝
    raise AssertionError("unreachable")


def _validate_packed(ctx: _Ctx, fschema: FieldSchema, start: int, end: int,
                     path: Tuple[int, ...]) -> None:
    kind = fschema.packed_kind
    if kind == _KIND_VARINT:
        pos = start
        while pos < end:
            try:
                _, pos = _read_varint(ctx, pos, end, path)
            except ExportError as exc:
                raise ExportError(
                    INVALID_PACKED, f"packed varint 内容不合法（{exc.detail}）",
                    event_seq=ctx.event_seq, field_path=path,
                    byte_offset=exc.byte_offset,
                ) from None
    elif kind == _KIND_FIXED32:
        if (end - start) % 4:
            ctx.fail(INVALID_PACKED, "packed fixed32 长度不是 4 的倍数", path, start)
    else:  # _KIND_FIXED64
        if (end - start) % 8:
            ctx.fail(INVALID_PACKED, "packed fixed64 长度不是 8 的倍数", path, start)


def _rewrite(ctx: _Ctx, schema: MessageSchema, start: int, end: int,
             plan: PlanNode, depth: int, prefix: Tuple[int, ...]) -> Optional[bytes]:
    """核验 [start, end) 内的 message 字段并按 plan 删除。

    返回 None 表示该 message 内容逐字节不变；否则返回新内容字节。
    """
    data = ctx.data
    out: Optional[list] = None
    cursor = start
    pos = start
    occurrences = 0
    while pos < end:
        occurrences += 1
        if occurrences > ctx.limits.max_occurrences:
            ctx.fail(RESOURCE_LIMIT,
                     f"单个 message 字段出现数超过上限 {ctx.limits.max_occurrences}",
                     prefix, pos)
        field_start = pos
        tag, pos = _read_varint(ctx, pos, end, prefix)
        num = tag >> 3
        wtype = tag & 7
        if num == 0:
            ctx.fail(INVALID_TAG, "字段号 0 非法", prefix, field_start)
        if num > MAX_FIELD_NUMBER:
            ctx.fail(INVALID_TAG, f"字段号 {num} 越界", prefix, field_start)
        if wtype not in (0, 1, 2, 5):
            ctx.fail(INVALID_TAG, f"非法 wire type {wtype}", prefix + (num,), field_start)
        fschema = schema.fields.get(num)
        if fschema is None:
            ctx.fail(UNKNOWN_FIELD,
                     f"消息 {schema.full_name} 的描述符不认识字段号 {num}",
                     prefix + (num,), field_start)
        if wtype not in fschema.wire_types:
            ctx.fail(WRONG_WIRE_TYPE,
                     f"字段号 {num} 的 wire type {wtype} 与描述符不符",
                     prefix + (num,), field_start)

        # ---- 解析值，确定 [field_start, value_end) -------------------------
        content_start = content_end = -1
        if wtype == 0:
            _, value_end = _read_varint(ctx, pos, end, prefix + (num,))
        elif wtype == 1:
            if end - pos < 8:
                ctx.fail(TRUNCATED, "fixed64 被截断", prefix + (num,), pos)
            value_end = pos + 8
        elif wtype == 5:
            if end - pos < 4:
                ctx.fail(TRUNCATED, "fixed32 被截断", prefix + (num,), pos)
            value_end = pos + 4
        else:  # wtype == 2，长度定界
            len_start = pos
            length, pos = _read_varint(ctx, pos, end, prefix + (num,))
            if length > end - pos:
                ctx.fail(INVALID_LENGTH,
                         f"长度前缀 {length} 越出所在消息边界（剩 {end - pos} 字节）",
                         prefix + (num,), len_start)
            content_start = pos
            content_end = pos + length
            value_end = content_end
            if fschema.is_string:
                try:
                    data[content_start:content_end].decode("utf-8", "strict")
                except UnicodeDecodeError:
                    ctx.fail(INVALID_UTF8, "string 字段不是合法 UTF-8",
                             prefix + (num,), content_start)
            elif fschema.packed_kind is not None:
                _validate_packed(ctx, fschema, content_start, content_end,
                                 prefix + (num,))
        pos = value_end

        # ---- 删除 / 下钻 ---------------------------------------------------
        if num in plan.delete:
            if fschema.sub is not None:
                # 整段删除前仍须核验被删 message 的内容：任何一层非法都拒绝，
                # 不因该字段将被删掉而跳过核验（结果字节仍被移除）。
                if depth + 1 > ctx.limits.max_depth:
                    ctx.fail(RESOURCE_LIMIT,
                             f"嵌套深度超过上限 {ctx.limits.max_depth}",
                             prefix + (num,), field_start)
                _rewrite(ctx, fschema.sub, content_start, content_end,
                         EMPTY_PLAN, depth + 1, prefix + (num,))
            if out is None:
                out = []
            out.append(data[cursor:field_start])
            cursor = value_end
            ctx.intervals.append((field_start, value_end))
            hit_key = prefix + (num,)
            ctx.hits[hit_key] = ctx.hits.get(hit_key, 0) + 1
        elif fschema.sub is not None:
            # message 字段（含 map entry）：无论是否删除都要递归核验每一层
            if depth + 1 > ctx.limits.max_depth:
                ctx.fail(RESOURCE_LIMIT,
                         f"嵌套深度超过上限 {ctx.limits.max_depth}",
                         prefix + (num,), field_start)
            child_plan = plan.children.get(num)
            child = _rewrite(
                ctx, fschema.sub, content_start, content_end,
                child_plan if child_plan is not None else EMPTY_PLAN,
                depth + 1, prefix + (num,),
            )
            if child is not None:
                if out is None:
                    out = []
                out.append(data[cursor:field_start])
                out.append(data[field_start:len_start])  # 原 tag 字节
                out.append(canonical_varint(len(child)))  # 仅重算长度前缀
                out.append(child)
                cursor = value_end

    if out is None:
        return None
    out.append(data[cursor:end])
    return b"".join(out)


def rewrite_payload(schema: MessageSchema, payload: bytes, plan: PlanNode,
                    limits: Limits = DEFAULT_LIMITS,
                    hits: Optional[Dict[Tuple[int, ...], int]] = None,
                    *, event_seq: Optional[int] = None) -> RewriteResult:
    """核验整条正文并应用删除计划。

    ``hits`` 若给出，按规范化路径（字段号元组）累计命中的线上出现数。
    超限/非法时抛 :class:`ExportError`，不返回半条消息。
    """
    data = bytes(payload)
    if len(data) > limits.max_bytes:
        raise ExportError(
            RESOURCE_LIMIT,
            f"载荷 {len(data)} 字节超过上限 {limits.max_bytes}",
            event_seq=event_seq,
        )
    ctx = _Ctx(data, limits, hits if hits is not None else {}, event_seq)
    result = _rewrite(ctx, schema, 0, len(data), plan, 1, ())
    if result is None:
        return RewriteResult(data, False, ctx.intervals)
    return RewriteResult(result, True, ctx.intervals)
