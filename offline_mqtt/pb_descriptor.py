"""FileDescriptorSet 载入、子集校验与删除路径编译。

只支持 **proto3** 描述符：普通标量、嵌套 message、repeated、packed 数值、
oneof、map 与 proto3 optional。明确拒绝：proto2（或未声明 syntax）、
extension/extension_range、group、required 字段、缺失依赖、同名符号冲突。

删除路径（``remove_paths``）是字段号数组，从根消息开始：
- 中间每一项必须是 message 类型字段（map 字段见其合成 entry 的规则）；
- 最后一项是要**整个删去**的字段；
- map 字段按描述符里的合成 entry 处理：字段 1 是 key、字段 2 是 value。
  可以删整个 map 字段（路径终止于 map 字段本身）；value 为 message 时可以
  沿 value 继续向下删（``[map, 2, ...]``），也可以把 value 字段整个删掉
  （``[map, 2]``）；**不允许单独删 map 的 key**（``[map, 1, ...]`` 一律拒绝），
  也不允许沿标量 value 继续向下。
- 路径必须在描述符图上真实存在（即使本批数据里没有相应消息）；
  穿过标量的路径、不存在的字段号一律拒绝。
"""

from __future__ import annotations

import hashlib
import warnings
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from . import _protobuf_vendor  # noqa: F401  保证 vendored protobuf 可导入

from google.protobuf import descriptor_pb2
from google.protobuf import descriptor_pool
from google.protobuf.descriptor import Descriptor, FieldDescriptor

from .pb_errors import CONFIG_ERROR, DESCRIPTOR_ERROR, ExportError

MAX_FIELD_NUMBER = (1 << 29) - 1  # 2^29 - 1，protobuf 字段号上限


# ---------------------------------------------------------------------------
# 描述符集载入
# ---------------------------------------------------------------------------

class LoadedDescriptors:
    """一份通过校验的描述符集：连接池 + 文件摘要 + 原始字节摘要。"""

    def __init__(self, pool: descriptor_pool.DescriptorPool,
                 file_summaries: List[dict], sha256: str):
        self.pool = pool
        self.file_summaries = file_summaries
        self.sha256 = sha256

    def find_message(self, full_name: str) -> Descriptor:
        try:
            return self.pool.FindMessageTypeByName(full_name)
        except KeyError:
            raise ExportError(
                CONFIG_ERROR, f"描述符集中不存在消息类型 {full_name!r}"
            ) from None


def _fail(detail: str) -> None:
    raise ExportError(DESCRIPTOR_ERROR, detail)


def _check_message_proto(fname: str, msg, trail: str) -> None:
    path = f"{trail}.{msg.name}" if trail else msg.name
    if msg.extension:
        _fail(f"{fname}: 消息 {path} 含 extension，不受支持")
    if msg.extension_range:
        _fail(f"{fname}: 消息 {path} 含 extension_range，不受支持")
    seen_numbers = set()
    seen_names = set()
    for fld in msg.field:
        # 纯 Python 连接池不校验字段号合法性与唯一性，必须自查
        if not (1 <= fld.number <= MAX_FIELD_NUMBER):
            _fail(f"{fname}: 字段 {path}.{fld.name} 的字段号 {fld.number} 越界")
        if 19000 <= fld.number <= 19999:
            _fail(f"{fname}: 字段 {path}.{fld.name} 的字段号 {fld.number} "
                  "落在 protobuf 保留段 19000-19999")
        if fld.number in seen_numbers:
            _fail(f"{fname}: 消息 {path} 的字段号 {fld.number} 重复")
        seen_numbers.add(fld.number)
        if fld.name in seen_names:
            _fail(f"{fname}: 消息 {path} 的字段名 {fld.name!r} 重复")
        seen_names.add(fld.name)
        if fld.type == descriptor_pb2.FieldDescriptorProto.TYPE_GROUP:
            _fail(f"{fname}: 字段 {path}.{fld.name} 是 group，不受支持")
        if fld.label == descriptor_pb2.FieldDescriptorProto.LABEL_REQUIRED:
            _fail(f"{fname}: 字段 {path}.{fld.name} 是 required（proto2 语义），不受支持")
    if msg.options.map_entry:
        nums = {f.number for f in msg.field}
        if nums != {1, 2}:
            _fail(f"{fname}: map entry {path} 必须有且仅有 key=1/value=2 两个字段")
    for nested in msg.nested_type:
        _check_message_proto(fname, nested, path)


def _topo_order(files) -> List:
    """按依赖关系拓扑排序；缺失依赖/循环依赖明确报错。"""
    by_name = {f.name: f for f in files}
    order: List = []
    state: Dict[str, int] = {}  # 0=未访问 1=访问中 2=完成

    def visit(f, stack):
        st = state.get(f.name, 0)
        if st == 2:
            return
        if st == 1:
            _fail(f"描述符依赖成环: {' -> '.join(stack + [f.name])}")
        state[f.name] = 1
        for dep in f.dependency:
            dep_file = by_name.get(dep)
            if dep_file is None:
                _fail(f"文件 {f.name!r} 依赖缺失: {dep!r}")
            visit(dep_file, stack + [f.name])
        state[f.name] = 2
        order.append(f)

    for f in files:
        visit(f, [])
    return order


def _collect_symbols(files):
    """自备符号表：同名冲突明确拒绝（纯 Python 连接池对此只发警告）。

    返回 (messages, enums)：集合中定义的消息/枚举全名，供类型引用核验。
    """
    symbols: Dict[str, str] = {}
    messages = set()
    enums = set()

    def add(full: str, fname: str) -> None:
        if full in symbols:
            _fail(f"同名符号冲突: {full!r}（{symbols[full]!r} 与 {fname!r}）")
        symbols[full] = fname

    for f in files:
        pkg = f.package

        def qualified(name: str) -> str:
            return f"{pkg}.{name}" if pkg else name

        def walk_msg(msg, trail: str) -> None:
            name = f"{trail}.{msg.name}" if trail else msg.name
            full = qualified(name)
            add(full, f.name)
            messages.add(full)
            for nested in msg.nested_type:
                walk_msg(nested, name)
            for enum in msg.enum_type:
                add(qualified(f"{name}.{enum.name}"), f.name)
                enums.add(qualified(f"{name}.{enum.name}"))

        for msg in f.message_type:
            walk_msg(msg, "")
        for enum in f.enum_type:
            full = qualified(enum.name)
            add(full, f.name)
            enums.add(full)
        for svc in f.service:
            add(qualified(svc.name), f.name)
    return messages, enums


def _check_type_refs(files, messages: set, enums: set) -> None:
    """目录中类型引用必须完整可解析（纯 Python 连接池是惰性解析，须自查）。"""
    def walk_msg(msg, trail: str, fname: str) -> None:
        path = f"{trail}.{msg.name}" if trail else msg.name
        for fld in msg.field:
            if fld.type in (fld.TYPE_MESSAGE, fld.TYPE_GROUP):
                target = fld.type_name.lstrip(".")
                if target not in messages:
                    _fail(
                        f"{fname}: 字段 {path}.{fld.name} 引用的消息类型 "
                        f"{fld.type_name!r} 在描述符集中不可解析"
                    )
            elif fld.type == fld.TYPE_ENUM:
                target = fld.type_name.lstrip(".")
                if target not in enums:
                    _fail(
                        f"{fname}: 字段 {path}.{fld.name} 引用的枚举类型 "
                        f"{fld.type_name!r} 在描述符集中不可解析"
                    )
        for nested in msg.nested_type:
            walk_msg(nested, path, fname)

    for f in files:
        for msg in f.message_type:
            walk_msg(msg, "", f.name)


def load_descriptor_set(data: bytes) -> LoadedDescriptors:
    """解析并校验二进制 FileDescriptorSet；不合法时抛 :class:`ExportError`。"""
    fds = descriptor_pb2.FileDescriptorSet()
    try:
        fds.ParseFromString(bytes(data))
    except Exception as exc:
        raise ExportError(DESCRIPTOR_ERROR, f"FileDescriptorSet 解析失败: {exc}") from None
    if not fds.file:
        _fail("描述符集为空")

    seen = set()
    for f in fds.file:
        if not f.name:
            _fail("存在无名文件")
        if f.name in seen:
            _fail(f"重复文件名 {f.name!r}")
        seen.add(f.name)
        if f.syntax != "proto3":
            _fail(f"文件 {f.name!r} syntax={f.syntax!r}：仅支持 proto3")
        if f.extension:
            _fail(f"文件 {f.name!r} 含顶层 extension，不受支持")
        for msg in f.message_type:
            _check_message_proto(f.name, msg, "")

    messages, enums = _collect_symbols(list(fds.file))
    _check_type_refs(list(fds.file), messages, enums)

    pool = descriptor_pool.DescriptorPool()
    summaries: List[dict] = []
    for f in _topo_order(list(fds.file)):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            try:
                pool.Add(f)
            except Exception as exc:
                _fail(f"文件 {f.name!r} 载入失败（依赖或同名符号冲突）: {exc}")
        conflicts = [w for w in caught if "onflict" in str(w.message)]
        if conflicts:
            _fail(f"文件 {f.name!r} 载入冲突: {conflicts[0].message}")
        msg_names = []

        def _collect(prefix, msgs):
            for m in msgs:
                full = f"{prefix}.{m.name}" if prefix else m.name
                msg_names.append(full)
                _collect(full, m.nested_type)

        _collect(f.package, f.message_type)
        summaries.append({
            "name": f.name,
            "package": f.package,
            "messages": msg_names,
            "dependencies": list(f.dependency),
        })
    return LoadedDescriptors(pool, summaries, hashlib.sha256(bytes(data)).hexdigest())


# ---------------------------------------------------------------------------
# 删除路径校验与编译
# ---------------------------------------------------------------------------

def is_map_field(field: FieldDescriptor) -> bool:
    """是否为 map 字段（repeated 合成 entry message）。"""
    return (
        field.type == FieldDescriptor.TYPE_MESSAGE
        and field.is_repeated
        and field.message_type is not None
        and field.message_type.GetOptions().map_entry
    )


class PlanNode:
    """删除计划树节点：本层要整删的字段号集合 + 需要下钻的子计划。"""

    __slots__ = ("delete", "children")

    def __init__(self) -> None:
        self.delete: set = set()
        self.children: Dict[int, "PlanNode"] = {}

    def is_noop(self) -> bool:
        return not self.delete and not self.children


EMPTY_PLAN = PlanNode()


def _config_fail(detail: str) -> None:
    raise ExportError(CONFIG_ERROR, detail)


def validate_remove_path(root: Descriptor, path: Sequence[int]) -> None:
    """按描述符图校验单条路径；非法时抛 CONFIG_ERROR。"""
    node = root
    n = len(path)
    i = 0
    while i < n:
        num = path[i]
        field = node.fields_by_number.get(num)
        shown = ".".join(str(x) for x in path[: i + 1])
        if field is None:
            _config_fail(
                f"删除路径 [{shown}] 在消息 {node.full_name} 中不存在字段号 {num}"
            )
        if i == n - 1:
            return  # 最后一项：整个字段删去，任何已存在的字段都合法
        # 中间项：必须是 message 字段
        if field.type != FieldDescriptor.TYPE_MESSAGE:
            _config_fail(
                f"删除路径 [{shown}] 穿过标量字段 {node.full_name}.{field.name}，"
                "中间项必须是 message 字段"
            )
        if is_map_field(field):
            entry = field.message_type
            nxt = path[i + 1]
            if nxt == 1:
                _config_fail(
                    f"删除路径 [{shown}.1] 试图单独删除 map 的 key，不受支持"
                )
            if nxt != 2:
                _config_fail(
                    f"删除路径 [{shown}.{nxt}] 越出 map entry（只有 key=1/value=2）"
                )
            value_field = entry.fields_by_number[2]
            if value_field.type != FieldDescriptor.TYPE_MESSAGE:
                _config_fail(
                    f"删除路径 [{shown}.2] 沿标量 map value 继续向下，不受支持"
                    "（可删整个 map 字段）"
                )
            if i + 1 == n - 1:
                return  # [map, 2]：把每个 entry 的 message value 字段整个删去
            node = value_field.message_type
            i += 2
        else:
            node = field.message_type
            i += 1


def normalize_paths(raw_paths: Iterable[Sequence[int]], root: Descriptor) -> List[Tuple[int, ...]]:
    """结构校验 + 描述符校验 + 去重排序。清单次序/重复不影响结果。"""
    normalized = set()
    for raw in raw_paths:
        if not isinstance(raw, (list, tuple)) or not raw:
            _config_fail(f"删除路径必须是非空字段号数组，实际: {raw!r}")
        path = []
        for item in raw:
            if isinstance(item, bool) or not isinstance(item, int):
                _config_fail(f"删除路径元素必须是整数字段号，实际: {item!r}")
            if not (1 <= item <= MAX_FIELD_NUMBER):
                _config_fail(f"删除路径字段号越界: {item}")
            path.append(item)
        path_t = tuple(path)
        validate_remove_path(root, path_t)
        normalized.add(path_t)
    return sorted(normalized)


def build_plan(paths: Iterable[Tuple[int, ...]]) -> PlanNode:
    """把规范化后的路径编译成计划树；父路径覆盖子路径。"""
    root = PlanNode()
    for path in sorted(paths, key=len):  # 先父后子
        node = root
        covered = False
        for i, num in enumerate(path):
            if num in node.delete:
                covered = True  # 祖先已整删，该子路径被覆盖
                break
            if i == len(path) - 1:
                node.delete.add(num)
                node.children.pop(num, None)  # 父路径覆盖先到的子路径
            else:
                nxt = node.children.get(num)
                if nxt is None:
                    nxt = PlanNode()
                    node.children[num] = nxt
                node = nxt
        if covered:
            continue
    return root
