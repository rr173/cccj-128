"""可恢复流式加工作业的数据模型与有限无环图（DAG）校验。

图中的数据单元是 :class:`Contribution`（一份贡献）。它从 source 产生时
``route`` 为空，每经过一条边就把该边的唯一名字追加到 ``route`` 末尾；
广播到两条后继边会产生两份 route 不同的独立贡献，汇合时不去重。

图规格使用普通 JSON 可序列化的 dict 描述，例如::

    {
      "nodes": [
        {"name": "src-a", "kind": "source"},
        {"name": "sc", "kind": "scale", "factor": 10},
        {"name": "u", "kind": "union"},
        {"name": "sm", "kind": "sum"},
        {"name": "out", "kind": "sink"}
      ],
      "edges": [
        {"name": "e1", "from": "src-a", "to": "sc", "capacity": 2},
        {"name": "e2", "from": "sc", "to": "u", "capacity": 1}
      ]
    }

校验在任何业务读取之前完成：重名、环、悬空边、输入数错误、到不了 sink
的节点都会抛出 :class:`FlowGraphError`，坏图不能开始运行。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from ..topic import topic_matches, validate_filter

# 节点种类 -> 允许的输入边数约束。
# source 必须 0 条输入；union 至少 2 条；scale/sum/sink 恰好 1 条。
NODE_KINDS = ("source", "scale", "sum", "union", "sink")


class FlowGraphError(ValueError):
    """图规格非法。``reason`` 为机器可读原因码，``detail`` 为人读说明。"""

    def __init__(self, reason: str, detail: str):
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True)
class Contribution:
    """流经图的一份贡献。

    - ``source`` / ``seq``：来源名与原账本 seq，共同标识输入身份；
      不同 source 即便 seq 相同也是两条独立输入，相同字节的不同 seq 不合并。
    - ``amount``：当前整数值（payload 字节数，沿途被 scale 修改）。
    - ``origin_amount``：原始字节数，仅用于核对，不参与运算。
    - ``route``：经过的边名序列，source 产出时为空元组。
    """

    source: str
    seq: int
    topic: str
    amount: int
    route: tuple[str, ...] = field(default_factory=tuple)
    origin_amount: int = 0

    def to_json(self) -> str:
        return json.dumps(
            {
                "source": self.source,
                "seq": self.seq,
                "topic": self.topic,
                "amount": self.amount,
                "origin_amount": self.origin_amount,
                "route": list(self.route),
            },
            ensure_ascii=False,
        )

    @classmethod
    def from_json(cls, text: str) -> "Contribution":
        d = json.loads(text)
        return cls(
            source=d["source"],
            seq=int(d["seq"]),
            topic=d["topic"],
            amount=int(d["amount"]),
            origin_amount=int(d.get("origin_amount", d["amount"])),
            route=tuple(d["route"]),
        )

    def identity(self) -> tuple[str, int, tuple[str, ...]]:
        """sink 行身份：来源 + 原 seq + 完整 route。"""
        return self.source, self.seq, self.route


@dataclass(frozen=True)
class Barrier:
    """检查点屏障。屏障与贡献共用 FIFO 边，保证边内次序。"""

    cp_id: int


@dataclass(frozen=True)
class EndOfStream:
    """源耗尽标记，每条边至多一份，排在该边所有屏障与贡献之后。"""


# 边通道中允许出现的全部记录类型。
EdgeItem = Contribution | Barrier | EndOfStream


@dataclass(frozen=True)
class NodeSpec:
    name: str
    kind: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EdgeSpec:
    name: str
    src: str
    dst: str
    capacity: int


@dataclass(frozen=True)
class SourceSpec:
    """一个独立输入源：名称、只读源库路径、主题过滤器、起始位置（不含）。"""

    name: str
    path: str
    topic_filter: str
    start_after: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "path": self.path,
            "filter": self.topic_filter,
            "start_after": int(self.start_after),
        }


@dataclass(frozen=True)
class SourceBound:
    """建作业时固定下来的源读取区间 ``start_after < seq <= end_seq``。"""

    spec: SourceSpec
    end_seq: int


class FlowGraph:
    """校验后的不可变图对象，提供拓扑索引。"""

    def __init__(self, nodes: list[NodeSpec], edges: list[EdgeSpec]):
        self.nodes = {n.name: n for n in nodes}  # 构造时已查重
        self.edges = {e.name: e for e in edges}
        self.edge_order = [e.name for e in edges]
        self.incoming: dict[str, list[str]] = {n: [] for n in self.nodes}
        self.outgoing: dict[str, list[str]] = {n: [] for n in self.nodes}
        for e in edges:
            self.incoming[e.dst].append(e.name)
            self.outgoing[e.src].append(e.name)
        self.sources = sorted(n for n, s in self.nodes.items() if s.kind == "source")
        self.sinks = sorted(n for n, s in self.nodes.items() if s.kind == "sink")
        self.sum_nodes = sorted(n for n, s in self.nodes.items() if s.kind == "sum")

    # ------------------------------------------------------------ 构造/解析
    @classmethod
    def from_dict(cls, raw: Any) -> "FlowGraph":
        if not isinstance(raw, dict) or not isinstance(raw.get("nodes"), list) \
                or not isinstance(raw.get("edges"), list):
            raise FlowGraphError("GRAPH_SHAPE", "图规格必须包含 nodes 与 edges 两个列表")
        nodes: list[NodeSpec] = []
        seen: set[str] = set()
        for raw_node in raw["nodes"]:
            if not isinstance(raw_node, dict):
                raise FlowGraphError("NODE_SHAPE", f"节点规格必须是对象: {raw_node!r}")
            name = raw_node.get("name")
            kind = raw_node.get("kind")
            if not isinstance(name, str) or not name:
                raise FlowGraphError("NODE_NAME", f"节点名必须是非空字符串: {raw_node!r}")
            if name in seen:
                raise FlowGraphError("DUPLICATE_NODE", f"节点重名: {name!r}")
            seen.add(name)
            if kind not in NODE_KINDS:
                raise FlowGraphError(
                    "UNKNOWN_KIND",
                    f"节点 {name!r} 的 kind={kind!r} 不在 {NODE_KINDS} 内",
                )
            params = raw_node.get("params") or {}
            if not isinstance(params, dict):
                raise FlowGraphError("PARAM_SHAPE", f"节点 {name!r} 的 params 必须是对象")
            if kind == "scale":
                factor = params.get("factor")
                if isinstance(factor, bool) or not isinstance(factor, int):
                    raise FlowGraphError(
                        "SCALE_FACTOR",
                        f"scale 节点 {name!r} 需要整数 params.factor",
                    )
            nodes.append(NodeSpec(name=name, kind=kind, params=dict(params)))

        edges: list[EdgeSpec] = []
        edge_names: set[str] = set()
        for raw_edge in raw["edges"]:
            if not isinstance(raw_edge, dict):
                raise FlowGraphError("EDGE_SHAPE", f"边规格必须是对象: {raw_edge!r}")
            ename = raw_edge.get("name")
            if not isinstance(ename, str) or not ename:
                raise FlowGraphError("EDGE_NAME", f"边名必须是非空字符串: {raw_edge!r}")
            if ename in edge_names:
                raise FlowGraphError("DUPLICATE_EDGE", f"边重名: {ename!r}")
            edge_names.add(ename)
            src, dst = raw_edge.get("from"), raw_edge.get("to")
            if src not in seen or dst not in seen:
                raise FlowGraphError(
                    "DANGLING_EDGE",
                    f"边 {ename!r} 端点 {src!r}->{dst!r} 引用了不存在的节点",
                )
            capacity = raw_edge.get("capacity")
            if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity < 1:
                raise FlowGraphError(
                    "EDGE_CAPACITY",
                    f"边 {ename!r} 的 capacity 必须是正整数，实际 {capacity!r}",
                )
            edges.append(EdgeSpec(name=ename, src=src, dst=dst, capacity=capacity))

        graph = cls(nodes, edges)
        graph._validate()
        return graph

    def to_dict(self) -> dict[str, Any]:
        return {
            "nodes": [
                {"name": n.name, "kind": n.kind, "params": n.params}
                for n in self.nodes.values()
            ],
            "edges": [
                {"name": e.name, "from": e.src, "to": e.dst, "capacity": e.capacity}
                for e in self.edges.values()
            ],
        }

    # -------------------------------------------------------------- 校验
    def _validate(self) -> None:
        # 结构连通性优先：环与不可达节点是更基础的图错误，先于节点 arity 判定，
        # 避免“孤立算子”被报成输入数错误而掩盖“从 source 不可达”的真实问题。
        self._assert_acyclic()
        self._assert_reachable()
        for name, spec in self.nodes.items():
            indeg, outdeg = len(self.incoming[name]), len(self.outgoing[name])
            if spec.kind == "source":
                if indeg != 0:
                    raise FlowGraphError("SOURCE_INPUT", f"source 节点 {name!r} 不能有输入边")
                if outdeg == 0:
                    raise FlowGraphError("NO_OUTPUT", f"节点 {name!r} 没有任何后继边")
            elif spec.kind == "union":
                if indeg < 2:
                    raise FlowGraphError(
                        "UNION_ARITY", f"union 节点 {name!r} 至少需要两条输入边，实际 {indeg}"
                    )
                if outdeg == 0:
                    raise FlowGraphError("NO_OUTPUT", f"节点 {name!r} 没有任何后继边")
            elif spec.kind == "sink":
                if indeg != 1:
                    raise FlowGraphError(
                        "SINK_ARITY", f"sink 节点 {name!r} 必须恰好一条输入边，实际 {indeg}"
                    )
                if outdeg != 0:
                    raise FlowGraphError("SINK_OUTPUT", f"sink 节点 {name!r} 不能有输出边")
            else:  # scale / sum 普通算子
                if indeg != 1:
                    raise FlowGraphError(
                        "OPERATOR_ARITY",
                        f"{spec.kind} 节点 {name!r} 必须恰好一条输入边，实际 {indeg}",
                    )
                if outdeg == 0:
                    raise FlowGraphError("NO_OUTPUT", f"节点 {name!r} 没有任何后继边")

    def _assert_acyclic(self) -> None:
        # DFS 三色标记：gray 节点在当前递归栈上，再次遇到即有环。
        color: dict[str, int] = {n: 0 for n in self.nodes}

        def visit(name: str, stack: list[str]) -> None:
            color[name] = 1
            stack.append(name)
            for ename in self.outgoing[name]:
                nxt = self.edges[ename].dst
                if color[nxt] == 1:
                    cycle = " -> ".join(stack[stack.index(nxt):] + [nxt])
                    raise FlowGraphError("CYCLE", f"图中存在环: {cycle}")
                if color[nxt] == 0:
                    visit(nxt, stack)
            stack.pop()
            color[name] = 2

        for name in self.nodes:
            if color[name] == 0:
                visit(name, [])

    def _assert_reachable(self) -> None:
        # 从全部 source 前向遍历，未被访问的节点没有输入路径；
        # 再反向检查每个节点能否到达某个 sink。
        reached: set[str] = set()
        stack = list(self.sources)
        while stack:
            name = stack.pop()
            if name in reached:
                continue
            reached.add(name)
            for ename in self.outgoing[name]:
                stack.append(self.edges[ename].dst)
        unreachable = sorted(set(self.nodes) - reached)
        if unreachable:
            raise FlowGraphError(
                "UNREACHABLE_NODE", f"这些节点从任何 source 都不可达: {unreachable}"
            )
        can_sink: set[str] = set(self.sinks)
        stack = list(self.sinks)
        while stack:
            name = stack.pop()
            for ename in self.incoming[name]:
                upstream = self.edges[ename].src
                if upstream not in can_sink:
                    can_sink.add(upstream)
                    stack.append(upstream)
        dead = sorted(set(self.nodes) - can_sink)
        if dead:
            raise FlowGraphError("NO_SINK_PATH", f"这些节点到不了任何 sink: {dead}")

    def validate_source_names(self, source_names: list[str]) -> None:
        """图里的 source 节点集合必须与声明的数据源集合完全一致。"""
        declared = sorted(source_names)
        if declared != self.sources:
            raise FlowGraphError(
                "SOURCE_MISMATCH",
                f"图 source 节点 {self.sources} 与数据源声明 {declared} 不一致",
            )


def parse_sources(raw: Any) -> list[SourceSpec]:
    """解析并校验数据源声明列表。"""
    if not isinstance(raw, list) or not raw:
        raise FlowGraphError("SOURCE_SHAPE", "sources 必须是非空列表")
    specs: list[SourceSpec] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, dict):
            raise FlowGraphError("SOURCE_SHAPE", f"数据源规格必须是对象: {item!r}")
        name = item.get("name")
        path = item.get("path")
        topic_filter = item.get("filter")
        start_after = item.get("start_after", 0)
        if not isinstance(name, str) or not name:
            raise FlowGraphError("SOURCE_NAME", f"数据源名必须是非空字符串: {item!r}")
        if name in seen:
            raise FlowGraphError("DUPLICATE_SOURCE", f"数据源重名: {name!r}")
        seen.add(name)
        if not isinstance(path, str) or not path:
            raise FlowGraphError("SOURCE_PATH", f"数据源 {name!r} 的 path 必须是非空字符串")
        try:
            validate_filter(topic_filter)
        except Exception as exc:  # TopicError 等统一转成图错误
            raise FlowGraphError("SOURCE_FILTER", f"数据源 {name!r} 过滤器非法: {exc}") from exc
        if isinstance(start_after, bool) or not isinstance(start_after, int) or start_after < 0:
            raise FlowGraphError(
                "START_AFTER", f"数据源 {name!r} 的 start_after 必须是非负整数"
            )
        specs.append(SourceSpec(name=name, path=path, topic_filter=topic_filter,
                                start_after=start_after))
    return specs


def canonical_binding(graph: FlowGraph, bounds: list[SourceBound]) -> str:
    """计算作业绑定摘要：图、算子参数、源名/路径/过滤器/固定范围全部纳入。"""
    import hashlib

    payload = {
        "graph": graph.to_dict(),
        "sources": [
            {
                **b.spec.to_dict(),
                "end_seq": b.end_seq,
            }
            for b in sorted(bounds, key=lambda b: b.spec.name)
        ],
    }
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def row_matches_filter(spec: SourceSpec, topic: str) -> bool:
    return topic_matches(spec.topic_filter, topic)
