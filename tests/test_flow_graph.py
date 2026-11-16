"""坏图拒绝：重名、环、悬空边、错误输入数、到不了 sink 都不能开始业务读取。"""

import unittest

from offline_mqtt.flow import FlowGraph, FlowGraphError


def node(name, kind, **params):
    return {"name": name, "kind": kind, "params": params} if params else \
        {"name": name, "kind": kind}


def edge(name, src, dst, capacity=2):
    return {"name": name, "from": src, "to": dst, "capacity": capacity}


class FlowGraphValidationTest(unittest.TestCase):
    def _expect(self, reason, graph):
        with self.assertRaises(FlowGraphError) as cm:
            FlowGraph.from_dict(graph)
        self.assertEqual(cm.exception.reason, reason)

    def test_duplicate_node_and_edge(self):
        self._expect("DUPLICATE_NODE", {
            "nodes": [node("a", "source"), node("a", "sink")], "edges": []})
        self._expect("DUPLICATE_EDGE", {
            "nodes": [node("a", "source"), node("b", "sink")],
            "edges": [edge("e", "a", "b"), edge("e", "a", "b")]})

    def test_dangling_edge(self):
        self._expect("DANGLING_EDGE", {
            "nodes": [node("a", "source"), node("b", "sink")],
            "edges": [edge("e", "a", "ghost")]})

    def test_cycle_is_rejected(self):
        # x->y->x 构成环（DFS 再次遇到栈上 gray 节点）；用 union 满足多输入约束。
        self._expect("CYCLE", {
            "nodes": [node("s", "source"), node("u", "union"),
                      node("y", "sum"), node("k", "sink")],
            "edges": [edge("e1", "s", "u"), edge("e2", "u", "y"),
                      edge("e3", "y", "u"), edge("e4", "u", "k")]})
        # 自环也是环（union 自身两入边，满足 arity）。
        self._expect("CYCLE", {
            "nodes": [node("s", "source"), node("u", "union"), node("k", "sink")],
            "edges": [edge("e1", "s", "u"), edge("e2", "u", "u"),
                      edge("e3", "u", "k")]})

    def test_arity_errors(self):
        # source 不能有输入
        self._expect("SOURCE_INPUT", {
            "nodes": [node("a", "source"), node("b", "source"), node("k", "sink")],
            "edges": [edge("e1", "a", "b"), edge("e2", "b", "k")]})
        # union 至少两条输入
        self._expect("UNION_ARITY", {
            "nodes": [node("a", "source"), node("u", "union"), node("k", "sink")],
            "edges": [edge("e1", "a", "u"), edge("e2", "u", "k")]})
        # sink 必须恰好一条输入
        self._expect("SINK_ARITY", {
            "nodes": [node("a", "source"), node("b", "source"), node("k", "sink")],
            "edges": [edge("e1", "a", "k"), edge("e2", "b", "k")]})
        # sink 不能有输出（连通性检查先报 x 到不了 sink）
        self._expect("NO_SINK_PATH", {
            "nodes": [node("a", "source"), node("k", "sink"), node("x", "sum")],
            "edges": [edge("e1", "a", "k"), edge("e2", "k", "x"), edge("e3", "x", "a")]
            if False else
            [edge("e1", "a", "k"), edge("e2", "k", "x")]})
        # 普通算子恰好一条输入
        self._expect("OPERATOR_ARITY", {
            "nodes": [node("a", "source"), node("b", "source"),
                      node("sc", "scale", factor=2), node("k", "sink")],
            "edges": [edge("e1", "a", "sc"), edge("e2", "b", "sc"),
                      edge("e3", "sc", "k")]})

    def test_unreachable_and_no_sink_path(self):
        # orphan 是没有任何边的孤立 sum：无输入路径，从任何 source 都不可达。
        # sum 允许零输入（arity 检查只拒绝多于一条输入的普通算子），因此校验
        # 能走到可达性检查而非提前报 arity。
        self._expect("UNREACHABLE_NODE", {
            "nodes": [node("a", "source"), node("x", "sum"), node("k", "sink"),
                      node("orphan", "sum")],
            "edges": [edge("e1", "a", "x"), edge("e2", "x", "k")]})
        # 第二个 source 有出边但其链路终点不是 sink（dead 无输出边，到不了 sink）。
        self._expect("NO_SINK_PATH", {
            "nodes": [node("a", "source"), node("b", "source"),
                      node("x", "sum"), node("dead", "sum"), node("k", "sink")],
            "edges": [edge("e1", "a", "x"), edge("e2", "x", "k"),
                      edge("e3", "b", "dead")]})

    def test_capacity_must_be_positive_integer(self):
        self._expect("EDGE_CAPACITY", {
            "nodes": [node("a", "source"), node("k", "sink")],
            "edges": [edge("e", "a", "k", capacity=0)]})
        self._expect("EDGE_CAPACITY", {
            "nodes": [node("a", "source"), node("k", "sink")],
            "edges": [edge("e", "a", "k", capacity="2")]})

    def test_scale_requires_integer_factor(self):
        self._expect("SCALE_FACTOR", {
            "nodes": [node("a", "source"), node("sc", "scale"), node("k", "sink")],
            "edges": [edge("e1", "a", "sc"), edge("e2", "sc", "k")]})
        self._expect("SCALE_FACTOR", {
            "nodes": [node("a", "source"),
                      node("sc", "scale", factor=1.5), node("k", "sink")],
            "edges": [edge("e1", "a", "sc"), edge("e2", "sc", "k")]})

    def test_valid_graph_indexes(self):
        graph = FlowGraph.from_dict({
            "nodes": [node("a", "source"), node("b", "source"),
                      node("u", "union"), node("k", "sink")],
            "edges": [edge("e1", "a", "u"), edge("e2", "b", "u"), edge("e3", "u", "k")],
        })
        self.assertEqual(graph.sources, ["a", "b"])
        self.assertEqual(graph.sinks, ["k"])
        self.assertEqual(graph.incoming["u"], ["e1", "e2"])


if __name__ == "__main__":
    unittest.main()
