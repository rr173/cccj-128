"""端到端正确性：身份多重集合、手写 route 清单、独立 sum 核对、空源与固定尾部。"""

import unittest

from tests._flow_fixtures import (
    FlowTempDir,
    TOPIC,
    assert_standard_sink_result,
    assert_sum_final,
    expected_identities,
    get_results,
    get_sums,
    identities,
    inproc_gates,
    make_job,
    make_standard_ledgers,
    run_inproc,
    standard_graph,
    standard_sources,
)
from offline_mqtt.flow import create_job, create_ledger, get_snapshot, job_diagnostics
from offline_mqtt.ingress import MqttIngress
from offline_mqtt.generator import connect, publish


class StandardScenarioTest(FlowTempDir, unittest.TestCase):
    def test_two_sinks_each_five_contributions_and_sum_69(self):
        make_job(self.tmp, self.job_db)
        result = run_inproc(self.job_db)
        self.assertEqual(result["final_cp"], 1)

        cp1, rows1 = get_results(self.job_db, "job-1", "s1")
        cp2, rows2 = get_results(self.job_db, "job-1", "s2")
        # 所有 sink 与算子状态属于同一次完整编号。
        self.assertEqual(cp1, cp2)
        cp_sum, sums = get_sums(self.job_db, "job-1")
        self.assertEqual(cp_sum, cp1)

        assert_standard_sink_result(self, rows1, "sm_s1")
        assert_standard_sink_result(self, rows2, "sm_s2")
        # 两 sink 合计恰好等于手写清单的 10 条身份，不多不少。
        self.assertEqual(identities(rows1) + identities(rows2),
                         identities([
                             {"source": s, "seq": q, "route": list(r)}
                             for (s, q, r) in expected_identities()
                         ]))
        assert_sum_final(self, sums)

        # 同号 seq 来自两个源时仍是两条独立输入（src-a/1 与 src-b/1 同时存在）。
        sources_present = {r["source"] for r in rows1}
        self.assertEqual(sources_present, {"src-a", "src-b"})

    def test_same_topic_two_rows_not_overwritten(self):
        make_job(self.tmp, self.job_db)
        run_inproc(self.job_db)
        _, rows = get_results(self.job_db, "job-1", "s1")
        a_rows = [r for r in rows if r["source"] == "src-a"]
        # 甲的同主题两条（seq 1、2）都在，没有被后者覆盖。
        self.assertEqual(sorted(r["seq"] for r in a_rows), [1, 1, 2, 2])

    def test_snapshot_source_positions_match_fixed_bounds(self):
        make_job(self.tmp, self.job_db)
        run_inproc(self.job_db)
        snap = get_snapshot(self.job_db, "job-1")
        # 源位置必须等于建作业时固定的结束 seq（甲 2、乙 1）。
        self.assertEqual(snap["sources"]["src-a"]["position"], 2)
        self.assertEqual(snap["sources"]["src-b"]["position"], 1)
        self.assertTrue(all(d["exhausted"] for d in snap["sources"].values()))
        diag = job_diagnostics(self.job_db, "job-1")
        # 实际读取的 seq 区间可核对，且没有重读。
        self.assertEqual(diag["latest_run"]["read_seqs"]["src-a"], [1, 2])
        self.assertEqual(diag["latest_run"]["read_seqs"]["src-b"], [1])

    def test_appended_rows_after_job_creation_are_not_read(self):
        led_a, led_b = make_standard_ledgers(self.tmp)
        create_job(self.job_db, "job-1", standard_graph(2), standard_sources(led_a, led_b))
        # 作业创建后源库追加新行：固定尾部之后不得重新取尾部。
        with MqttIngress(led_a) as ing:
            ing.ingest(publish("flow-seed", TOPIC, b"zzzzzzzz", qos=1, message_id=99))
        run_inproc(self.job_db)
        _, rows = get_results(self.job_db, "job-1", "s1")
        self.assertEqual(len(rows), 5)  # 仍是 5 份，追加的长度 8 不进入
        snap = get_snapshot(self.job_db, "job-1")
        self.assertEqual(snap["sources"]["src-a"]["position"], 2)

    def test_start_after_excludes_prefix(self):
        led_a, led_b = make_standard_ledgers(self.tmp)
        create_job(self.job_db, "job-1", standard_graph(2),
                   standard_sources(led_a, led_b, start_after=1))
        run_inproc(self.job_db)
        _, rows = get_results(self.job_db, "job-1", "s1")
        # 甲只剩 seq=2（两条路径），乙 1 条 -> 3 份贡献。
        self.assertEqual(len(rows), 3)
        self.assertTrue(all(r["seq"] != 1 or r["source"] != "src-a" for r in rows))


class EmptyAndDuplicateInputTest(FlowTempDir, unittest.TestCase):
    def _build(self, graph, sources):
        create_job(self.job_db, "job-1", graph, sources)

    def test_empty_source_finishes_and_final_checkpoint_completes(self):
        empty_ledger = self.tmp + "/empty.db"
        create_ledger(empty_ledger, [])  # 只有会话，没有 ledger 行
        full_ledger = self.tmp + "/full.db"
        create_ledger(full_ledger, [{"topic": "t", "payload": b"abcd"}])
        graph = {
            "nodes": [
                {"name": "empty", "kind": "source"},
                {"name": "full", "kind": "source"},
                {"name": "u", "kind": "union"},
                {"name": "k", "kind": "sink"},
            ],
            "edges": [
                {"name": "e1", "from": "empty", "to": "u", "capacity": 1},
                {"name": "e2", "from": "full", "to": "u", "capacity": 1},
                {"name": "e3", "from": "u", "to": "k", "capacity": 1},
            ],
        }
        sources = [
            {"name": "empty", "path": empty_ledger, "filter": "#", "start_after": 0},
            {"name": "full", "path": full_ledger, "filter": "#", "start_after": 0},
        ]
        self._build(graph, sources)
        result = run_inproc(self.job_db)
        self.assertIsNotNone(result["final_cp"])
        cp, rows = get_results(self.job_db, "job-1")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["source"], "full")
        snap = get_snapshot(self.job_db, "job-1")
        self.assertTrue(snap["sources"]["empty"]["exhausted"])
        self.assertEqual(snap["sources"]["empty"]["position"], 0)

    def test_duplicate_byte_messages_are_not_merged(self):
        ledger = self.tmp + "/dup.db"
        # 三条完全相同字节、相同主题、不同 seq 的消息。
        create_ledger(ledger, [
            {"topic": "t", "payload": b"aa"},
            {"topic": "t", "payload": b"aa"},
            {"topic": "t", "payload": b"aa"},
        ])
        graph = {
            "nodes": [{"name": "s", "kind": "source"}, {"name": "k", "kind": "sink"}],
            "edges": [{"name": "e", "from": "s", "to": "k", "capacity": 1}],
        }
        sources = [{"name": "s", "path": ledger, "filter": "#", "start_after": 0}]
        self._build(graph, sources)
        run_inproc(self.job_db)
        cp, rows = get_results(self.job_db, "job-1")
        self.assertEqual([r["seq"] for r in rows], [1, 2, 3])
        self.assertEqual([r["amount"] for r in rows], [2, 2, 2])

    def test_broadcast_then_union_duplicates_contributions(self):
        ledger = self.tmp + "/bc.db"
        create_ledger(ledger, [{"topic": "t", "payload": b"abc"}])
        # 一条输入广播成两条边再汇合：sink 应有两份不同 route 的贡献。
        graph = {
            "nodes": [
                {"name": "s", "kind": "source"},
                {"name": "u", "kind": "union"},
                {"name": "k", "kind": "sink"},
            ],
            "edges": [
                {"name": "p1", "from": "s", "to": "u", "capacity": 1},
                {"name": "p2", "from": "s", "to": "u", "capacity": 1},
                {"name": "out", "from": "u", "to": "k", "capacity": 1},
            ],
        }
        sources = [{"name": "s", "path": ledger, "filter": "#", "start_after": 0}]
        self._build(graph, sources)
        run_inproc(self.job_db)
        _, rows = get_results(self.job_db, "job-1")
        self.assertEqual(len(rows), 2)  # 广播后汇合，不去重
        self.assertEqual({tuple(r["route"]) for r in rows},
                         {("p1", "out"), ("p2", "out")})


if __name__ == "__main__":
    unittest.main()
