"""终端入口（CLI）端到端：建作业、运行、控制请求、结果与诊断、退出码约定。"""

import json
import os
import unittest

from tests._flow_fixtures import (
    FlowTempDir,
    TOPIC,
    cli_run,
    make_standard_ledgers,
    standard_graph,
    standard_sources,
)
from offline_mqtt.flow import create_job


class FlowCliTest(FlowTempDir, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.led_a, self.led_b = make_standard_ledgers(self.tmp)
        self.graph_path = os.path.join(self.tmp, "graph.json")
        self.sources_path = os.path.join(self.tmp, "sources.json")
        with open(self.graph_path, "w", encoding="utf-8") as fh:
            json.dump(standard_graph(2), fh)
        with open(self.sources_path, "w", encoding="utf-8") as fh:
            json.dump(standard_sources(self.led_a, self.led_b), fh)

    def test_create_run_results_sums_diag(self):
        created = cli_run(["create", self.job_db, "job-1",
                           "--graph", self.graph_path, "--sources", self.sources_path])
        self.assertEqual(created.returncode, 0, created.stderr)
        body = json.loads(created.stdout)
        self.assertEqual(body["status"], "created")
        # 固定尾部 seq：甲 2、乙 1。
        bounds = {s["name"]: s["end_seq"] for s in body["sources"]}
        self.assertEqual(bounds, {"src-a": 2, "src-b": 1})

        ran = cli_run(["run", self.job_db, "job-1"])
        self.assertEqual(ran.returncode, 0, ran.stderr)
        self.assertEqual(json.loads(ran.stdout)["status"], "run_finished")

        results = cli_run(["results", self.job_db, "job-1", "--sink", "s1"])
        self.assertEqual(results.returncode, 0, results.stderr)
        rj = json.loads(results.stdout)
        self.assertEqual(rj["count"], 5)
        self.assertGreaterEqual(rj["cp_id"], 1)

        sums = cli_run(["sums", self.job_db, "job-1"])
        sj = json.loads(sums.stdout)
        self.assertEqual(sj["sums"]["sm"][TOPIC], {"count": 5, "total": 69})
        # sink 与 sum 同编号。
        self.assertEqual(rj["cp_id"], sj["cp_id"])

        status = cli_run(["status", self.job_db, "job-1"])
        st = json.loads(status.stdout)
        self.assertTrue(any(c["status"] == "complete" for c in st["checkpoints"]))

        diag = cli_run(["diag", self.job_db, "job-1"])
        dj = json.loads(diag.stdout)
        self.assertEqual(dj["recoverable_cp"], rj["cp_id"])
        self.assertIn("edge_watermark", dj["latest_snapshot"]["metrics"])

    def test_control_request_does_not_take_run_lock(self):
        create_job(self.job_db, "job-1", standard_graph(1),
                   standard_sources(self.led_a, self.led_b))
        # 没有运行者时控制请求也能插入（pending），证明它不获取执行锁。
        req = cli_run(["request", self.job_db, "job-1", "--timeout", "2"])
        # 超时退出码仍是 0 的输入类路径之外：请求已写入，等待受理超时返回错误码。
        self.assertIn(req.returncode, (0, 2))
        rj = json.loads(req.stdout)
        # 无论 pending 还是超时，请求行已落库。
        self.assertIn("status", rj)

    def test_bad_graph_returns_input_exit_code_before_run(self):
        bad_graph = os.path.join(self.tmp, "bad.json")
        with open(bad_graph, "w", encoding="utf-8") as fh:
            json.dump({"nodes": [{"name": "k", "kind": "sink"}], "edges": []}, fh)
        proc = cli_run(["create", self.job_db, "job-x",
                        "--graph", bad_graph, "--sources", self.sources_path])
        self.assertEqual(proc.returncode, 2)
        self.assertEqual(json.loads(proc.stdout)["status"], "error")

    def test_run_nonexistent_job_fails_cleanly(self):
        proc = cli_run(["run", os.path.join(self.tmp, "missing.db"), "nope"])
        self.assertIn(proc.returncode, (2, 4))


if __name__ == "__main__":
    unittest.main()
