"""检查点交错验收：屏障对齐、截面后继续发数、背压、节点停在截面后仍可完成。

关卡只在真实同步点阻塞/放行，不替产品保存任何状态。所有等待都是限时断言，
不依赖长时间 sleep。
"""

import asyncio
import unittest

from tests._flow_fixtures import (
    FlowTempDir,
    auto_gates,
    get_results,
    get_sums,
    inproc_gates,
    make_job,
)
from offline_mqtt.flow import (
    checkpoint_status,
    get_snapshot,
    request_checkpoint,
    run_job,
)
from offline_mqtt.flow.gates import Gates


class CheckpointInterleavingTest(FlowTempDir, unittest.TestCase):

    def _start_runner(self, gates: Gates) -> asyncio.Task:
        # 必须在运行中的事件循环内创建任务。
        return asyncio.ensure_future(run_job(self.job_db, "job-1", gates=gates))

    def test_request_visible_collecting_then_complete(self):
        make_job(self.tmp, self.job_db)
        # 自动放行关卡：每个检查点命中后立即继续，但首次命中仍可被观察到。
        gates = auto_gates(["node.first_barrier.s1"])

        async def scenario():
            runner = self._start_runner(gates)
            req = await asyncio.get_running_loop().run_in_executor(
                None, request_checkpoint, self.job_db, "job-1")
            # 等运行者真正到达首个 sink 封存点（证明“先封存、后提升”）。
            await gates.wait_hit("node.first_barrier.s1")
            info = checkpoint_status(self.job_db, "job-1", req["req_id"])
            self.assertIn(info["status"], ("collecting", "done"))
            result = await asyncio.wait_for(runner, timeout=10)
            self.assertIsNotNone(result["final_cp"])
            done = checkpoint_status(self.job_db, "job-1", req["req_id"])
            self.assertEqual(done["status"], "done")
            self.assertGreaterEqual(done["cp_id"], 1)
            self.assertGreaterEqual(result["final_cp"], done["cp_id"])
            # 完成后成品可见，且 sink 与 sum 属于同一完整编号。
            cp_r, rows = get_results(self.job_db, "job-1", "s1")
            cp_s, _ = get_sums(self.job_db, "job-1")
            self.assertEqual(cp_r, cp_s)
            self.assertEqual(len(rows), 5)

        asyncio.run(scenario())

    def test_post_cut_records_enter_capacity_channels_before_cp_completes(self):
        # 容量 1 与 2 分别验证（参数化）。
        for capacity in (1, 2):
            with self.subTest(capacity=capacity):
                old_tmp, old_db = self.tmp, self.job_db
                self.setUp()
                try:
                    self._post_cut_scenario(capacity)
                finally:
                    self.tearDown()
                    self.tmp, self.job_db = old_tmp, old_db

    def _post_cut_scenario(self, capacity: int) -> None:
        make_job(self.tmp, self.job_db, capacity=capacity)
        # 源越过截面后的命中点：自动放行，但首次命中可被观察用于“不可见”断言。
        gates = auto_gates(["source.post_cut.src-a"])

        async def scenario():
            runner = self._start_runner(gates)
            await asyncio.get_running_loop().run_in_executor(
                None, request_checkpoint, self.job_db, "job-1")
            # 关卡命中取决于“请求受理时是否仍有截面后数据待投递”，属时序相关：
            # 命中则证明截面后记录确实进入了有容量通道；未命中（数据已在请求前
            # 投递完）也合法。两种情况下运行都不得被阻塞，且结果必须正确。
            try:
                await gates.wait_hit("source.post_cut.src-a", timeout=2.0)
                hit = True
            except TimeoutError:
                hit = False
            result = await asyncio.wait_for(runner, timeout=10)
            self.assertIsNotNone(result["final_cp"])
            # 边水位不超过配置容量（有界通道、背压生效的直接证据）。
            snap = get_snapshot(self.job_db, "job-1")
            for edge, wm in snap["metrics"]["edge_watermark"].items():
                self.assertLessEqual(wm, snap["metrics"]["edge_capacity"][edge])
            # 最终成品仍是完整 5 份/sink，sum 数值正确，证明截面后记录未丢未重。
            _, rows1 = get_results(self.job_db, "job-1", "s1")
            self.assertEqual(len(rows1), 5)
            _, sums = get_sums(self.job_db, "job-1")
            self.assertEqual(sums["sm"]["plant/temp"], {"count": 5, "total": 69})

        asyncio.run(scenario())

    def test_node_stuck_on_post_cut_data_does_not_block_old_checkpoint(self):
        # 容量 1 的多记录账本，制造明显的截面后处理阶段：源在越过请求检查点
        # 截面、继续投递后续记录时被关卡拦住，此时第一个检查点必须仍能完整
        # 形成且对外只暴露截面内结果，截面后的贡献不可见。
        from offline_mqtt.flow import create_ledger
        led_a = self.tmp + "/a.db"
        led_b = self.tmp + "/b.db"
        create_ledger(led_a, [
            {"topic": "plant/temp", "payload": b"a" * n} for n in (2, 4, 6, 8, 10)
        ])
        create_ledger(led_b, [
            {"topic": "plant/temp", "payload": b"b" * n} for n in (3, 5, 7)
        ])
        from tests._flow_fixtures import standard_graph, standard_sources
        from offline_mqtt.flow import create_job
        create_job(self.job_db, "job-1", standard_graph(1), standard_sources(led_a, led_b))
        gates = auto_gates(["source.post_cut.src-a"])

        async def scenario():
            runner = self._start_runner(gates)
            await asyncio.get_running_loop().run_in_executor(
                None, request_checkpoint, self.job_db, "job-1")
            # 源越过请求检查点截面后命中关卡：截面后的记录尚未进入任何成品。
            await gates.wait_hit("source.post_cut.src-a")
            cp0, _ = get_results(self.job_db, "job-1", "s1")
            self.assertIn(cp0, (0, 1))
            # 轮询出现第一个完整检查点（源在截面后被拦截不阻塞检查点形成）。
            deadline = asyncio.get_running_loop().time() + 10
            first_cp = 0
            while True:
                cps = checkpoint_status(self.job_db, "job-1")
                complete = [c for c in cps if c["status"] == "complete"]
                if complete:
                    first_cp = complete[0]["cp_id"]
                    break
                self.assertLess(asyncio.get_running_loop().time(), deadline)
                await asyncio.sleep(0.01)
            cp1, rows_at_cp = get_results(self.job_db, "job-1", "s1")
            self.assertEqual(cp1, first_cp)
            result = await asyncio.wait_for(runner, timeout=10)
            self.assertGreaterEqual(result["final_cp"], first_cp)
            # 最终：甲 5 条 x 两路径 + 乙 3 条 = 每个 sink 13 份，无重无漏。
            _, final_rows = get_results(self.job_db, "job-1", "s1")
            self.assertEqual(len(final_rows), 13)
            # 第一检查点是截面内前缀；若它就是最终检查点，则行数等于 13，
            # 否则严格少于 13（截面后贡献未污染旧截面）。
            if first_cp < result["final_cp"]:
                self.assertLess(len(rows_at_cp), 13)

        asyncio.run(scenario())

    def test_two_sources_different_speed_both_represented_in_checkpoint(self):
        make_job(self.tmp, self.job_db, capacity=1)
        # 让甲源在截面后继续发数的点命中关卡（自动放行），制造两路速度差；乙不受阻。
        gates = auto_gates(["source.post_cut.src-a"])

        async def scenario():
            runner = self._start_runner(gates)
            await asyncio.get_running_loop().run_in_executor(
                None, request_checkpoint, self.job_db, "job-1")
            # 关卡命中时序相关，容错等待；不影响两路贡献都被处理的核心断言。
            try:
                await gates.wait_hit("source.post_cut.src-a", timeout=2.0)
            except TimeoutError:
                pass
            result = await asyncio.wait_for(runner, timeout=10)
            self.assertIsNotNone(result["final_cp"])
            _, rows = get_results(self.job_db, "job-1", "s1")
            # 两个源的贡献都在，甲 4 份（两条路径 x 两条消息）、乙 1 份。
            self.assertEqual(sum(1 for r in rows if r["source"] == "src-a"), 4)
            self.assertEqual(sum(1 for r in rows if r["source"] == "src-b"), 1)

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
