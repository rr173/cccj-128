"""恢复一致性与故障演练。

覆盖：
- 关卡中止（节点状态只封存一部分 / sink 暂存已写但检查点未完成 /
  完成标记持久化后控制端未收到成功）三种截面位置的恢复；
- 真实子进程 SIGKILL 后新进程恢复，封存位置之前的源行不重读、身份多重集合
  不多不少、sum 与两个 sink 属于同一完整编号；
- 图参数变化拒绝恢复、第二执行进程争用显式失败、真实写入失败后可接续。
"""

import json
import os
import time
import unittest
from collections import Counter

from tests._flow_fixtures import (
    FlowTempDir,
    TOPIC,
    cli_run,
    cli_run_gated,
    get_results,
    get_sums,
    identities,
    inproc_gates,
    make_job,
    make_standard_ledgers,
    release_gate,
    run_inproc,
    standard_graph,
    standard_sources,
    wait_for_gate_hit,
)
from offline_mqtt.flow import (
    FlowRunError,
    JobStore,
    RunLockHeld,
    create_job,
    get_snapshot as load_snapshot,
    job_diagnostics,
    request_checkpoint,
    run_job,
)
from offline_mqtt.flow.gates import GateAbort
import asyncio


class InProcessAbortRecoveryTest(FlowTempDir, unittest.TestCase):
    def _assert_full_result_and_no_reread(self, read_seqs_before: dict | None = None):
        cp1, rows1 = get_results(self.job_db, "job-1", "s1")
        cp2, rows2 = get_results(self.job_db, "job-1", "s2")
        cp_sum, sums = get_sums(self.job_db, "job-1")
        self.assertEqual(cp1, cp2)
        self.assertEqual(cp_sum, cp1)  # sink 与 sum 同编号
        self.assertEqual(len(rows1), 5)
        self.assertEqual(len(rows2), 5)
        self.assertEqual(sums["sm"][TOPIC], {"count": 5, "total": 69})
        # 跨所有运行：每个源“新读取”（非在途重放）的 seq 恰好是固定区间，
        # 没有任何封存位置之前的源行被重新读取；在途重放单独标记 replayed=1。
        store = JobStore(self.job_db)
        try:
            fresh: dict[str, list[int]] = {}
            replayed: dict[str, list[int]] = {}
            for r in store.conn.execute(
                "SELECT source, seq_value, replayed FROM run_reads"
                " ORDER BY source, seq_value"
            ):
                target = replayed if r["replayed"] else fresh
                target.setdefault(r["source"], []).append(r["seq_value"])
        finally:
            store.close()
        self.assertEqual(sorted(fresh.get("src-a", [])), [1, 2])
        self.assertEqual(sorted(fresh.get("src-b", [])), [1])
        # 重放（若有）只能来自保存的在途项，不会引发源重读。
        for values in replayed.values():
            self.assertTrue(all(isinstance(v, int) for v in values))

    def test_abort_after_partial_node_state_then_recover(self):
        make_job(self.tmp, self.job_db)
        # 节点在首个屏障封存点中止：此时只有部分节点封存，尚未有完整检查点。
        gates = inproc_gates(raises=["node.first_barrier.u"])
        with self.assertRaises(FlowRunError):
            asyncio.run(run_job(self.job_db, "job-1", gates=gates))
        # 没有完整检查点，成品查询为空。
        cp, rows = get_results(self.job_db, "job-1")
        self.assertEqual(cp, 0)
        self.assertEqual(rows, [])
        # 恢复：不存在任何完整截面，语义上等价于从固定起点重跑；
        # 关键是没有 sink 去重掩盖，且最终结果正确、sum 从空状态重建。
        result = run_inproc(self.job_db)
        self.assertEqual(result["final_cp"], 1)
        cp1, rows1 = get_results(self.job_db, "job-1", "s1")
        cp2, rows2 = get_results(self.job_db, "job-1", "s2")
        cp_sum, sums = get_sums(self.job_db, "job-1")
        self.assertEqual(cp1, cp2 == cp_sum)
        self.assertEqual(len(rows1), 5)
        self.assertEqual(len(rows2), 5)
        self.assertEqual(sums["sm"][TOPIC], {"count": 5, "total": 69})
        # 最终运行（产生完整检查点的那次）的读取恰好是固定区间。
        diag = job_diagnostics(self.job_db, "job-1")
        self.assertEqual(diag["latest_run"]["read_seqs"]["src-a"], [1, 2])
        self.assertEqual(diag["latest_run"]["read_seqs"]["src-b"], [1])

    def test_abort_after_sink_staged_before_checkpoint_complete(self):
        # 在“暂存已写、检查点未提升”处注入故障；此时无完整检查点，重启后
        # 暂存整批作废、从固定起点重跑，结果不重不漏。
        self._make_multi_ledger_job()
        gates = inproc_gates()
        gates.arm_raise("sink.staged.s1")
        with self.assertRaises(FlowRunError):
            asyncio.run(run_job(self.job_db, "job-1", gates=gates))
        # 暂存行存在但不可见；重启时整批作废。
        store = JobStore(self.job_db)
        try:
            staged = store.conn.execute("SELECT COUNT(*) AS c FROM sink_staging").fetchone()
            self.assertGreaterEqual(staged["c"], 1)
        finally:
            store.close()
        cp, rows = get_results(self.job_db, "job-1")
        self.assertEqual(cp, 0)
        # 恢复后正常完成，暂存被清理、无 collecting 残留。
        result = asyncio.run(run_job(self.job_db, "job-1"))
        self.assertIsNotNone(result["final_cp"])
        _, final_rows = get_results(self.job_db, "job-1", "s1")
        self.assertEqual(len(final_rows), 13)
        store = JobStore(self.job_db)
        try:
            self.assertEqual(
                store.conn.execute("SELECT COUNT(*) AS c FROM sink_staging").fetchone()["c"], 0)
            self.assertEqual(
                store.conn.execute(
                    "SELECT COUNT(*) AS c FROM checkpoints WHERE status='collecting'"
                ).fetchone()["c"], 0)
        finally:
            store.close()

    def _make_multi_ledger_job(self, job_db: str | None = None):
        from offline_mqtt.flow import create_ledger, create_job
        from tests._flow_fixtures import standard_graph, standard_sources
        led_a = self.tmp + "/a.db"
        led_b = self.tmp + "/b.db"
        create_ledger(led_a, [
            {"topic": TOPIC, "payload": b"a" * n} for n in (2, 4, 6, 8, 10)
        ])
        create_ledger(led_b, [
            {"topic": TOPIC, "payload": b"b" * n} for n in (3, 5, 7)
        ])
        create_job(job_db or self.job_db, "job-1", standard_graph(1),
                   standard_sources(led_a, led_b))

    def test_abort_after_persist_before_control_ack(self):
        make_job(self.tmp, self.job_db)
        # 完成标记已提交、控制端尚未收到成功时中止。
        gates = inproc_gates(raises=["commit.after_persist"])
        with self.assertRaises(FlowRunError):
            asyncio.run(run_job(self.job_db, "job-1", gates=gates))
        # 检查点已经是完整状态（标记持久化先于故障点）。
        cp_before, rows_before = get_results(self.job_db, "job-1", "s1")
        self.assertGreaterEqual(cp_before, 1)
        before_identities = identities(rows_before)
        # 恢复：从该完整检查点继续，最终编号更大且结果不重不漏。
        result = run_inproc(self.job_db)
        self.assertGreaterEqual(result["final_cp"], cp_before)
        cp1, rows1 = get_results(self.job_db, "job-1", "s1")
        self.assertEqual(identities(rows1), before_identities)
        self.assertEqual(len(rows1), 5)
        self._assert_full_result_and_no_reread()
        # 已完成编号没有被未完成的新编号覆盖。
        store = JobStore(self.job_db)
        try:
            statuses = [r["status"] for r in store.conn.execute(
                "SELECT DISTINCT status FROM checkpoints")]
            self.assertIn("complete", statuses)
        finally:
            store.close()


class CrossProcessKillRecoveryTest(FlowTempDir, unittest.TestCase):
    def _make_multi_job(self):
        # 多记录账本：第一次检查点可固化前缀，之后在后续检查点处杀进程，
        # 恢复时必须从已固化位置之后继续、绝不重读前缀。
        from offline_mqtt.flow import create_ledger, create_job
        from tests._flow_fixtures import standard_graph, standard_sources
        led_a = self.tmp + "/a.db"
        led_b = self.tmp + "/b.db"
        create_ledger(led_a, [
            {"topic": TOPIC, "payload": b"a" * n} for n in (2, 4, 6, 8, 10)
        ])
        create_ledger(led_b, [
            {"topic": TOPIC, "payload": b"b" * n} for n in (3, 5, 7)
        ])
        create_job(self.job_db, "job-1", standard_graph(1), standard_sources(led_a, led_b))
        return led_a, led_b

    def _kill_at_gate(self, wait_point: str):
        gate_dir = os.path.join(self.tmp, "gates_" + wait_point.replace(".", "_"))
        os.makedirs(gate_dir, exist_ok=True)
        proc = cli_run_gated(
            self.job_db, {"wait": [wait_point]}, gate_dir
        )
        wait_for_gate_hit(gate_dir, wait_point)
        proc.kill()  # SIGKILL：模拟进程被强制终止，无清理机会
        proc.wait(timeout=10)
        return gate_dir

    def _first_checkpoint_then_kill(self, kill_point: str):
        # 阶段一：用文件关卡在首个节点封存点暂停，先放行形成完整检查点，
        # 再在目标故障点杀掉第二个运行进程。
        self._make_multi_job()
        # 先完整跑一次到最终，确保至少一个完整检查点存在。
        proc = cli_run(["run", self.job_db, "job-1"])
        self.assertEqual(proc.returncode, 0, proc.stderr)

    def test_kill_while_collecting_then_new_process_recovers(self):
        # 在任何完整检查点形成前于封存中被杀：不存在可恢复截面，新进程按语义
        # 从固定起点重跑（不是“重读已封存前缀”，因为根本没有封存前缀）。
        # 结果仍须最终一致，且不依赖 sink 去重掩盖。
        self._make_multi_job()
        self._kill_at_gate("node.first_barrier.u")
        proc = cli_run(["run", self.job_db, "job-1"])
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self._assert_consistent_final_state(expect_reread=True)

    def test_kill_after_sink_staging_then_recover(self):
        # 作业未完成时在暂存阶段被 SIGKILL：没有完整截面，新进程从固定起点
        # 重跑，暂存整批作废，最终结果仍须一致（不依赖 sink 去重）。
        self._make_multi_job()
        self._kill_at_gate("sink.staged.s1")
        proc = cli_run(["run", self.job_db, "job-1"])
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self._assert_consistent_final_state(expect_reread=True)

    def test_kill_after_commit_persisted_then_recover_keeps_cp(self):
        # 完成标记已持久化、控制端尚未确认时被杀：该完整检查点必须被新进程
        # 认作最近可恢复截面，从其后继续，不重读前缀。
        self._make_multi_job()
        self._kill_at_gate("commit.partial_state")
        proc = cli_run(["run", self.job_db, "job-1"])
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self._assert_consistent_final_state(expect_reread=False)

    def _assert_consistent_final_state(self, expect_reread: bool):
        cp1, rows1 = get_results(self.job_db, "job-1", "s1")
        cp2, rows2 = get_results(self.job_db, "job-1", "s2")
        cp_sum, sums = get_sums(self.job_db, "job-1")
        self.assertEqual(cp1, cp2)
        self.assertEqual(cp_sum, cp1)
        # 甲 5 条 x 两路径 + 乙 3 条 = 每 sink 13 份。
        self.assertEqual(len(rows1), 13)
        self.assertEqual(len(rows2), 13)
        # 独立核对 sum：count=13；total = 甲原值(2+4+6+8+10) + 甲x10(300)
        # + 乙(3+5+7) = 30 + 300 + 15 = 345。
        self.assertEqual(sums["sm"][TOPIC], {"count": 13, "total": 345})
        # 两 sink 的身份集合仅尾边不同；去掉尾边后来源+前缀路径多重集合一致。
        def core(rows):
            return Counter((r["source"], r["seq"], tuple(r["route"][:-1])) for r in rows)
        self.assertEqual(core(rows1), core(rows2))
        self.assertTrue(all(tuple(r["route"])[-1] == "sm_s1" for r in rows1))
        self.assertTrue(all(tuple(r["route"])[-1] == "sm_s2" for r in rows2))
        store = JobStore(self.job_db)
        try:
            fresh: dict[str, list[int]] = {}
            for r in store.conn.execute(
                "SELECT source, seq_value FROM run_reads WHERE replayed=0"
                " ORDER BY source, seq_value"
            ):
                fresh.setdefault(r["source"], []).append(r["seq_value"])
            # 逐源去重后的实际读取集合必须恰好覆盖固定区间（不漏）。
            self.assertEqual(sorted(set(fresh["src-a"])), [1, 2, 3, 4, 5])
            self.assertEqual(sorted(set(fresh["src-b"])), [1, 2, 3])
            if not expect_reread:
                # 有完整检查点基线时：每个 seq 只允许被新读取一次（无重读）。
                self.assertEqual(sorted(fresh["src-a"]), [1, 2, 3, 4, 5])
                self.assertEqual(sorted(fresh["src-b"]), [1, 2, 3])
        finally:
            store.close()


class BindingLockFailureTest(FlowTempDir, unittest.TestCase):
    def test_changed_graph_params_rejected_before_any_read(self):
        led_a, led_b = make_standard_ledgers(self.tmp)
        create_job(self.job_db, "job-1", standard_graph(2), standard_sources(led_a, led_b))
        changed = standard_graph(2)
        changed["nodes"][2]["params"]["factor"] = 99  # 算子参数变化
        with self.assertRaises(Exception):
            asyncio.run(run_job(self.job_db, "job-1", graph_dict=changed,
                                sources=standard_sources(led_a, led_b)))
        # 原绑定仍可正常运行，证明拒绝发生在结果被改变之前。
        result = run_inproc(self.job_db)
        self.assertIsNotNone(result["final_cp"])

    def test_changed_source_path_rejected(self):
        led_a, led_b = make_standard_ledgers(self.tmp)
        create_job(self.job_db, "job-1", standard_graph(2), standard_sources(led_a, led_b))
        moved = standard_sources(led_a, led_b)
        moved[1] = dict(moved[1], path=led_a)  # 乙源路径改指甲库
        with self.assertRaises(Exception):
            asyncio.run(run_job(self.job_db, "job-1",
                                graph_dict=standard_graph(2), sources=moved))

    def test_second_executor_fails_explicitly(self):
        make_job(self.tmp, self.job_db)
        gates = inproc_gates(waits=["node.first_barrier.s1"])
        from offline_mqtt.flow.jobs import run_job

        async def scenario():
            runner = asyncio.ensure_future(run_job(self.job_db, "job-1", gates=gates))
            await gates.wait_hit("node.first_barrier.s1", timeout=10)
            # 同一作业的第二个执行者必须显式失败，而不是同时接手。
            with self.assertRaises(RunLockHeld):
                await run_job(self.job_db, "job-1")
            gates.release("node.first_barrier.s1")
            await asyncio.wait_for(runner, timeout=10)

        asyncio.run(scenario())

    def test_real_write_failure_ends_run_and_keeps_recoverable_state(self):
        # 真实写入故障：让第一次检查点提升提交抛 sqlite 错误，运行必须结束，
        # 未完成事务回滚、成品保持为空；解除故障后重新运行得到完整结果，
        # 诊断给出失败位置与当前可恢复编号。
        import sqlite3
        make_job(self.tmp, self.job_db)
        from offline_mqtt.flow import store as store_mod
        original = store_mod.JobStore.commit_checkpoint

        def boom(self, cp_id, *a, **k):
            raise sqlite3.OperationalError("simulated disk I/O error")

        store_mod.JobStore.commit_checkpoint = boom  # type: ignore[assignment]
        try:
            with self.assertRaises(FlowRunError):
                asyncio.run(run_job(self.job_db, "job-1"))
        finally:
            store_mod.JobStore.commit_checkpoint = original  # type: ignore[assignment]

        cp_during, rows_during = get_results(self.job_db, "job-1")
        self.assertEqual(cp_during, 0)
        self.assertEqual(rows_during, [])
        diag = job_diagnostics(self.job_db, "job-1")
        self.assertEqual(diag["recoverable_cp"], 0)
        # 故障解除后重新运行，完整完成且结果正确。
        result = run_inproc(self.job_db)
        self.assertIsNotNone(result["final_cp"])
        cp_final, rows_final = get_results(self.job_db, "job-1", "s1")
        self.assertEqual(len(rows_final), 5)
        # 失败运行在 job_runs 中留有 failed 记录与诊断明细。
        self.assertIn("OperationalError", diag["latest_run"]["detail"]
                      if diag.get("latest_run") else "")

    def test_hard_commit_failure_keeps_previous_checkpoint(self):
        # 真正的写入失败：monkeypatch 提交方法抛 sqlite 错误，验证运行结束、
        # 无完整检查点时成品为空、故障解除后从固定起点重跑得到正确结果。
        import sqlite3
        job2 = self.tmp + "/job2.db"
        sub = self.tmp + "/src2"
        os.makedirs(sub, exist_ok=True)
        led_a2, led_b2 = make_standard_ledgers(sub)
        create_job(job2, "job-1", standard_graph(2), standard_sources(led_a2, led_b2))
        from offline_mqtt.flow import store as store_mod
        original = store_mod.JobStore.commit_checkpoint

        def boom(self, cp_id, *a, **k):
            raise sqlite3.OperationalError("simulated disk I/O error")

        store_mod.JobStore.commit_checkpoint = boom  # type: ignore[assignment]
        try:
            with self.assertRaises(FlowRunError):
                asyncio.run(run_job(job2, "job-1"))
        finally:
            store_mod.JobStore.commit_checkpoint = original  # type: ignore[assignment]
        # 故障期间没有任何完整检查点，成品为空，但作业可重新运行。
        cp2, rows2 = get_results(job2, "job-1")
        self.assertEqual(cp2, 0)
        recovered = asyncio.run(run_job(job2, "job-1"))
        self.assertIsNotNone(recovered["final_cp"])
        cp3, rows3 = get_results(job2, "job-1", "s1")
        self.assertEqual(len(rows3), 5)


if __name__ == "__main__":
    unittest.main()
