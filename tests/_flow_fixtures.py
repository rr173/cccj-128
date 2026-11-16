"""流式加工验收测试的共享夹具。

标准验收图（与需求演练一致）::

    源甲 src-a（账本甲：同主题两条 payload，长度 2、4）
       ├── a_raw ───────────────► union u ──► sum sm ──┬── sm_s1 ─► sink s1
       └── a_sc ─► scale(x10) ─sc_u─►      ▲          └── sm_s2 ─► sink s2
    源乙 src-b（账本乙：同主题一条，长度 3）── b_u ─────┘

每个 sink 最终 5 份贡献；sum：count=5, total=2+20+4+40+3=69。
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import tempfile
from collections import Counter
from typing import Any, Optional

from offline_mqtt.flow import (
    GateConfig,
    Gates,
    create_job,
    create_ledger,
    get_results,
    get_sums,
    run_job,
)

TOPIC = "plant/temp"

# 每条边的容量在测试中可参数化（需求要求分别用 1 和 2 跑）。
def standard_graph(capacity: int = 2) -> dict[str, Any]:
    c = int(capacity)
    return {
        "nodes": [
            {"name": "src-a", "kind": "source"},
            {"name": "src-b", "kind": "source"},
            {"name": "sc", "kind": "scale", "params": {"factor": 10}},
            {"name": "u", "kind": "union"},
            {"name": "sm", "kind": "sum"},
            {"name": "s1", "kind": "sink"},
            {"name": "s2", "kind": "sink"},
        ],
        "edges": [
            {"name": "a_raw", "from": "src-a", "to": "u", "capacity": c},
            {"name": "a_sc", "from": "src-a", "to": "sc", "capacity": c},
            {"name": "sc_u", "from": "sc", "to": "u", "capacity": c},
            {"name": "b_u", "from": "src-b", "to": "u", "capacity": c},
            {"name": "u_sm", "from": "u", "to": "sm", "capacity": c},
            {"name": "sm_s1", "from": "sm", "to": "s1", "capacity": c},
            {"name": "sm_s2", "from": "sm", "to": "s2", "capacity": c},
        ],
    }


def standard_sources(led_a: str, led_b: str, start_after: int = 0) -> list[dict[str, Any]]:
    return [
        {"name": "src-a", "path": led_a, "filter": "#", "start_after": start_after},
        {"name": "src-b", "path": led_b, "filter": "#", "start_after": 0},
    ]


# 手写来源路径清单（身份多重集合），每个 sink 各一份，共 5 条/ sink。
def expected_identities() -> set[tuple[str, int, tuple[str, ...]]]:
    return {
        ("src-a", 1, ("a_raw", "u_sm", "sm_s1")),
        ("src-a", 1, ("a_sc", "sc_u", "u_sm", "sm_s1")),
        ("src-a", 2, ("a_raw", "u_sm", "sm_s1")),
        ("src-a", 2, ("a_sc", "sc_u", "u_sm", "sm_s1")),
        ("src-b", 1, ("b_u", "u_sm", "sm_s1")),
        ("src-a", 1, ("a_raw", "u_sm", "sm_s2")),
        ("src-a", 1, ("a_sc", "sc_u", "u_sm", "sm_s2")),
        ("src-a", 2, ("a_raw", "u_sm", "sm_s2")),
        ("src-a", 2, ("a_sc", "sc_u", "u_sm", "sm_s2")),
        ("src-b", 1, ("b_u", "u_sm", "sm_s2")),
    }


def make_standard_ledgers(dir_path: str) -> tuple[str, str]:
    led_a = os.path.join(dir_path, "ledger_a.db")
    led_b = os.path.join(dir_path, "ledger_b.db")
    create_ledger(led_a, [
        {"topic": TOPIC, "payload": b"ab"},      # seq=1, 长度 2
        {"topic": TOPIC, "payload": b"cdef"},    # seq=2, 长度 4（同主题不覆盖）
    ])
    create_ledger(led_b, [
        {"topic": TOPIC, "payload": b"ghi"},     # seq=1, 长度 3（与甲同号 seq 独立）
    ])
    return led_a, led_b


def identities(rows: list[dict[str, Any]]) -> Counter:
    return Counter((r["source"], r["seq"], tuple(r["route"])) for r in rows)


def assert_standard_sink_result(testcase: Any, rows: list[dict[str, Any]],
                                sink_route_tail: str) -> None:
    """核对一个 sink 的 5 份贡献多重集合与每份 amount。"""
    testcase.assertEqual(len(rows), 5)
    want = {ident for ident in expected_identities() if ident[2][-1] == sink_route_tail}
    got = {(r["source"], r["seq"], tuple(r["route"])) for r in rows}
    testcase.assertEqual(got, want)
    amount_by_identity = {
        (r["source"], r["seq"], tuple(r["route"])): r["amount"] for r in rows
    }
    # 原值路径 amount=字节数；scale 路径 amount=字节数*10。
    testcase.assertEqual(amount_by_identity[("src-a", 1, ("a_raw", "u_sm", sink_route_tail))], 2)
    testcase.assertEqual(
        amount_by_identity[("src-a", 1, ("a_sc", "sc_u", "u_sm", sink_route_tail))], 20)
    testcase.assertEqual(amount_by_identity[("src-a", 2, ("a_raw", "u_sm", sink_route_tail))], 4)
    testcase.assertEqual(
        amount_by_identity[("src-a", 2, ("a_sc", "sc_u", "u_sm", sink_route_tail))], 40)
    testcase.assertEqual(amount_by_identity[("src-b", 1, ("b_u", "u_sm", sink_route_tail))], 3)


def assert_sum_final(testcase: Any, sums: dict[str, Any]) -> None:
    """独立核对 sum 最终状态：同主题 count=5、total=69。"""
    testcase.assertEqual(set(sums.keys()), {"sm"})
    testcase.assertEqual(sums["sm"], {TOPIC: {"count": 5, "total": 69}})


class FlowTempDir:
    """每个用例独立临时目录，结束清理作业库、锁目录与源库。"""

    def setUp(self) -> None:  # noqa: N802
        self.tmp = tempfile.mkdtemp(prefix="flow_")
        self.job_db = os.path.join(self.tmp, "job.db")

    def tearDown(self) -> None:  # noqa: N802
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)


def make_job(tmp: str, job_db: str, capacity: int = 2,
             start_after: int = 0) -> tuple[str, str]:
    led_a, led_b = make_standard_ledgers(tmp)
    create_job(job_db, "job-1", standard_graph(capacity),
               standard_sources(led_a, led_b, start_after))
    return led_a, led_b


def inproc_gates(waits: Optional[list[str]] = None,
                 raises: Optional[list[str]] = None) -> Gates:
    return Gates(GateConfig(waits=set(waits or []), raises=set(raises or [])))


def auto_gates(points: list[str]) -> Gates:
    """命中即自动放行、但仍可用 wait_hit 观察命中的关卡（无轮询竞态）。"""
    gates = Gates(GateConfig())
    for point in points:
        gates.enable_auto_release(point)
    return gates


# ----------------------------------------------------------- 跨进程辅助
def _bootstrap_code() -> str:
    # -I 模式下子进程需要显式注入仓库根目录（与 run_tests.py 同策略）。
    return (
        "import sys; sys.path.insert(0, %r); "
        "from offline_mqtt.flow.cli import main; sys.exit(main())"
    ) % _repo_root()


def _repo_root() -> str:
    # 本模块固定位于 <repo_root>/tests/_flow_fixtures.py，向上两级即仓库根。
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def cli_run(args: list[str], gates_file: Optional[str] = None,
            gates_dir: Optional[str] = None, timeout: float = 30.0,
            env_extra: Optional[dict[str, str]] = None) -> subprocess.CompletedProcess:
    cmd = [sys.executable, "-I", "-c", _bootstrap_code(), *args]
    env = dict(os.environ)
    if env_extra:
        env.update(env_extra)
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env)


def cli_run_gated(job_db: str, gates: dict[str, list[str]], gate_dir: str,
                  wait_points: Optional[list[str]] = None) -> subprocess.Popen:
    """启动带文件关卡的运行者子进程，返回 Popen（测试自行等待/杀进程）。"""
    gates_path = os.path.join(gate_dir, "gates.json")
    with open(gates_path, "w", encoding="utf-8") as fh:
        json.dump(gates, fh)
    cmd = [
        sys.executable, "-I", "-c", _bootstrap_code(),
        "run", job_db, "job-1",
        "--gates", gates_path,
        "--gates-dir", gate_dir,
    ]
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def wait_for_gate_hit(gate_dir: str, point: str, timeout: float = 10.0) -> None:
    """限时等待文件关卡被命中（轮询，不依赖长 sleep）。"""
    hit = os.path.join(gate_dir, f"gate_{point}.hit".replace("/", "_"))
    deadline = asyncio.get_event_loop_policy().get_event_loop().time() \
        if False else _deadline(timeout)
    import time
    while time.time() < deadline:
        if os.path.exists(hit):
            return
        time.sleep(0.01)
    raise TimeoutError(f"关卡 {point} 未在 {timeout}s 内命中: {gate_dir}")


def _deadline(timeout: float) -> float:
    import time
    return time.time() + timeout


def release_gate(gate_dir: str, point: str) -> None:
    path = os.path.join(gate_dir, f"gate_{point}.release".replace("/", "_"))
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("1")


def run_inproc(job_db: str, gates: Optional[Gates] = None) -> dict[str, Any]:
    return asyncio.run(run_job(job_db, "job-1", gates=gates))
