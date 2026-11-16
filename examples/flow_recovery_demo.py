"""端到端恢复演练脚本（可直接 `python -I examples/flow_recovery_demo.py`）。

步骤：
1. 用现有接入器创建两个真实 MQTT 账本（甲：同主题长度 2、4；乙：长度 3）；
2. 建标准两源 DAG 作业并固定各源尾部 seq；
3. 运行到完成，核对每个 sink 5 份贡献、sum count=5/total=69；
4. 注入“检查点已持久化、控制端未确认”故障后重跑，验证从完整检查点恢复、
   源不重读前缀、结果不重不漏；
5. 打印诊断（容量、实际边水位、读取 seq、节点处理计数、可恢复编号）。
"""

import asyncio
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from offline_mqtt.flow import (  # noqa: E402
    GateConfig,
    Gates,
    FlowRunError,
    create_job,
    create_ledger,
    get_results,
    get_sums,
    job_diagnostics,
    request_checkpoint,
    run_job,
)

TOPIC = "plant/temp"


def standard_graph(capacity: int = 2) -> dict:
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


async def main() -> None:
    work = tempfile.mkdtemp(prefix="flow_demo_")
    led_a, led_b = os.path.join(work, "a.db"), os.path.join(work, "b.db")
    job_db = os.path.join(work, "job.db")

    create_ledger(led_a, [
        {"topic": TOPIC, "payload": b"ab"}, {"topic": TOPIC, "payload": b"cdef"},
    ])
    create_ledger(led_b, [{"topic": TOPIC, "payload": b"ghi"}])

    sources = [
        {"name": "src-a", "path": led_a, "filter": "#", "start_after": 0},
        {"name": "src-b", "path": led_b, "filter": "#", "start_after": 0},
    ]
    info = create_job(job_db, "demo", standard_graph(2), sources)
    print("created, fixed bounds:",
          {s["name"]: (s["start_after"], s["end_seq"]) for s in info["sources"]})

    # 第一次运行：在“完成标记持久化后、控制端确认前”注入故障。
    gates = Gates(GateConfig(raises=["commit.after_persist"]))
    try:
        await run_job(job_db, "demo", gates=gates)
    except FlowRunError as exc:
        print("injected failure:", str(exc).split("；")[0])

    cp_after_fault, _ = get_results(job_db, "demo", "s1")
    print("recoverable checkpoint after fault:", cp_after_fault)

    # 新进程语义：再次 run，从完整检查点恢复并跑到最终完成。
    result = await run_job(job_db, "demo")
    print("resumed run final checkpoint:", result["final_cp"])

    for sink in ("s1", "s2"):
        cp, rows = get_results(job_db, "demo", sink)
        print(f"sink {sink}: cp={cp} count={len(rows)}")
        assert len(rows) == 5, sink
    cp_sum, sums = get_sums(job_db, "demo")
    print("sum:", sums, "(cp", cp_sum, ")")
    assert sums["sm"][TOPIC] == {"count": 5, "total": 69}

    diag = job_diagnostics(job_db, "demo")
    print("edge watermarks:", diag["latest_snapshot"]["metrics"]["edge_watermark"])
    print("latest run reads:", diag["latest_run"]["read_seqs"])
    print("recoverable_cp:", diag["recoverable_cp"])
    print("DEMO OK")


if __name__ == "__main__":
    asyncio.run(main())
