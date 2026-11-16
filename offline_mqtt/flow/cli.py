"""流式加工作业的终端入口（可在独立进程中验证恢复持久性）。

用法::

    python -m offline_mqtt.flow.cli create  JOB_DB JOB_ID --graph graph.json --sources sources.json
    python -m offline_mqtt.flow.cli run     JOB_DB JOB_ID [--gates gates.json] [--gates-dir DIR]
    python -m offline_mqtt.flow.cli request JOB_DB JOB_ID [--wait] [--timeout SEC]
    python -m offline_mqtt.flow.cli status  JOB_DB JOB_ID
    python -m offline_mqtt.flow.cli results JOB_DB JOB_ID [--sink SINK]
    python -m offline_mqtt.flow.cli sums    JOB_DB JOB_ID
    python -m offline_mqtt.flow.cli diag    JOB_DB JOB_ID [--run-id N]

退出码：0 成功；2 输入/绑定/图错误；3 执行锁被占用；4 运行期失败（可恢复）。
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Optional

from .gates import GateConfig, Gates
from .jobs import (
    FlowGraphError,
    FlowRunError,
    JobStoreError,
    RunLockHeld,
    checkpoint_status,
    create_job,
    get_results,
    get_snapshot,
    get_sums,
    job_diagnostics,
    list_checkpoints,
    request_checkpoint,
    run_job,
)

EXIT_OK = 0
EXIT_INPUT = 2
EXIT_LOCKED = 3
EXIT_RUN_FAILED = 4


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _print(obj: Any) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, default=_default))


def _default(obj: Any) -> Any:
    if isinstance(obj, tuple):
        return list(obj)
    if hasattr(obj, "__dict__"):
        return obj.__dict__
    return repr(obj)


def _create(args: argparse.Namespace) -> int:
    try:
        result = create_job(
            args.job_db, args.job_id,
            _load_json(args.graph), _load_json(args.sources),
        )
    except (FlowGraphError, JobStoreError, FileNotFoundError) as exc:
        _print({"status": "error", "kind": type(exc).__name__, "detail": str(exc)})
        return EXIT_INPUT
    _print({"status": "created", **result})
    return EXIT_OK


def _run(args: argparse.Namespace) -> int:
    import asyncio

    gates: Optional[Gates] = None
    if args.gates:
        gates = Gates(GateConfig.parse(_load_json(args.gates)),
                      state_dir=args.gates_dir)
    try:
        result = asyncio.run(run_job(args.job_db, args.job_id, gates=gates))
    except RunLockHeld as exc:
        _print({"status": "error", "kind": "RunLockHeld", "detail": str(exc)})
        return EXIT_LOCKED
    except (FlowRunError, JobStoreError, FlowGraphError) as exc:
        _print({"status": "error", "kind": type(exc).__name__, "detail": str(exc)})
        return EXIT_RUN_FAILED
    _print({"status": "run_finished", **result})
    return EXIT_OK


def _request(args: argparse.Namespace) -> int:
    try:
        result = request_checkpoint(args.job_db, args.job_id,
                                    wait=args.wait, timeout=args.timeout)
    except (JobStoreError, TimeoutError) as exc:
        _print({"status": "error", "kind": type(exc).__name__, "detail": str(exc)})
        return EXIT_INPUT
    _print(result)
    return EXIT_OK


def _status(args: argparse.Namespace) -> int:
    _print({
        "checkpoints": [
            {"cp_id": c.cp_id, "status": c.status, "is_final": c.is_final}
            for c in list_checkpoints(args.job_db, args.job_id)
        ]
    })
    return EXIT_OK


def _results(args: argparse.Namespace) -> int:
    cp_id, rows = get_results(args.job_db, args.job_id, args.sink)
    _print({"cp_id": cp_id, "sink": args.sink, "count": len(rows), "rows": rows})
    return EXIT_OK


def _sums(args: argparse.Namespace) -> int:
    cp_id, sums = get_sums(args.job_db, args.job_id)
    _print({"cp_id": cp_id, "sums": sums})
    return EXIT_OK


def _diag(args: argparse.Namespace) -> int:
    _print(job_diagnostics(args.job_db, args.job_id, args.run_id))
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="offline_mqtt.flow")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_create = sub.add_parser("create", help="建作业并固定各源结束 seq")
    p_create.add_argument("job_db")
    p_create.add_argument("job_id")
    p_create.add_argument("--graph", required=True)
    p_create.add_argument("--sources", required=True)
    p_create.set_defaults(fn=_create)

    p_run = sub.add_parser("run", help="运行或从最近完整检查点恢复")
    p_run.add_argument("job_db")
    p_run.add_argument("job_id")
    p_run.add_argument("--gates", default=None, help="关卡配置 JSON（测试用）")
    p_run.add_argument("--gates-dir", default=None, help="跨进程关卡状态目录")
    p_run.set_defaults(fn=_run)

    p_req = sub.add_parser("request", help="控制入口请求检查点")
    p_req.add_argument("job_db")
    p_req.add_argument("job_id")
    p_req.add_argument("--wait", action="store_true")
    p_req.add_argument("--timeout", type=float, default=30.0)
    p_req.set_defaults(fn=_request)

    p_status = sub.add_parser("status", help="查看检查点收集/完成状态")
    p_status.add_argument("job_db")
    p_status.add_argument("job_id")
    p_status.set_defaults(fn=_status)

    p_results = sub.add_parser("results", help="读取最近完整检查点的 sink 成品")
    p_results.add_argument("job_db")
    p_results.add_argument("job_id")
    p_results.add_argument("--sink", default=None)
    p_results.set_defaults(fn=_results)

    p_sums = sub.add_parser("sums", help="读取 sum 节点最终状态（同检查点编号）")
    p_sums.add_argument("job_db")
    p_sums.add_argument("job_id")
    p_sums.set_defaults(fn=_sums)

    p_diag = sub.add_parser("diag", help="诊断：位置/水位/读取与处理计数")
    p_diag.add_argument("job_db")
    p_diag.add_argument("job_id")
    p_diag.add_argument("--run-id", type=int, default=None)
    p_diag.set_defaults(fn=_diag)

    args = parser.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
