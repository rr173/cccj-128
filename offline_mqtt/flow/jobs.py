"""可恢复流式加工作业的公共 API。

典型用法::

    from offline_mqtt.flow import create_job, run_job, request_checkpoint, get_results

    create_job("job.db", "job-1", graph_dict, source_dicts)
    result = asyncio.run(run_job("job.db", "job-1"))          # 首次运行
    cp = request_checkpoint("job.db", "job-1", wait=True)    # 另一控制入口
    cp_id, rows = get_results("job.db", "job-1")             # 只读成品

被杀后再次 ``run_job`` 即从最近完整检查点恢复；绑定（图、算子参数、源名称/
路径/固定范围）不一致时在任何业务读取之前拒绝。
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Optional

from .gates import Gates
from .locks import ReadOnlyLedger, RunLockHeld
from .model import (
    FlowGraph,
    FlowGraphError,
    SourceBound,
    SourceSpec,
    canonical_binding,
    parse_sources,
)
from .runtime import FlowRunError, FlowRunner
from .store import CheckpointInfo, JobStore, JobStoreError

__all__ = [
    "create_job",
    "run_job",
    "request_checkpoint",
    "checkpoint_status",
    "list_checkpoints",
    "get_results",
    "get_sums",
    "get_snapshot",
    "job_diagnostics",
    "FlowGraphError",
    "FlowRunError",
    "JobStoreError",
    "RunLockHeld",
]


def _bind(graph: FlowGraph, specs: list[SourceSpec]) -> tuple[list[SourceBound], str]:
    graph.validate_source_names([s.name for s in specs])
    bounds: list[SourceBound] = []
    # 每个源独立打开只读账本，分别固定尾部 seq；两源选中同一库同一行也各自固定。
    for spec in specs:
        with ReadOnlyLedger(spec.path) as ledger:
            end_seq = ledger.max_seq()
        if spec.start_after > end_seq:
            raise FlowGraphError(
                "START_AFTER",
                f"数据源 {spec.name!r} 的 start_after={spec.start_after} 超过库尾 {end_seq}",
            )
        bounds.append(SourceBound(spec=spec, end_seq=end_seq))
    return bounds, canonical_binding(graph, bounds)


def create_job(db_path: str, job_id: str, graph_dict: dict[str, Any],
               sources: list[dict[str, Any]]) -> dict[str, Any]:
    """建作业：校验坏图 -> 固定各源结束 seq -> 写入作业绑定。

    坏图在打开任何源账本之前即被拒绝；本函数不执行任何业务读取。
    """
    graph = FlowGraph.from_dict(graph_dict)
    specs = parse_sources(sources)
    bounds, digest = _bind(graph, specs)
    store = JobStore(db_path)
    try:
        if store.job_exists(job_id):
            raise JobStoreError(f"作业 {job_id!r} 已存在，禁止重复创建")
        store.create_job(job_id, graph, bounds, digest, time.time())
    finally:
        store.close()
    return {
        "job_id": job_id,
        "binding_digest": digest,
        "sources": [
            {"name": b.spec.name, "start_after": b.spec.start_after,
             "end_seq": b.end_seq, "filter": b.spec.topic_filter,
             "path": b.spec.path}
            for b in bounds
        ],
    }


def _verify_binding(store: JobStore, job_id: str,
                    graph_dict: Optional[dict[str, Any]],
                    sources: Optional[list[dict[str, Any]]]) -> dict[str, Any]:
    loaded = store.load_job(job_id)
    if graph_dict is None and sources is None:
        return loaded
    # 恢复参数显式传入时必须与绑定完全一致（在改变任何结果前校验）。
    graph = FlowGraph.from_dict(graph_dict) if graph_dict is not None else loaded["graph"]
    specs = parse_sources(sources) if sources is not None else [b.spec for b in loaded["bounds"]]
    _, digest = _bind(graph, specs)
    if digest != loaded["binding_digest"]:
        raise JobStoreError(
            "恢复参数与作业绑定不一致（图/算子参数/源名称/路径/固定范围不可变更）；"
            f"绑定摘要 期望={loaded['binding_digest']} 实际={digest}"
        )
    return loaded


async def run_job(db_path: str, job_id: str, *,
                  graph_dict: Optional[dict[str, Any]] = None,
                  sources: Optional[list[dict[str, Any]]] = None,
                  gates: Optional[Gates] = None) -> dict[str, Any]:
    """运行（或恢复）作业。同一作业同时只允许一个执行进程，否则抛 RunLockHeld。"""
    store = JobStore(db_path)
    try:
        loaded = _verify_binding(store, job_id, graph_dict, sources)
        if loaded["finished"]:
            latest = store.latest_complete()
            return {"run_id": None, "final_cp": latest.cp_id if latest else None,
                    "note": "作业已完成，无需再次执行"}
        runner = FlowRunner(job_id, store, loaded["graph"], loaded["bounds"],
                            loaded["binding_digest"], gates=gates)
        return await runner.run()
    finally:
        store.close()


def request_checkpoint(db_path: str, job_id: str, *, wait: bool = False,
                       timeout: float = 30.0) -> dict[str, Any]:
    """控制入口：请求一次检查点。不获取执行锁，可由另一进程调用。

    wait=True 时轮询到该请求被处理（collecting/done），限时断言避免无限等待。
    """
    store = JobStore(db_path)
    try:
        store.load_job(job_id)  # 作业存在性检查
        req_id = store.insert_request(job_id, time.time())
        if not wait:
            return {"req_id": req_id, "status": "pending"}
        deadline = time.time() + timeout
        while time.time() < deadline:
            info = store.request_status(req_id)
            if info and info["status"] in ("collecting", "done", "failed"):
                return info
            time.sleep(0.01)
        raise TimeoutError(f"检查点请求 {req_id} 在 {timeout}s 内未被运行者受理")
    finally:
        store.close()


def checkpoint_status(db_path: str, job_id: str, req_id: Optional[int] = None) -> Any:
    """查询检查点请求状态（正在收集/已完成），或列出全部检查点截面。"""
    store = JobStore(db_path)
    try:
        store.load_job(job_id)
        if req_id is not None:
            return store.request_status(req_id)
        return [
            {"cp_id": c.cp_id, "status": c.status, "is_final": c.is_final}
            for c in store.list_checkpoints()
        ]
    finally:
        store.close()


def list_checkpoints(db_path: str, job_id: str) -> list[CheckpointInfo]:
    store = JobStore(db_path)
    try:
        store.load_job(job_id)
        return store.list_checkpoints()
    finally:
        store.close()


def get_results(db_path: str, job_id: str, sink: Optional[str] = None
                ) -> tuple[int, list[dict[str, Any]]]:
    """产品查询入口：只返回最近一次**完整**检查点的 sink 结果。

    返回 ``(cp_id, 贡献字典列表)``；收集期间的暂存数据永远不可见。
    """
    store = JobStore(db_path)
    try:
        store.load_job(job_id)
        cp_id, rows = store.latest_sink_rows(sink)
        return cp_id, [
            {
                "source": c.source, "seq": c.seq, "topic": c.topic,
                "amount": c.amount, "route": list(c.route),
                "origin_amount": c.origin_amount,
            }
            for c in rows
        ]
    finally:
        store.close()


def get_sums(db_path: str, job_id: str) -> tuple[int, dict[str, Any]]:
    """返回最近完整检查点中所有 sum 节点的 count/total（与 sink 同编号）。"""
    store = JobStore(db_path)
    try:
        store.load_job(job_id)
        return store.latest_sums()
    finally:
        store.close()


def get_snapshot(db_path: str, job_id: str, cp_id: Optional[int] = None) -> dict[str, Any]:
    """读取完整截面（源位置、sum、边在途、sink、指标），用于核对与诊断。"""
    store = JobStore(db_path)
    try:
        store.load_job(job_id)
        if cp_id is None:
            latest = store.latest_complete()
            if latest is None:
                raise JobStoreError("尚无完整检查点")
            cp_id = latest.cp_id
        return store.load_snapshot(cp_id)
    finally:
        store.close()


def job_diagnostics(db_path: str, job_id: str, run_id: Optional[int] = None) -> dict[str, Any]:
    """诊断信息：检查点列表、实际读取 seq、节点处理计数、边容量与水位。"""
    store = JobStore(db_path)
    try:
        loaded = store.load_job(job_id)
        latest = store.latest_complete()
        out: dict[str, Any] = {
            "job_id": job_id,
            "finished": loaded["finished"],
            "binding_digest": loaded["binding_digest"],
            "checkpoints": [
                {"cp_id": c.cp_id, "status": c.status, "is_final": c.is_final}
                for c in store.list_checkpoints()
            ],
            "recoverable_cp": latest.cp_id if latest else 0,
            "sources": [
                {"name": b.spec.name, "path": b.spec.path,
                 "filter": b.spec.topic_filter, "start_after": b.spec.start_after,
                 "end_seq": b.end_seq}
                for b in loaded["bounds"]
            ],
        }
        if latest is not None:
            snap = store.load_snapshot(latest.cp_id)
            out["latest_snapshot"] = {
                "cp_id": latest.cp_id,
                "source_positions": {
                    name: d["position"] for name, d in snap["sources"].items()
                },
                "metrics": snap["metrics"],
                "edge_inflight": {
                    edge: len(d["items"]) for edge, d in snap["edges"].items()
                },
            }
        if run_id is not None:
            out["run"] = store.run_diagnostics(run_id)
        else:
            rrow = store.conn.execute(
                "SELECT run_id FROM job_runs ORDER BY run_id DESC LIMIT 1"
            ).fetchone()
            if rrow is not None:
                out["latest_run"] = store.run_diagnostics(int(rrow["run_id"]))
        return out
    finally:
        store.close()


def create_ledger(path: str, messages: list[dict[str, Any]]) -> None:
    """测试/演练辅助：用现有接入器创建一个真实 MQTT 账本。

    每条消息形如 ``{"topic": str, "payload": bytes|str}``，seq 由账本自增。
    """
    from ..ingress import MqttIngress
    from ..generator import connect, publish

    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    with MqttIngress(path) as ing:
        ing.ingest(connect("flow-seed"))
        for idx, msg in enumerate(messages, start=1):
            payload = msg["payload"]
            if isinstance(payload, str):
                payload = payload.encode("utf-8")
            ing.ingest(publish("flow-seed", msg["topic"], bytes(payload),
                               qos=1, message_id=idx))
