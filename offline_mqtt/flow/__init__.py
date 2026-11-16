"""可恢复的本地流式加工作业（Python + asyncio + SQLite）。

核心保证：检查点是整张 DAG 的**一致处理截面**（Chandy-Lamport 屏障对齐 +
单事务原子提升），每条贡献在截面内要么效果已进入下游封存状态，要么作为
边在途项保存、恢复后先于新读取重放，不重不漏。
"""

from .model import (
    Barrier,
    Contribution,
    EdgeSpec,
    EndOfStream,
    FlowGraph,
    FlowGraphError,
    NodeSpec,
    SourceBound,
    SourceSpec,
    canonical_binding,
    parse_sources,
)
from .store import JobStore, JobStoreError
from .locks import ReadOnlyLedger, RunLock, RunLockHeld
from .gates import GateAbort, GateConfig, Gates
from .runtime import Edge, FlowRunError, FlowRunner, MultiQueueReader
from .jobs import (
    checkpoint_status,
    create_job,
    create_ledger,
    get_results,
    get_snapshot,
    get_sums,
    job_diagnostics,
    list_checkpoints,
    request_checkpoint,
    run_job,
)

__all__ = [
    # 模型与图
    "Contribution", "Barrier", "EndOfStream", "NodeSpec", "EdgeSpec",
    "FlowGraph", "FlowGraphError", "SourceSpec", "SourceBound",
    "parse_sources", "canonical_binding",
    # 存储与锁
    "JobStore", "JobStoreError", "ReadOnlyLedger", "RunLock", "RunLockHeld",
    # 运行时
    "Edge", "FlowRunner", "FlowRunError", "MultiQueueReader",
    "Gates", "GateConfig", "GateAbort",
    # 公共 API
    "create_job", "run_job", "request_checkpoint", "checkpoint_status",
    "list_checkpoints", "get_results", "get_sums", "get_snapshot",
    "job_diagnostics", "create_ledger",
]
