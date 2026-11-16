"""流式加工运行时：asyncio 任务 + 有界 FIFO 边 + Chandy-Lamport 对齐检查点。

一致性截面的形成方式（不是分别保存各节点瞬时状态）：

1. 协调器把同一个屏障 ``Barrier(cp_id)`` 注入每个 source 的控制队列；
   源在把截面位置之前的数据全部投递后，沿每条出边发屏障，然后**立即继续**
   向仍有容量的边投递截面之后的新记录（源不会被统一停到检查点完成）。
2. 边是有界 FIFO 通道，屏障与贡献同队，保证边内次序。
3. 节点在**首个**屏障到达时封存自己的算子状态；此后、其他入边屏障到达前
   从这些边读出的贡献属于“截面时仍在途”，逐边记录；每条入边屏障到齐后
   节点才提交本节点报告。
4. 协调器收齐全部节点报告后，在**单个 SQLite 立即写事务**里把源位置、
   sum 状态、边在途、sink 截面与指标一起提升为完整检查点。

于是每条贡献在截面内非此即彼：其效果已进入封存的下游状态，或者它作为
某条边的在途项被保存、恢复后先于任何新读取被重放——不会两边都有或都没有。
"""

from __future__ import annotations

import asyncio
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Optional

from .gates import GateAbort, Gates
from .locks import ReadOnlyLedger, RunLock
from .model import (
    Barrier,
    Contribution,
    EdgeSpec,
    EndOfStream,
    FlowGraph,
    NodeSpec,
    SourceBound,
)
from .store import JobStore


class FlowRunError(RuntimeError):
    """运行期失败（含真实写入故障）；此前完整检查点保持可恢复。"""


@dataclass
class Edge:
    """一条有界 FIFO 边。容量是正整数，满了 ``put`` 挂起形成背压。"""

    spec: EdgeSpec
    queue: asyncio.Queue = field(default=None)
    watermark: int = 0  # 跨检查点保留的历史最高水位

    @classmethod
    def create(cls, spec: EdgeSpec) -> "Edge":
        return cls(spec=spec, queue=asyncio.Queue(maxsize=spec.capacity))

    async def put(self, item: Any) -> None:
        await self.queue.put(item)
        size = self.queue.qsize()
        if size > self.watermark:
            self.watermark = size

    def qsize(self) -> int:
        return self.queue.qsize()

    def put_nowait(self, item: Any) -> None:
        """恢复在途项时使用：截面在途不超过容量，必然可立即入队。"""
        self.queue.put_nowait(item)
        if self.queue.qsize() > self.watermark:
            self.watermark = self.queue.qsize()


class EdgeInbox:
    """节点某一条入边的串行收件箱（由专属守护协程填充）。

    pump 只负责把边队列的项逐个搬到单槽 ``item``，节点通过 :meth:`consume`
    取走。检查点封存调用 :meth:`snapshot_inflight` **只读**快照 held 与槽中的
    贡献，不移除任何项、不干预 pump——在途贡献随后仍被节点正常处理一次，靠
    节点“每条入边屏障是否已到”的记账保证它们不进入封存状态，恢复时再重放，
    因而既不丢项也不需要把控制标记从数据流里挑出来。
    """

    def __init__(self, edge: Edge):
        self.edge = edge
        self.item: Any = None
        self._held: Any = None
        self._cond = asyncio.Condition()

    async def pump(self) -> None:
        """守护协程：取一项 -> 入槽 -> 等消费 -> 再取下一项。"""
        while True:
            item = await self.edge.queue.get()
            async with self._cond:
                self._held = item
                await self._cond.wait_for(lambda: self.item is None)
                self.item = self._held
                self._held = None
                self._cond.notify_all()

    async def consume(self) -> Any:
        async with self._cond:
            await self._cond.wait_for(lambda: self.item is not None)
            item = self.item
            self.item = None
            self._cond.notify_all()
            return item

    async def wait_item(self) -> None:
        """在持有条件锁的前提下等待槽位有项（供多路读取安全调用）。"""
        async with self._cond:
            await self._cond.wait_for(lambda: self.item is not None)

    async def snapshot_inflight(self) -> list:
        """只读快照 held + 槽中尚未处理的**贡献**（控制标记跳过），保持 FIFO。"""
        async with self._cond:
            snapshot: list = []
            for candidate in (self._held, self.item):
                if isinstance(candidate, Contribution):
                    snapshot.append(candidate)
            return snapshot


class MultiQueueReader:
    """对节点的多条 :class:`EdgeInbox` 做公平多路读取，每条边内部严格 FIFO。

    每个收件箱有独立 pump 守护协程；本读取器只在“槽中已有项”的收件箱间轮换
    选择，全空时等待任一收件箱就绪并立即取消未命中的等待任务。
    """

    def __init__(self, inboxes: dict[str, EdgeInbox]):
        self._inboxes = inboxes
        self._names = list(inboxes.keys())
        self._pump_tasks: list[asyncio.Task] = []
        self._cursor = 0

    def start_pumps(self) -> None:
        for name in self._names:
            self._pump_tasks.append(
                asyncio.create_task(self._inboxes[name].pump(), name=f"pump:{name}")
            )

    async def get(self) -> tuple[str, Any]:
        n = len(self._names)
        while True:
            for i in range(n):
                name = self._names[(self._cursor + i) % n]
                inbox = self._inboxes[name]
                if inbox.item is not None:
                    self._cursor = (self._cursor + i + 1) % n
                    return name, await inbox.consume()
            waiters = {
                asyncio.create_task(self._inboxes[name].wait_item()): name
                for name in self._names
            }
            try:
                await asyncio.wait(set(waiters.keys()),
                                   return_when=asyncio.FIRST_COMPLETED)
            finally:
                for task in waiters:
                    if not task.done():
                        task.cancel()

    async def drain_inflight(self, edge_name: str) -> list:
        """只读快照某条入边当前尚未处理的在途贡献（不改变数据流）。"""
        return await self._inboxes[edge_name].snapshot_inflight()

    async def aclose(self) -> None:
        """取消 pump 守护任务并吞掉 CancelledError，避免未处理异常告警。"""
        for task in self._pump_tasks:
            if not task.done():
                task.cancel()
        if self._pump_tasks:
            await asyncio.gather(*self._pump_tasks, return_exceptions=True)


@dataclass
class _NodeReport:
    cp_id: int
    node: str
    state: Optional[dict[str, Any]] = None      # sum 节点的 topic 状态
    edge_items: dict[str, list[Contribution]] = field(default_factory=dict)
    processed: int = 0
    # source 专用
    position: Optional[int] = None
    exhausted: Optional[bool] = None
    read_count: Optional[int] = None
    # sink 专用
    sink_count: Optional[int] = None


class FlowRunner:
    """单个作业的一次执行（恢复时从最近完整检查点重建全部运行态）。"""

    POLL_INTERVAL = 0.01

    def __init__(self, job_id: str, store: JobStore, graph: FlowGraph,
                 bounds: list[SourceBound], binding_digest: str,
                 gates: Optional[Gates] = None):
        self.job_id = job_id
        self.store = store
        self.graph = graph
        self.bounds = {b.spec.name: b for b in bounds}
        self.binding_digest = binding_digest
        self.gates = gates or Gates()

        self.edges: dict[str, Edge] = {
            e.name: Edge.create(e) for e in graph.edges.values()
        }
        self.ledgers: dict[str, ReadOnlyLedger] = {}
        self._write_lock = asyncio.Lock()
        self._executor: Optional[Any] = None
        self._tasks: list[asyncio.Task] = []
        self.run_id: Optional[int] = None

        # 运行态（恢复时从截面重建）
        self.sum_state: dict[str, dict[str, dict[str, int]]] = {
            n: {} for n in graph.sum_nodes
        }
        self.node_processed: dict[str, int] = {n: 0 for n in graph.nodes}
        self.source_position: dict[str, int] = {
            b.spec.name: b.spec.start_after for b in bounds
        }
        self.source_read_count: dict[str, int] = {b.spec.name: 0 for b in bounds}
        self.source_exhausted: dict[str, bool] = {b.spec.name: False for b in bounds}
        self.source_control: dict[str, asyncio.Queue] = {
            b.spec.name: asyncio.Queue() for b in bounds
        }

        # 协调器状态
        self._reports: dict[int, dict[str, _NodeReport]] = {}
        self._report_events: dict[int, asyncio.Event] = {}
        self._exhausted_sources: set[str] = set()
        self._done_sinks: set[str] = set()
        self._open_cp: Optional[int] = None
        self._final_cp: Optional[int] = None
        self._finished = False
        self._failure: Optional[BaseException] = None

    # ------------------------------------------------------------- 公共入口
    async def run(self) -> dict[str, Any]:
        lock = RunLock(self.store.path)
        lock.acquire()
        try:
            return await self._run_locked()
        finally:
            lock.release()

    async def _run_locked(self) -> dict[str, Any]:
        latest = self.store.latest_complete()
        from_cp = latest.cp_id if latest else None
        self.run_id = self.store.start_run(
            self.job_id, os.getpid(), from_cp, time.time()
        )
        # 重启清理：未完成编号的暂存/封存行全部作废，只认最近完整截面。
        stale = self.store.discard_incomplete_after(from_cp or 0)
        if latest is not None:
            self._restore_snapshot(latest.cp_id)
        # 多工作线程：多个节点任务会并发提交 SQLite 访问，单线程执行器在锁
        # 竞争下会自我饿死；写操作仍由 _write_lock 串行，连接不会被并发使用。
        self._executor = ThreadPoolExecutor(
            max_workers=max(4, len(self.graph.nodes) + 2),
            thread_name_prefix="flow-db",
        )
        for name, bound in self.bounds.items():
            self.ledgers[name] = ReadOnlyLedger(bound.spec.path)
        try:
            self._start_tasks()
            await self._coordinator()
        except BaseException as exc:
            self._failure = exc
            for task in self._tasks:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            detail = f"{type(exc).__name__}: {exc}"
            if stale:
                detail += f"；启动时清理未完成检查点 {stale}"
            # 任何真实写入/状态故障都结束本次运行，并显式给出最近可恢复编号；
            # 此前完整检查点不受影响（未完成事务已回滚、暂存重启即清理）。
            recoverable = self.store.latest_complete()
            self.store.end_run(self.run_id, time.time(), "failed", detail)
            if isinstance(exc, asyncio.CancelledError):
                raise
            raise FlowRunError(
                f"作业运行失败于 {detail}；最近可恢复检查点="
                f"{recoverable.cp_id if recoverable else 0}"
            ) from exc
        else:
            self.store.end_run(
                self.run_id, time.time(), "completed",
                f"最终检查点 {self._final_cp} 已持久化",
            )
            return {
                "run_id": self.run_id, "final_cp": self._final_cp,
                "cleaned_stale": stale,
            }
        finally:
            for ledger in self.ledgers.values():
                ledger.close()
            if self._executor is not None:
                self._executor.shutdown(wait=True)
                self._executor = None

    # ------------------------------------------------------------- 状态恢复
    def _restore_snapshot(self, cp_id: int) -> None:
        snap = self.store.load_snapshot(cp_id)
        for name, d in snap["sources"].items():
            self.source_position[name] = d["position"]
            self.source_read_count[name] = d["read_count"]
            self.source_exhausted[name] = d["exhausted"]
        for node, per_topic in snap["sums"].items():
            self.sum_state[node] = {
                topic: dict(state) for topic, state in per_topic.items()
            }
        # 在途贡献先于任何新读取注回原边；总量不超过该边容量（截面时本就驻留在队中）。
        for edge_name, d in snap["edges"].items():
            edge = self.edges[edge_name]
            for item in d["items"]:
                edge.put_nowait(item)
            edge.watermark = d["watermark"]
            self._record_replays(edge_name, d["items"])
        self.node_processed.update(snap["metrics"]["node_processed"])
        for edge_name, wm in snap["metrics"]["edge_watermark"].items():
            self.edges[edge_name].watermark = max(self.edges[edge_name].watermark, wm)

    def _record_replays(self, edge_name: str, items: list[Contribution]) -> None:
        seqs = [i.seq for i in items if isinstance(i, Contribution)]
        if seqs:
            by_source: dict[str, list[int]] = {}
            for item in items:
                if isinstance(item, Contribution):
                    by_source.setdefault(item.source, []).append(item.seq)
            for source, values in by_source.items():
                self.store.record_reads(self.run_id, source, values, replayed=True)

    # ------------------------------------------------------------- 任务编排
    def _start_tasks(self) -> None:
        for bound in self.bounds.values():
            self._tasks.append(asyncio.create_task(
                self._source_task(bound), name=f"source:{bound.spec.name}"
            ))
        for name, spec in self.graph.nodes.items():
            if spec.kind == "source":
                continue
            self._tasks.append(asyncio.create_task(
                self._node_task(name, spec), name=f"node:{name}"
            ))

    async def _sync_write(self, fn, *args) -> Any:
        # 所有 JobStore 访问都经同一把锁并在专用执行器运行：sqlite 连接对象
        # 不能被事件循环线程与执行器线程并发使用，串行化同时也对齐写事务。
        assert self._executor is not None
        async with self._write_lock:
            return await asyncio.get_running_loop().run_in_executor(
                self._executor, fn, *args
            )

    # ------------------------------------------------------------- 源任务
    async def _source_task(self, bound: SourceBound) -> None:
        spec = bound.spec
        ledger = self.ledgers[spec.name]
        position = self.source_position[spec.name]
        read_count = self.source_read_count[spec.name]
        batch_iter = ledger.iter_range(spec, position, bound.end_seq)
        next_batch: Optional[list] = None
        idx = 0
        exhausted = self.source_exhausted[spec.name]

        def refill() -> Optional[list]:
            try:
                return next(batch_iter)
            except StopIteration:
                return None

        if not exhausted:
            next_batch = refill()
            if next_batch is None:
                exhausted = True
                self.source_exhausted[spec.name] = True

        while True:
            # 控制队列优先：屏障必须排在已投递数据之后、且先于 EOS。
            if not self.source_control[spec.name].empty():
                order = await self.source_control[spec.name].get()
                if isinstance(order, Barrier):
                    await self._emit_to_outputs(spec.name, order)
                    await self._submit_source_report(order.cp_id, spec.name,
                                                     position, read_count, exhausted)
                    if order.cp_id == self._final_cp:
                        await self._emit_to_outputs(spec.name, EndOfStream())
                        self._exhausted_sources.add(spec.name)
                        return
                    continue

            if exhausted:
                # 空源/已耗尽源仍须响应后续检查点屏障。
                await asyncio.sleep(self.POLL_INTERVAL)
                continue

            if next_batch is not None and idx < len(next_batch):
                row = next_batch[idx]
                idx += 1
                contrib = Contribution(
                    source=spec.name, seq=row.seq, topic=row.topic,
                    amount=row.payload_len, origin_amount=row.payload_len,
                )
                await self._emit_to_outputs(spec.name, contrib)
                position = row.seq
                read_count += 1
                self.source_position[spec.name] = position
                self.source_read_count[spec.name] = read_count
                await self._sync_write(
                    self.store.record_reads, self.run_id, spec.name, [row.seq]
                )
                # 截面之后的新记录进入有容量通道的证据点。
                if self._open_cp is not None:
                    await self.gates.hit(f"source.post_cut.{spec.name}")
                if idx >= len(next_batch):
                    next_batch = refill()
                    idx = 0
                    if next_batch is None:
                        exhausted = True
                        self.source_exhausted[spec.name] = True
            else:
                await asyncio.sleep(0)

    async def _emit_to_outputs(self, node_name: str, item: Any) -> None:
        for edge_name in self.graph.outgoing[node_name]:
            edge = self.edges[edge_name]
            if isinstance(item, Contribution):
                forwarded = Contribution(
                    source=item.source, seq=item.seq, topic=item.topic,
                    amount=item.amount, route=item.route + (edge_name,),
                    origin_amount=item.origin_amount,
                )
                await edge.put(forwarded)
            else:  # Barrier / EndOfStream 原样传播
                await edge.put(item)

    async def _submit_source_report(self, cp_id: int, name: str, position: int,
                                    read_count: int, exhausted: bool) -> None:
        await self._accept_report(_NodeReport(
            cp_id=cp_id, node=name, position=position,
            exhausted=exhausted, read_count=read_count,
            processed=self.node_processed.get(name, 0),
        ))

    # ------------------------------------------------------------- 节点任务
    async def _node_task(self, name: str, spec: NodeSpec) -> None:
        in_edges = [self.edges[e] for e in self.graph.incoming[name]]
        inboxes = {e.spec.name: EdgeInbox(e) for e in in_edges}
        reader = MultiQueueReader(inboxes) if in_edges else None
        if reader is not None:
            reader.start_pumps()
        # cp_id -> 已收到屏障的入边集合
        barriers_seen: dict[int, set[str]] = {}
        # cp_id -> 每条入边在截面后读到的在途贡献
        recording: dict[int, dict[str, list[Contribution]]] = {}
        snapshotted: set[int] = set()
        eos_seen: set[str] = set()
        # 首个屏障封存的算子状态（多入边节点在最后一条边屏障到达时才提交报告）。
        state: Optional[dict[str, Any]] = None
        # cp_id -> 尚未收到屏障的入边集合；该集合中的边读到的贡献属于截面在途。
        edges_pending_barrier: dict[int, set[str]] = {}
        # sink：当前暂存归属的检查点编号（首个屏障处翻转到下一编号）。
        latest = await self._sync_write(self.store.latest_complete)
        sink_active_cp = (latest.cp_id + 1) if latest else 1
        sink_buffer: list[Contribution] = []
        # 该 sink 已暂存/落成品的行数，决定下一行 seq_in（按 sink 独立 FIFO 编号）。
        sink_seq_cursor = await self._sink_seq_origin(name)

        try:
            while True:
                assert reader is not None
                edge_name, item = await reader.get()
                if isinstance(item, EndOfStream):
                    eos_seen.add(edge_name)
                    if len(eos_seen) == len(in_edges):
                        if spec.kind == "sink":
                            await self._flush_sink(
                                name, sink_buffer, sink_seq_cursor, sink_active_cp
                            )
                            self._done_sinks.add(name)
                        else:
                            for out in self.graph.outgoing[name]:
                                await self.edges[out].put(EndOfStream())
                        return
                    continue

                if isinstance(item, Barrier):
                    cp_id = item.cp_id
                    barriers_seen.setdefault(cp_id, set()).add(edge_name)
                    if cp_id not in snapshotted:
                        snapshotted.add(cp_id)
                        # 封存的算子状态只含“已处理完”的贡献。对每条入边，把
                        # 前置槽与边队列里尚未处理的项无损冻结为在途基线；这些
                        # 项的效果不进入封存状态，恢复后按 FIFO 先重放。
                        recording[cp_id] = {
                            e.spec.name: await reader.drain_inflight(e.spec.name)
                            for e in in_edges
                        }
                        # 仍未收到屏障的边，其后续到达项继续追加到同一在途列表。
                        edges_pending_barrier[cp_id] = {
                            e.spec.name for e in in_edges
                        }
                        state = await self._snapshot_node(name, spec, cp_id)
                        if spec.kind == "sink":
                            # 截面封存：先把截面内暂存行冲掉，再翻转编号，
                            # 后续贡献写入新编号，不可能污染正在封存的旧截面。
                            sink_seq_cursor = await self._flush_sink(
                                name, sink_buffer, sink_seq_cursor, sink_active_cp
                            )
                            sink_active_cp = cp_id + 1
                        await self.gates.hit(f"node.first_barrier.{name}")
                        # 首个屏障：封存状态后立刻向下游转发，不等其他入边对齐。
                        await self._emit_to_outputs(name, item)
                    # 该边的屏障已到：此边在屏障之后读到的贡献不再属于本截面在途。
                    edges_pending_barrier[cp_id].discard(edge_name)
                    if len(barriers_seen[cp_id]) == len(in_edges):
                        await self._accept_report(_NodeReport(
                            cp_id=cp_id, node=name,
                            state=state if spec.kind == "sum" else None,
                            edge_items=recording.pop(cp_id),
                            processed=self.node_processed[name],
                            sink_count=(sink_seq_cursor if spec.kind == "sink" else None),
                        ))
                        barriers_seen.pop(cp_id)
                        edges_pending_barrier.pop(cp_id, None)
                    continue

                # 普通贡献：算子语义。
                contribution = item
                self.node_processed[name] += 1
                if spec.kind == "sum":
                    bucket = self.sum_state[name].setdefault(
                        contribution.topic, {"count": 0, "total": 0}
                    )
                    bucket["count"] += 1
                    bucket["total"] += contribution.amount
                elif spec.kind == "scale":
                    contribution = Contribution(
                        source=contribution.source, seq=contribution.seq,
                        topic=contribution.topic,
                        amount=contribution.amount * spec.params["factor"],
                        route=contribution.route,
                        origin_amount=contribution.origin_amount,
                    )

                # 在途判定（精确）：若某进行中截面尚未在这条入边收到屏障，
                # 则这条贡献在截面时刻仍“在途”——它的效果不能进入封存状态，
                # 必须逐边记录，恢复后先于新读取重放。注意 sum 已在上面更新
                # 运行态，但封存的 state 是屏障时刻的深拷贝快照，不含此项；
                # 恢复时该贡献会被重新处理一次，与快照恰好不重不漏。
                for open_cp, pending_edges in edges_pending_barrier.items():
                    if edge_name in pending_edges:
                        recording[open_cp][edge_name].append(contribution)
                        await self.gates.hit(f"node.recorded.{name}")

                if spec.kind == "sink":
                    sink_buffer.append(contribution)
                    # 每条贡献立即按当前封存编号写入暂存表（单条插入，有界），
                    # 关卡位于真实落盘之后：命中时暂存行必然已存在但检查点尚未提升。
                    sink_seq_cursor = await self._flush_sink(
                        name, sink_buffer, sink_seq_cursor, sink_active_cp
                    )
                    await self.gates.hit(f"sink.staged.{name}")
                else:
                    await self._emit_to_outputs(name, contribution)
        finally:
            if reader is not None:
                await reader.aclose()

    async def _sink_seq_origin(self, sink: str) -> int:
        """恢复时该 sink 已进入最近完整截面的行数，即下一行的 seq_in 起点。"""
        latest = await self._sync_write(self.store.latest_complete)
        if latest is None:
            return 0
        return await self._sync_write(
            self.store.count_sink_rows, sink, latest.cp_id
        )

    async def _snapshot_node(self, name: str, spec: NodeSpec, cp_id: int) -> Optional[dict]:
        if spec.kind != "sum":
            return None
        # 深拷贝封存，封存后节点继续推进也不影响截面。
        return {
            topic: dict(state) for topic, state in self.sum_state[name].items()
        }

    async def _flush_sink(self, sink: str, buffer: list[Contribution],
                          seq_cursor: int, active_cp: int) -> int:
        """把缓冲的 sink 行按当前封存编号写入暂存表，返回新的 seq 游标。

        暂存行携带明确 cp_id：只在该编号整体提升后才进入成品视图，
        中止恢复时会被 ``discard_incomplete_after`` 整批清理。
        """
        if not buffer:
            return seq_cursor
        await self._sync_write(
            self.store.stage_sink_rows, active_cp, sink, seq_cursor, list(buffer)
        )
        next_cursor = seq_cursor + len(buffer)
        buffer.clear()
        return next_cursor

    async def _accept_report(self, report: _NodeReport) -> None:
        bucket = self._reports.setdefault(report.cp_id, {})
        bucket[report.node] = report
        if len(bucket) == len(self.graph.nodes):
            self._report_events.setdefault(report.cp_id, asyncio.Event()).set()

    # ------------------------------------------------------------- 协调器
    async def _coordinator(self) -> None:
        pending_req: Optional[Any] = None
        while not self._finished:
            # 领取控制请求（收集期间不重复领取，天然一次只处理一个）。
            if pending_req is None and self._open_cp is None:
                row = await self._sync_write(self.store.take_pending_request)
                if row is not None:
                    pending_req = row

            # 触发最终检查点只依据“各源都已读至固定尾部”；屏障对齐会保证
            # 所有在途贡献在截面形成前被下游处理或封存，不能依赖屏障后的
            # EOS 状态（那会形成最终屏障永不注入的循环）。
            all_sources_done = all(self.source_exhausted.values())
            if self._open_cp is None and (pending_req is not None or all_sources_done):
                is_final = all_sources_done
                cp_id = await self._sync_write(self.store.next_checkpoint_id)
                self._open_cp = cp_id
                if is_final:
                    self._final_cp = cp_id
                await self._sync_write(
                    self.store.begin_checkpoint, self.job_id, cp_id, is_final, time.time()
                )
                for queue in self.source_control.values():
                    queue.put_nowait(Barrier(cp_id))
                if pending_req is not None:
                    await self._sync_write(
                        self.store.resolve_request, pending_req["req_id"],
                        "collecting", None, f"cp_id={cp_id} 封存中",
                    )

            if self._open_cp is not None:
                event = self._report_events.setdefault(self._open_cp, asyncio.Event())
                try:
                    await asyncio.wait_for(event.wait(), timeout=self.POLL_INTERVAL)
                except asyncio.TimeoutError:
                    pass
                if event.is_set():
                    await self._finalize_checkpoint(self._open_cp)
                    cp_id = self._open_cp
                    self._open_cp = None
                    event.clear()
                    if pending_req is not None:
                        await self._sync_write(
                            self.store.resolve_request, pending_req["req_id"],
                            "done", cp_id, "检查点已完整持久化",
                        )
                        pending_req = None
                    if cp_id == self._final_cp:
                        self._finished = True
                        self.store.mark_finished(self.job_id)
                        # 等所有 sink 任务自然结束（EOS 在屏障之后）。
                        await asyncio.gather(*self._tasks, return_exceptions=True)
                        return
                    continue

            # 任务异常传播：任一节点任务退出且非完成态即失败。
            for task in self._tasks:
                if task.done() and not task.cancelled() and task.exception() is not None:
                    raise task.exception()  # type: ignore[misc]
            await asyncio.sleep(self.POLL_INTERVAL)

    async def _finalize_checkpoint(self, cp_id: int) -> None:
        reports = self._reports[cp_id]
        if len(reports) != len(self.graph.nodes):  # pragma: no cover - 事件已保证
            raise FlowRunError(f"检查点 {cp_id} 报告不完整")

        sources: dict[str, dict[str, Any]] = {}
        sums: dict[str, dict[str, dict[str, int]]] = {}
        edges_payload: dict[str, dict[str, Any]] = {}
        for name, report in reports.items():
            spec = self.graph.nodes[name]
            if spec.kind == "source":
                sources[name] = {
                    "position": report.position,
                    "exhausted": bool(report.exhausted),
                    "read_count": report.read_count,
                }
            elif spec.kind == "sum":
                assert report.state is not None
                sums[name] = report.state
            for edge_name, items in report.edge_items.items():
                edges_payload[edge_name] = items
        # 每条边恰好由其目的节点报告一次。
        edge_section: dict[str, dict[str, Any]] = {}
        for edge_name, edge in self.edges.items():
            edge_section[edge_name] = {
                "items": edges_payload.get(edge_name, []),
                "watermark": edge.watermark,
            }

        async with self._write_lock:
            assert self._executor is not None
            loop = asyncio.get_running_loop()
            # sink 截面必须是“截至本截面的累计结果”：合并上一完整截面的成品
            # 与本编号暂存的新增行。暂存行在收集期间对外不可见，且中止恢复时
            # 会被整批清理；seq_in 在每个完整截面内按 sink 从 0 重新枚举。
            new_sinks = await loop.run_in_executor(
                self._executor, self.store.load_staging_sinks, cp_id
            )
            prev_cp = self._reports_prev_complete_cp(cp_id)
            if prev_cp is not None:
                prev_sinks = await loop.run_in_executor(
                    self._executor, self.store.load_complete_sinks, prev_cp
                )
            else:
                prev_sinks = {}
            all_sink_names = set(prev_sinks) | set(new_sinks)
            sinks: dict[str, list[Contribution]] = {}
            for sink in all_sink_names:
                sinks[sink] = list(prev_sinks.get(sink, [])) + list(new_sinks.get(sink, []))
            metrics = {
                "node_processed": dict(self.node_processed),
                "edge_capacity": {n: e.spec.capacity for n, e in self.edges.items()},
                "edge_watermark": {n: e.watermark for n, e in self.edges.items()},
            }
            await loop.run_in_executor(
                self._executor, self.store.commit_checkpoint, cp_id, time.time(),
                sources, sums, edge_section, sinks, metrics,
            )
            await self.gates.hit("commit.partial_state")
            await loop.run_in_executor(
                self._executor, self.store.record_node_counts, self.run_id,
                dict(self.node_processed),
            )
        await self.gates.hit("commit.after_persist")

    def _reports_prev_complete_cp(self, current_cp: int) -> Optional[int]:
        latest = self.store.latest_complete()
        if latest is None or latest.cp_id >= current_cp:
            return None
        return latest.cp_id
