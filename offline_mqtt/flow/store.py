"""加工作业的 SQLite 持久化层。

数据库文件与所有源账本分离；源账本始终以只读模式打开，本库是唯一可写对象。

检查点生命周期（编号严格单调，已完成编号永不被覆盖）::

    请求/触发   -> checkpoint_requests 写入
    开始封存    -> checkpoints 插入 status='collecting' 的新行（编号 +1）
    封存中暂存  -> sink_staging 携带 cp_id 暂写；edge_state/node_state 等暂写
    原子提升    -> 单个 BEGIN IMMEDIATE 事务：
                     1. 写齐 sources/sums/edges/sinks/metrics 所有截面行
                     2. checkpoints.status='complete'（编号不可变）
                     3. 清理本编号 staging 与更早编号的旧保留
    中止/被杀   -> collecting 行保留为诊断；恢复时只挑 status='complete' 的最大编号，
                  cp_id > 已完成编号的 staging / state 全部删除，绝不混入成品。

进程互斥由 ``flow_locks`` 目录下的 fcntl 排他锁保证（进程退出即释放），
SQLite 内的 job_runs 表同时记录执行者，便于诊断与查询。
"""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterable, Optional

from .model import Contribution, FlowGraph, SourceBound, SourceSpec

SCHEMA_VERSION = 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
    job_id          TEXT PRIMARY KEY,
    created_at      REAL NOT NULL,
    graph_json      TEXT NOT NULL,
    binding_digest  TEXT NOT NULL,
    finished        INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS job_sources (
    job_id       TEXT NOT NULL,
    name         TEXT NOT NULL,
    path         TEXT NOT NULL,
    filter       TEXT NOT NULL,
    start_after  INTEGER NOT NULL,
    end_seq      INTEGER NOT NULL,
    PRIMARY KEY (job_id, name)
);

-- 控制入口提出的检查点请求（一次只处理一个，收集期间的重复请求合并）。
CREATE TABLE IF NOT EXISTS checkpoint_requests (
    req_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id       TEXT NOT NULL,
    requested_at REAL NOT NULL,
    status       TEXT NOT NULL,            -- pending|collecting|done|failed
    cp_id        INTEGER,                  -- 完成后回填
    detail       TEXT
);

CREATE TABLE IF NOT EXISTS checkpoints (
    cp_id        INTEGER PRIMARY KEY,      -- 单调编号，由本库 AUTOINCREMENT 风格维护
    job_id       TEXT NOT NULL,
    status       TEXT NOT NULL,            -- collecting|complete
    is_final     INTEGER NOT NULL DEFAULT 0,
    created_at   REAL NOT NULL,
    completed_at REAL
);

-- 封存中暂存的节点状态 / 边在途（cp_id 级隔离，中止后整批清理）。
CREATE TABLE IF NOT EXISTS node_state_staging (
    cp_id   INTEGER NOT NULL,
    node    TEXT NOT NULL,
    state   TEXT NOT NULL,                 -- JSON
    PRIMARY KEY (cp_id, node)
);
CREATE TABLE IF NOT EXISTS edge_state_staging (
    cp_id   INTEGER NOT NULL,
    edge    TEXT NOT NULL,
    items   TEXT NOT NULL,                 -- JSON 序列化的在途 EdgeItem 列表
    PRIMARY KEY (cp_id, edge)
);

-- sink 暂存：只有检查点整体提升后才会被成品查询看到。
CREATE TABLE IF NOT EXISTS sink_staging (
    cp_id    INTEGER NOT NULL,
    sink     TEXT NOT NULL,
    seq_in   INTEGER NOT NULL,             -- 该 sink 内的 FIFO 序号
    payload  TEXT NOT NULL,                -- Contribution JSON
    PRIMARY KEY (cp_id, sink, seq_in)
);

-- 已完成检查点的一致截面（每个 (cp_id, ...) 主键在同一事务内写齐）。
CREATE TABLE IF NOT EXISTS cp_sources (
    cp_id        INTEGER NOT NULL,
    source       TEXT NOT NULL,
    position     INTEGER NOT NULL,         -- 该源已读到的最大 seq（恢复起点）
    exhausted    INTEGER NOT NULL,
    read_count   INTEGER NOT NULL,         -- 截至截面实际读出的匹配行数
    PRIMARY KEY (cp_id, source)
);
CREATE TABLE IF NOT EXISTS cp_sums (
    cp_id   INTEGER NOT NULL,
    node    TEXT NOT NULL,
    topic   TEXT NOT NULL,
    cnt     INTEGER NOT NULL,
    total   INTEGER NOT NULL,
    PRIMARY KEY (cp_id, node, topic)
);
CREATE TABLE IF NOT EXISTS cp_edges (
    cp_id      INTEGER NOT NULL,
    edge       TEXT NOT NULL,
    items      TEXT NOT NULL,              -- 恢复所必需的在途贡献
    watermark  INTEGER NOT NULL,           -- 截至截面观察到的实际最高水位
    PRIMARY KEY (cp_id, edge)
);
CREATE TABLE IF NOT EXISTS cp_sinks (
    cp_id   INTEGER NOT NULL,
    sink    TEXT NOT NULL,
    seq_in  INTEGER NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY (cp_id, sink, seq_in)
);
CREATE TABLE IF NOT EXISTS cp_metrics (
    cp_id           INTEGER PRIMARY KEY,
    node_processed  TEXT NOT NULL,         -- JSON: node -> 截至截面处理计数
    edge_capacity   TEXT NOT NULL,         -- JSON: edge -> 配置容量
    edge_watermark  TEXT NOT NULL          -- JSON: edge -> 实际最高水位
);

CREATE TABLE IF NOT EXISTS job_runs (
    run_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      TEXT NOT NULL,
    started_at  REAL NOT NULL,
    ended_at    REAL,
    pid         INTEGER NOT NULL,
    from_cp     INTEGER,                   -- 本次运行从哪个完整检查点恢复
    result      TEXT,                      -- completed|failed
    detail      TEXT
);

-- 核对用：每次运行每个源实际读取的 seq 区间与逐节点处理计数（增量追加）。
CREATE TABLE IF NOT EXISTS run_reads (
    run_id     INTEGER NOT NULL,
    source     TEXT NOT NULL,
    seq_value  INTEGER NOT NULL,
    replayed   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, source, seq_value)
);
CREATE TABLE IF NOT EXISTS run_node_counts (
    run_id   INTEGER NOT NULL,
    node     TEXT NOT NULL,
    processed INTEGER NOT NULL,            -- 本次运行内处理数（含在途重放）
    PRIMARY KEY (run_id, node)
);

CREATE INDEX IF NOT EXISTS idx_staging_cp ON sink_staging(cp_id);
CREATE INDEX IF NOT EXISTS idx_cp_sinks_cp ON cp_sinks(cp_id);
"""


class JobStoreError(RuntimeError):
    """作业库访问失败（绑定不符、作业不存在等）。"""


@dataclass
class CheckpointInfo:
    cp_id: int
    status: str
    is_final: bool
    created_at: float
    completed_at: Optional[float]


class JobStore:
    """单个作业库的同步 SQLite 封装；并发由文件锁 + busy_timeout 串行化。"""

    def __init__(self, path: str, busy_timeout_ms: int = 10000):
        self.path = path
        self.conn = sqlite3.connect(
            path, check_same_thread=False, isolation_level=None,
            timeout=busy_timeout_ms / 1000,
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)}")
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(SCHEMA)
        self._check_schema()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "JobStore":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _check_schema(self) -> None:
        row = self.conn.execute(
            "SELECT value FROM meta WHERE key='schema_version'"
        ).fetchone()
        if row is None:
            self.conn.execute(
                "INSERT INTO meta(key, value) VALUES('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )
        elif int(row["value"]) != SCHEMA_VERSION:
            raise JobStoreError(
                f"作业库 schema 版本 {row['value']} 与代码 {SCHEMA_VERSION} 不兼容"
            )

    @contextmanager
    def write(self) -> Iterable[sqlite3.Connection]:
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    # ----------------------------------------------------------- 作业创建
    def create_job(
        self,
        job_id: str,
        graph: FlowGraph,
        bounds: list[SourceBound],
        binding_digest: str,
        created_at: float,
    ) -> None:
        try:
            with self.write() as conn:
                conn.execute(
                    "INSERT INTO jobs(job_id, created_at, graph_json, binding_digest)"
                    " VALUES(?, ?, ?, ?)",
                    (job_id, created_at, json.dumps(graph.to_dict(), ensure_ascii=False),
                     binding_digest),
                )
                conn.executemany(
                    "INSERT INTO job_sources(job_id, name, path, filter, start_after, end_seq)"
                    " VALUES(?, ?, ?, ?, ?, ?)",
                    [
                        (job_id, b.spec.name, b.spec.path, b.spec.topic_filter,
                         b.spec.start_after, b.end_seq)
                        for b in bounds
                    ],
                )
        except sqlite3.IntegrityError as exc:
            raise JobStoreError(f"作业 {job_id!r} 已存在") from exc

    def job_exists(self, job_id: str) -> bool:
        return self.conn.execute(
            "SELECT 1 FROM jobs WHERE job_id=?", (job_id,)
        ).fetchone() is not None

    def load_job(self, job_id: str) -> dict[str, Any]:
        row = self.conn.execute(
            "SELECT * FROM jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        if row is None:
            raise JobStoreError(f"作业 {job_id!r} 不存在")
        source_rows = self.conn.execute(
            "SELECT * FROM job_sources WHERE job_id=? ORDER BY name", (job_id,)
        ).fetchall()
        bounds = [
            SourceBound(
                spec=SourceSpec(
                    name=r["name"], path=r["path"], topic_filter=r["filter"],
                    start_after=int(r["start_after"]),
                ),
                end_seq=int(r["end_seq"]),
            )
            for r in source_rows
        ]
        return {
            "job_id": job_id,
            "graph": FlowGraph.from_dict(json.loads(row["graph_json"])),
            "bounds": bounds,
            "binding_digest": row["binding_digest"],
            "finished": bool(row["finished"]),
        }

    def mark_finished(self, job_id: str) -> None:
        with self.write() as conn:
            conn.execute("UPDATE jobs SET finished=1 WHERE job_id=?", (job_id,))

    # ----------------------------------------------------- 检查点编号与状态
    def begin_checkpoint(self, job_id: str, cp_id: int, is_final: bool,
                         created_at: float) -> None:
        with self.write() as conn:
            conn.execute(
                "INSERT INTO checkpoints(cp_id, job_id, status, is_final, created_at)"
                " VALUES(?, ?, 'collecting', ?, ?)",
                (cp_id, job_id, int(is_final), created_at),
            )

    def next_checkpoint_id(self) -> int:
        row = self.conn.execute("SELECT COALESCE(MAX(cp_id), 0) + 1 AS m FROM checkpoints").fetchone()
        return int(row["m"])

    def latest_complete(self) -> Optional[CheckpointInfo]:
        row = self.conn.execute(
            "SELECT * FROM checkpoints WHERE status='complete'"
            " ORDER BY cp_id DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        return CheckpointInfo(
            cp_id=int(row["cp_id"]), status=row["status"], is_final=bool(row["is_final"]),
            created_at=float(row["created_at"]), completed_at=row["completed_at"],
        )

    def list_checkpoints(self) -> list[CheckpointInfo]:
        rows = self.conn.execute("SELECT * FROM checkpoints ORDER BY cp_id").fetchall()
        return [
            CheckpointInfo(
                cp_id=int(r["cp_id"]), status=r["status"], is_final=bool(r["is_final"]),
                created_at=float(r["created_at"]),
                completed_at=r["completed_at"],
            )
            for r in rows
        ]

    def checkpoint_status(self, cp_id: int) -> Optional[str]:
        row = self.conn.execute(
            "SELECT status FROM checkpoints WHERE cp_id=?", (cp_id,)
        ).fetchone()
        return None if row is None else row["status"]

    # ----------------------------------------------------------- 封存暂存
    def stage_node_state(self, cp_id: int, node: str, state: dict[str, Any]) -> None:
        with self.write() as conn:
            conn.execute(
                "INSERT INTO node_state_staging(cp_id, node, state) VALUES(?, ?, ?)"
                " ON CONFLICT(cp_id, node) DO UPDATE SET state=excluded.state",
                (cp_id, node, json.dumps(state, ensure_ascii=False, sort_keys=True)),
            )

    def stage_edge_state(self, cp_id: int, edge: str, items: list) -> None:
        with self.write() as conn:
            conn.execute(
                "INSERT INTO edge_state_staging(cp_id, edge, items) VALUES(?, ?, ?)"
                " ON CONFLICT(cp_id, edge) DO UPDATE SET items=excluded.items",
                (cp_id, edge, json.dumps([_dump_item(i) for i in items], ensure_ascii=False)),
            )

    def stage_sink_rows(self, cp_id: int, sink: str, start_seq: int,
                        contributions: list[Contribution]) -> None:
        with self.write() as conn:
            conn.executemany(
                "INSERT INTO sink_staging(cp_id, sink, seq_in, payload) VALUES(?, ?, ?, ?)",
                [
                    (cp_id, sink, start_seq + idx, c.to_json())
                    for idx, c in enumerate(contributions)
                ],
            )

    def load_staging_sinks(self, cp_id: int) -> dict[str, list[Contribution]]:
        rows = self.conn.execute(
            "SELECT sink, seq_in, payload FROM sink_staging WHERE cp_id=?"
            " ORDER BY sink, seq_in",
            (cp_id,),
        ).fetchall()
        sinks: dict[str, list[Contribution]] = {}
        for r in rows:
            sinks.setdefault(r["sink"], []).append(Contribution.from_json(r["payload"]))
        return sinks

    def load_complete_sinks(self, cp_id: int) -> dict[str, list[Contribution]]:
        """读取某个完整截面中每个 sink 的累计结果（按 seq_in 排序）。"""
        sinks: dict[str, list[Contribution]] = {}
        for r in self.conn.execute(
            "SELECT sink, seq_in, payload FROM cp_sinks WHERE cp_id=?"
            " ORDER BY sink, seq_in",
            (cp_id,),
        ):
            sinks.setdefault(r["sink"], []).append(Contribution.from_json(r["payload"]))
        return sinks

    def count_sink_rows(self, sink: str, cp_id: int) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS c FROM cp_sinks WHERE cp_id=? AND sink=?",
            (cp_id, sink),
        ).fetchone()
        return int(row["c"])

    # ----------------------------------------------------- 原子提升检查点
    def commit_checkpoint(
        self,
        cp_id: int,
        completed_at: float,
        sources: dict[str, dict[str, Any]],
        sums: dict[str, dict[str, int]],
        edges: dict[str, dict[str, Any]],
        sinks: dict[str, list[Contribution]],
        metrics: dict[str, Any],
    ) -> None:
        """在**单个立即写事务**内把完整截面提升为成品。

        事务中途被杀只会整体回滚（保留此前完整检查点），绝不会出现半个成品；
        已完成编号永远不会被再次写入（主键冲突即失败）。
        """
        with self.write() as conn:
            row = conn.execute(
                "SELECT status FROM checkpoints WHERE cp_id=?", (cp_id,)
            ).fetchone()
            if row is None:
                raise JobStoreError(f"检查点 {cp_id} 不存在，无法提升")
            if row["status"] == "complete":
                raise JobStoreError(f"检查点 {cp_id} 已完成，禁止覆盖")

            conn.executemany(
                "INSERT INTO cp_sources(cp_id, source, position, exhausted, read_count)"
                " VALUES(?, ?, ?, ?, ?)",
                [
                    (cp_id, name, d["position"], int(d["exhausted"]), d["read_count"])
                    for name, d in sorted(sources.items())
                ],
            )
            sum_rows = [
                (cp_id, node, topic, int(state["count"]), int(state["total"]))
                for node, per_topic in sorted(sums.items())
                for topic, state in sorted(per_topic.items())
            ]
            if sum_rows:
                conn.executemany(
                    "INSERT INTO cp_sums(cp_id, node, topic, cnt, total)"
                    " VALUES(?, ?, ?, ?, ?)", sum_rows,
                )
            conn.executemany(
                "INSERT INTO cp_edges(cp_id, edge, items, watermark) VALUES(?, ?, ?, ?)",
                [
                    (cp_id, edge,
                     json.dumps([_dump_item(i) for i in d["items"]], ensure_ascii=False),
                     int(d["watermark"]))
                    for edge, d in sorted(edges.items())
                ],
            )
            sink_rows = [
                (cp_id, sink, seq_in, c.to_json())
                for sink, contribs in sorted(sinks.items())
                for seq_in, c in enumerate(contribs)
            ]
            if sink_rows:
                conn.executemany(
                    "INSERT INTO cp_sinks(cp_id, sink, seq_in, payload) VALUES(?, ?, ?, ?)",
                    sink_rows,
                )
            conn.execute(
                "INSERT INTO cp_metrics(cp_id, node_processed, edge_capacity, edge_watermark)"
                " VALUES(?, ?, ?, ?)",
                (
                    cp_id,
                    json.dumps(metrics["node_processed"], sort_keys=True),
                    json.dumps(metrics["edge_capacity"], sort_keys=True),
                    json.dumps(metrics["edge_watermark"], sort_keys=True),
                ),
            )
            conn.execute(
                "UPDATE checkpoints SET status='complete', completed_at=? WHERE cp_id=?",
                (completed_at, cp_id),
            )
            # 清理本编号暂存以及任何更小编号的残留（保留策略：只留完整截面）。
            conn.execute("DELETE FROM sink_staging WHERE cp_id<=?", (cp_id,))
            conn.execute("DELETE FROM node_state_staging WHERE cp_id<=?", (cp_id,))
            conn.execute("DELETE FROM edge_state_staging WHERE cp_id<=?", (cp_id,))

    def discard_incomplete_after(self, last_complete_cp: int) -> list[int]:
        """恢复清理：删除未完成编号的全部暂存与 collecting 行。

        只作废运行开始前已处于 ``collecting`` 的请求（它们属于上一个被杀的
        运行者）；``pending`` 请求可能是控制入口在本运行者启动前后插入的新
        请求，必须保留给本轮处理，不能误判为残留。
        """
        with self.write() as conn:
            rows = conn.execute(
                "SELECT cp_id FROM checkpoints WHERE cp_id>? ORDER BY cp_id",
                (last_complete_cp,),
            ).fetchall()
            stale = [int(r["cp_id"]) for r in rows]
            conn.execute("DELETE FROM sink_staging WHERE cp_id>?", (last_complete_cp,))
            conn.execute("DELETE FROM node_state_staging WHERE cp_id>?", (last_complete_cp,))
            conn.execute("DELETE FROM edge_state_staging WHERE cp_id>?", (last_complete_cp,))
            conn.execute("DELETE FROM checkpoints WHERE cp_id>?", (last_complete_cp,))
            conn.execute(
                "UPDATE checkpoint_requests SET status='failed',"
                " detail='runner restarted before checkpoint completed'"
                " WHERE status='collecting'"
            )
        return stale

    # ----------------------------------------------------------- 成品查询
    def load_snapshot(self, cp_id: int) -> dict[str, Any]:
        row = self.conn.execute(
            "SELECT status FROM checkpoints WHERE cp_id=?", (cp_id,)
        ).fetchone()
        if row is None or row["status"] != "complete":
            raise JobStoreError(f"检查点 {cp_id} 不是已完成截面")
        sources = {
            r["source"]: {
                "position": int(r["position"]),
                "exhausted": bool(r["exhausted"]),
                "read_count": int(r["read_count"]),
            }
            for r in self.conn.execute(
                "SELECT * FROM cp_sources WHERE cp_id=? ORDER BY source", (cp_id,))
        }
        sums: dict[str, dict[str, dict[str, int]]] = {}
        for r in self.conn.execute("SELECT * FROM cp_sums WHERE cp_id=?", (cp_id,)):
            sums.setdefault(r["node"], {})[r["topic"]] = {
                "count": int(r["cnt"]), "total": int(r["total"]),
            }
        edges = {}
        for r in self.conn.execute("SELECT * FROM cp_edges WHERE cp_id=?", (cp_id,)):
            edges[r["edge"]] = {
                "items": [_load_item(x) for x in json.loads(r["items"])],
                "watermark": int(r["watermark"]),
            }
        sinks: dict[str, list[Contribution]] = {}
        for r in self.conn.execute(
            "SELECT * FROM cp_sinks WHERE cp_id=? ORDER BY sink, seq_in", (cp_id,)
        ):
            sinks.setdefault(r["sink"], []).append(Contribution.from_json(r["payload"]))
        mrow = self.conn.execute(
            "SELECT * FROM cp_metrics WHERE cp_id=?", (cp_id,)
        ).fetchone()
        metrics = {
            "node_processed": json.loads(mrow["node_processed"]),
            "edge_capacity": json.loads(mrow["edge_capacity"]),
            "edge_watermark": json.loads(mrow["edge_watermark"]),
        }
        return {"sources": sources, "sums": sums, "edges": edges,
                "sinks": sinks, "metrics": metrics}

    def latest_sink_rows(self, sink: Optional[str] = None) -> tuple[int, list[Contribution]]:
        """产品查询入口：只返回最近一次完整检查点的 sink 结果。"""
        latest = self.latest_complete()
        if latest is None:
            return 0, []
        if sink is None:
            rows = self.conn.execute(
                "SELECT * FROM cp_sinks WHERE cp_id=? ORDER BY sink, seq_in",
                (latest.cp_id,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM cp_sinks WHERE cp_id=? AND sink=? ORDER BY seq_in",
                (latest.cp_id, sink),
            ).fetchall()
        return latest.cp_id, [Contribution.from_json(r["payload"]) for r in rows]

    def latest_sums(self) -> tuple[int, dict[str, dict[str, dict[str, int]]]]:
        latest = self.latest_complete()
        if latest is None:
            return 0, {}
        snap = self.load_snapshot(latest.cp_id)
        return latest.cp_id, snap["sums"]

    # ----------------------------------------------------- 控制入口请求
    def insert_request(self, job_id: str, requested_at: float) -> int:
        with self.write() as conn:
            cur = conn.execute(
                "INSERT INTO checkpoint_requests(job_id, requested_at, status)"
                " VALUES(?, ?, 'pending')",
                (job_id, requested_at),
            )
            return int(cur.lastrowid)

    def take_pending_request(self) -> Optional[sqlite3.Row]:
        """运行者领取最老的 pending 请求；收集期间的重复请求留给下一轮。"""
        with self.write() as conn:
            row = conn.execute(
                "SELECT * FROM checkpoint_requests WHERE status='pending'"
                " ORDER BY req_id LIMIT 1"
            ).fetchone()
            if row is not None:
                conn.execute(
                    "UPDATE checkpoint_requests SET status='collecting' WHERE req_id=?",
                    (row["req_id"],),
                )
            return row

    def resolve_request(self, req_id: int, status: str, cp_id: Optional[int],
                        detail: Optional[str]) -> None:
        with self.write() as conn:
            conn.execute(
                "UPDATE checkpoint_requests SET status=?, cp_id=?, detail=? WHERE req_id=?",
                (status, cp_id, detail, req_id),
            )

    def request_status(self, req_id: int) -> Optional[dict[str, Any]]:
        row = self.conn.execute(
            "SELECT * FROM checkpoint_requests WHERE req_id=?", (req_id,)
        ).fetchone()
        if row is None:
            return None
        return {
            "req_id": int(row["req_id"]), "status": row["status"],
            "cp_id": row["cp_id"], "detail": row["detail"],
        }

    # ------------------------------------------------------------- 运行记录
    def start_run(self, job_id: str, pid: int, from_cp: Optional[int],
                  started_at: float) -> int:
        with self.write() as conn:
            cur = conn.execute(
                "INSERT INTO job_runs(job_id, started_at, pid, from_cp) VALUES(?, ?, ?, ?)",
                (job_id, started_at, pid, from_cp),
            )
            return int(cur.lastrowid)

    def end_run(self, run_id: int, ended_at: float, result: str, detail: str) -> None:
        with self.write() as conn:
            conn.execute(
                "UPDATE job_runs SET ended_at=?, result=?, detail=? WHERE run_id=?",
                (ended_at, result, detail, run_id),
            )

    def record_reads(self, run_id: int, source: str, seqs: Iterable[int],
                     replayed: bool = False) -> None:
        rows = [(run_id, source, int(s), int(replayed)) for s in seqs]
        if not rows:
            return
        with self.write() as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO run_reads(run_id, source, seq_value, replayed)"
                " VALUES(?, ?, ?, ?)", rows,
            )

    def record_node_counts(self, run_id: int, counts: dict[str, int]) -> None:
        with self.write() as conn:
            conn.executemany(
                "INSERT INTO run_node_counts(run_id, node, processed) VALUES(?, ?, ?)"
                " ON CONFLICT(run_id, node) DO UPDATE SET processed=excluded.processed",
                [(run_id, node, int(n)) for node, n in counts.items()],
            )

    def run_diagnostics(self, run_id: int) -> dict[str, Any]:
        rrun = self.conn.execute(
            "SELECT * FROM job_runs WHERE run_id=?", (run_id,)
        ).fetchone()
        if rrun is None:
            raise JobStoreError(f"运行 {run_id} 不存在")
        reads: dict[str, list[int]] = {}
        for r in self.conn.execute(
            "SELECT * FROM run_reads WHERE run_id=? ORDER BY source, seq_value", (run_id,)
        ):
            reads.setdefault(r["source"], []).append(int(r["seq_value"]))
        counts = {
            r["node"]: int(r["processed"])
            for r in self.conn.execute(
                "SELECT * FROM run_node_counts WHERE run_id=? ORDER BY node", (run_id,))
        }
        return {
            "run_id": run_id, "from_cp": rrun["from_cp"], "result": rrun["result"],
            "detail": rrun["detail"], "read_seqs": reads, "node_processed": counts,
        }


def _dump_item(item: Any) -> dict[str, Any]:
    if isinstance(item, Contribution):
        return {"t": "c", "v": json.loads(item.to_json())}
    from .model import Barrier, EndOfStream
    if isinstance(item, Barrier):
        return {"t": "b", "v": item.cp_id}
    if isinstance(item, EndOfStream):
        return {"t": "e"}
    raise TypeError(f"不可持久化的边记录: {item!r}")


def _load_item(d: dict[str, Any]):
    from .model import Barrier, EndOfStream
    kind = d["t"]
    if kind == "c":
        return Contribution.from_json(json.dumps(d["v"]))
    if kind == "b":
        return Barrier(int(d["v"]))
    if kind == "e":
        return EndOfStream()
    raise JobStoreError(f"未知边记录类型: {kind!r}")
