"""进程级互斥锁与只读源账本访问。"""

from __future__ import annotations

import fcntl
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator, Optional

from .model import SourceSpec


class RunLockHeld(Exception):
    """同一作业已有执行进程持锁。"""


class RunLock:
    """基于 fcntl 的排他执行锁。

    控制入口（请求检查点、查询）不获取该锁；只有运行者获取。
    进程退出（含被 SIGKILL）时内核自动释放文件锁，新进程可以明确接手，
    不会出现两个执行者同时跑同一作业。
    """

    def __init__(self, job_db_path: str):
        lock_dir = os.path.join(os.path.dirname(os.path.abspath(job_db_path)) or ".",
                                "flow_locks")
        os.makedirs(lock_dir, exist_ok=True)
        self.lock_path = os.path.join(lock_dir, os.path.basename(job_db_path) + ".run.lock")
        self._fh: Optional[int] = None

    def acquire(self) -> None:
        self._fh = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(self._fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(self._fh)
            self._fh = None
            holder = "未知"
            try:
                with open(self.lock_path, "r", encoding="utf-8") as fh:
                    holder = fh.read().strip() or holder
            except OSError:
                pass
            raise RunLockHeld(
                f"作业已有执行进程持锁（{holder}），另一执行者必须显式失败而不是同时接手"
            ) from exc
        os.ftruncate(self._fh, 0)
        os.write(self._fh, f"pid={os.getpid()}".encode("utf-8"))

    def release(self) -> None:
        if self._fh is None:
            return
        try:
            fcntl.flock(self._fh, fcntl.LOCK_UN)
        finally:
            os.close(self._fh)
            self._fh = None

    def __enter__(self) -> "RunLock":
        self.acquire()
        return self

    def __exit__(self, *exc) -> None:
        self.release()


@dataclass(frozen=True)
class LedgerRow:
    seq: int
    topic: str
    payload_len: int


class ReadOnlyLedger:
    """以只读模式打开现有 MQTT SQLite 账本，绝不写源库（含 WAL）。

    前提：源库历史不变、只可能追加。读取使用 ``mode=ro`` URI，任何误写都会报错。
    """

    def __init__(self, path: str):
        if not os.path.exists(path):
            raise FileNotFoundError(f"源账本不存在: {path}")
        uri = "file:" + os.path.abspath(path) + "?mode=ro"
        self.conn = sqlite3.connect(uri, uri=True, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        # 只读连接不能切换 journal_mode；保持源库原状。

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "ReadOnlyLedger":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def max_seq(self) -> int:
        row = self.conn.execute("SELECT COALESCE(MAX(seq), 0) AS m FROM ledger").fetchone()
        return int(row["m"])

    def iter_range(self, spec: SourceSpec, after_seq: int, end_seq: int,
                   batch_size: int = 256) -> Iterator[list[LedgerRow]]:
        """按 seq 升序分批产出 ``after_seq < seq <= end_seq`` 的全部行。

        主题过滤在读取侧完成；不过滤的行同样推进位置（seq 是账本物理次序）。
        调用方每批处理完再取下一批，业务数据不会随总输入量无限驻留内存。
        """
        from ..topic import topic_matches

        cursor = after_seq
        while cursor < end_seq:
            rows = self.conn.execute(
                "SELECT seq, topic, payload FROM ledger"
                " WHERE seq > ? AND seq <= ? ORDER BY seq LIMIT ?",
                (cursor, end_seq, batch_size),
            ).fetchall()
            if not rows:
                break
            batch: list[LedgerRow] = []
            for r in rows:
                cursor = int(r["seq"])
                if topic_matches(spec.topic_filter, r["topic"]):
                    batch.append(LedgerRow(
                        seq=cursor, topic=r["topic"],
                        payload_len=len(bytes(r["payload"])),
                    ))
            yield batch
