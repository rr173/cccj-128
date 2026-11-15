"""SQLite 持久化层。

设计要点：
- 所有写操作在 ``BEGIN IMMEDIATE`` 事务内进行，SQLite 保证同一时刻只有一个
  写事务，多线程/多进程并发访问通过文件锁 + busy_timeout 串行化；
- 事件账本 ``ledger`` 仅追加，seq 单调递增，即全局到达次序；
- ``retained`` 以主题为主键，同一主题永远只有一行（当前版本）；
- 订阅在单个写事务内完成：先确定截断序号，再在同一事务快照里读取保留值，
  因此并发发布不可能既漏进初始保留视图、又被当作普通实时事件重复投递。
"""

from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from typing import Any, Iterable, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    session_id        TEXT PRIMARY KEY,
    client_id         TEXT NOT NULL,
    connected         INTEGER NOT NULL,
    session_present   INTEGER NOT NULL,
    clean_session     INTEGER NOT NULL,
    keep_alive        INTEGER NOT NULL,
    connected_seq     INTEGER,
    disconnected_seq  INTEGER,
    created_at        REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS ledger (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id   TEXT NOT NULL,
    topic        TEXT NOT NULL,
    payload      BLOB NOT NULL,
    qos          INTEGER NOT NULL,
    retain       INTEGER NOT NULL,
    dup          INTEGER NOT NULL,
    message_id   INTEGER,
    ingested_at  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ledger_topic   ON ledger(topic);
CREATE INDEX IF NOT EXISTS idx_ledger_msgid   ON ledger(session_id, message_id);
CREATE INDEX IF NOT EXISTS idx_ledger_seq     ON ledger(seq);

CREATE TABLE IF NOT EXISTS retained (
    topic             TEXT PRIMARY KEY,
    payload           BLOB NOT NULL,
    qos               INTEGER NOT NULL,
    source_session_id TEXT NOT NULL,
    message_id        INTEGER,
    version           INTEGER NOT NULL,
    last_event_seq    INTEGER NOT NULL,
    updated_at        REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS subscriptions (
    sub_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id        TEXT NOT NULL,
    message_id        INTEGER,
    created_cutoff_seq INTEGER NOT NULL,
    created_at        REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS subscription_filters (
    sub_id   INTEGER NOT NULL REFERENCES subscriptions(sub_id),
    filter   TEXT NOT NULL,
    PRIMARY KEY (sub_id, filter)
);

CREATE TABLE IF NOT EXISTS subscription_retained (
    sub_id            INTEGER NOT NULL REFERENCES subscriptions(sub_id),
    topic             TEXT NOT NULL,
    payload           BLOB NOT NULL,
    qos               INTEGER NOT NULL,
    retain_version    INTEGER NOT NULL,
    source_event_seq  INTEGER NOT NULL,
    delivered         INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (sub_id, topic)
);

CREATE TABLE IF NOT EXISTS quarantine (
    quar_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    reason      TEXT NOT NULL,
    detail      TEXT NOT NULL,
    frame_type  TEXT,
    session_id  TEXT,
    topic       TEXT,
    raw_json    TEXT NOT NULL,
    created_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS delivery_cursor (
    sub_id          INTEGER PRIMARY KEY REFERENCES subscriptions(sub_id),
    last_event_seq  INTEGER NOT NULL
);
"""


def _dump_raw(raw: Any) -> str:
    """把原始帧记录序列化为 JSON；bytes 以 hex 包装，保证可还原。"""

    def default(obj: Any) -> Any:
        if isinstance(obj, (bytes, bytearray, memoryview)):
            return {"__bytes_hex__": bytes(obj).hex()}
        return repr(obj)

    return json.dumps(raw, ensure_ascii=False, sort_keys=True, default=default)


class MqttStore:
    """单库访问对象。线程安全依赖 SQLite 文件锁；调用方可用 :meth:`write` 加锁。"""

    def __init__(self, path: str, busy_timeout_ms: int = 10000):
        self.path = path
        # check_same_thread=False：锁由 SQLite 自身（文件锁）负责，
        # 接入层另外有进程内 RLock 做第一道串行化。
        self.conn = sqlite3.connect(
            path, check_same_thread=False, isolation_level=None, timeout=busy_timeout_ms / 1000
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)}")
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(SCHEMA)

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "MqttStore":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    @contextmanager
    def write(self) -> Iterable[sqlite3.Connection]:
        """打开一个立即写事务；提交/回滚由上下文自动处理。"""
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    # ---------------------------------------------------------------- sessions
    def upsert_session(
        self,
        conn: sqlite3.Connection,
        session_id: str,
        client_id: str,
        session_present: bool,
        clean_session: bool,
        keep_alive: int,
        current_seq: Optional[int],
    ) -> None:
        now = time.time()
        conn.execute(
            """
            INSERT INTO sessions(session_id, client_id, connected, session_present,
                                 clean_session, keep_alive, connected_seq,
                                 disconnected_seq, created_at)
            VALUES (?, ?, 1, ?, ?, ?, ?, NULL, ?)
            ON CONFLICT(session_id) DO UPDATE SET
                connected=1,
                session_present=excluded.session_present,
                clean_session=excluded.clean_session,
                keep_alive=excluded.keep_alive,
                connected_seq=excluded.connected_seq,
                disconnected_seq=NULL
            """,
            (session_id, client_id, int(session_present), int(clean_session),
             keep_alive, current_seq, now),
        )

    def mark_disconnected(self, conn: sqlite3.Connection, session_id: str, seq: Optional[int]) -> None:
        conn.execute(
            "UPDATE sessions SET connected=0, disconnected_seq=? WHERE session_id=?",
            (seq, session_id),
        )

    def get_session(self, session_id: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM sessions WHERE session_id=?", (session_id,)
        ).fetchone()

    def max_ledger_seq(self, conn: Optional[sqlite3.Connection] = None) -> int:
        conn = conn or self.conn
        row = conn.execute("SELECT COALESCE(MAX(seq), 0) AS m FROM ledger").fetchone()
        return int(row["m"])

    # ----------------------------------------------------------------- ledger
    def insert_event(
        self,
        conn: sqlite3.Connection,
        *,
        session_id: str,
        topic: str,
        payload: bytes,
        qos: int,
        retain: bool,
        dup: bool,
        message_id: Optional[int],
    ) -> int:
        cur = conn.execute(
            """
            INSERT INTO ledger(session_id, topic, payload, qos, retain, dup,
                               message_id, ingested_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (session_id, topic, bytes(payload), qos, int(retain), int(dup),
             message_id, time.time()),
        )
        return int(cur.lastrowid)

    def list_events(self, limit: Optional[int] = None) -> list[sqlite3.Row]:
        sql = "SELECT * FROM ledger ORDER BY seq"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        return list(self.conn.execute(sql))

    def get_event(self, seq: int) -> Optional[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM ledger WHERE seq=?", (seq,)).fetchone()

    def find_events_by_message_id(self, session_id: str, message_id: int) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT * FROM ledger WHERE session_id=? AND message_id=? ORDER BY seq",
                (session_id, message_id),
            )
        )

    # --------------------------------------------------------------- retained
    def all_retained(self, conn: sqlite3.Connection) -> list[sqlite3.Row]:
        return list(conn.execute("SELECT * FROM retained ORDER BY topic"))

    def upsert_retained(
        self,
        conn: sqlite3.Connection,
        *,
        topic: str,
        payload: bytes,
        qos: int,
        source_session_id: str,
        message_id: Optional[int],
        event_seq: int,
    ) -> None:
        now = time.time()
        conn.execute(
            """
            INSERT INTO retained(topic, payload, qos, source_session_id, message_id,
                                 version, last_event_seq, updated_at)
            VALUES (?, ?, ?, ?, ?, 1, ?, ?)
            ON CONFLICT(topic) DO UPDATE SET
                payload=excluded.payload,
                qos=excluded.qos,
                source_session_id=excluded.source_session_id,
                message_id=excluded.message_id,
                version=retained.version + 1,
                last_event_seq=excluded.last_event_seq,
                updated_at=excluded.updated_at
            """,
            (topic, bytes(payload), qos, source_session_id, message_id, event_seq, now),
        )

    def clear_retained(self, conn: sqlite3.Connection, topic: str, event_seq: int) -> int:
        cur = conn.execute("DELETE FROM retained WHERE topic=?", (topic,))
        return int(cur.rowcount)

    def get_retained(self, topic: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM retained WHERE topic=?", (topic,)
        ).fetchone()

    def list_retained(self) -> list[sqlite3.Row]:
        return list(self.conn.execute("SELECT * FROM retained ORDER BY topic"))

    # ------------------------------------------------------------ subscriptions
    def insert_subscription(
        self,
        conn: sqlite3.Connection,
        *,
        session_id: str,
        message_id: Optional[int],
        cutoff_seq: int,
        filters: Iterable[str],
        retained_rows: Iterable[sqlite3.Row],
    ) -> int:
        now = time.time()
        cur = conn.execute(
            """
            INSERT INTO subscriptions(session_id, message_id, created_cutoff_seq, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (session_id, message_id, cutoff_seq, now),
        )
        sub_id = int(cur.lastrowid)
        conn.executemany(
            "INSERT INTO subscription_filters(sub_id, filter) VALUES (?, ?)",
            [(sub_id, f) for f in filters],
        )
        conn.executemany(
            """
            INSERT INTO subscription_retained(sub_id, topic, payload, qos,
                                              retain_version, source_event_seq)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (sub_id, r["topic"], bytes(r["payload"]), r["qos"],
                 r["version"], r["last_event_seq"])
                for r in retained_rows
            ],
        )
        # 首次轮询游标停在截断序号；实时事件只取 seq > cutoff。
        conn.execute(
            "INSERT INTO delivery_cursor(sub_id, last_event_seq) VALUES (?, ?)",
            (sub_id, cutoff_seq),
        )
        return sub_id

    def get_subscription(self, sub_id: int) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM subscriptions WHERE sub_id=?", (sub_id,)
        ).fetchone()

    def get_filters(self, sub_id: int) -> list[str]:
        return [
            r["filter"]
            for r in self.conn.execute(
                "SELECT filter FROM subscription_filters WHERE sub_id=? ORDER BY rowid",
                (sub_id,),
            )
        ]

    def get_cursor(self, sub_id: int) -> Optional[int]:
        row = self.conn.execute(
            "SELECT last_event_seq FROM delivery_cursor WHERE sub_id=?", (sub_id,)
        ).fetchone()
        return None if row is None else int(row["last_event_seq"])

    def advance_cursor(self, conn: sqlite3.Connection, sub_id: int, seq: int) -> None:
        conn.execute(
            "UPDATE delivery_cursor SET last_event_seq=? WHERE sub_id=? AND last_event_seq<?",
            (seq, sub_id, seq),
        )

    def pending_retained(self, sub_id: int) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT * FROM subscription_retained WHERE sub_id=? AND delivered=0 ORDER BY topic",
                (sub_id,),
            )
        )

    def mark_retained_delivered(self, conn: sqlite3.Connection, sub_id: int, topics: Iterable[str]) -> None:
        conn.executemany(
            "UPDATE subscription_retained SET delivered=1 WHERE sub_id=? AND topic=?",
            [(sub_id, t) for t in topics],
        )

    def live_events_after(self, after_seq: int, limit: Optional[int]) -> list[sqlite3.Row]:
        sql = "SELECT * FROM ledger WHERE seq > ? ORDER BY seq"
        params: list[Any] = [after_seq]
        if limit is not None:
            sql += " LIMIT ?"
            params.append(int(limit))
        return list(self.conn.execute(sql, params))

    # --------------------------------------------------------------- quarantine
    def quarantine(
        self,
        *,
        reason: str,
        detail: str,
        raw: Any,
        frame_type: Optional[str] = None,
        session_id: Optional[str] = None,
        topic: Optional[str] = None,
    ) -> int:
        with self.write() as conn:
            cur = conn.execute(
                """
                INSERT INTO quarantine(reason, detail, frame_type, session_id, topic,
                                       raw_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (reason, detail, frame_type, session_id, topic, _dump_raw(raw), time.time()),
            )
            return int(cur.lastrowid)

    def list_quarantine(self) -> list[sqlite3.Row]:
        return list(self.conn.execute("SELECT * FROM quarantine ORDER BY quar_id"))

    def count_quarantine(self) -> int:
        return int(self.conn.execute("SELECT COUNT(*) AS c FROM quarantine").fetchone()["c"])
