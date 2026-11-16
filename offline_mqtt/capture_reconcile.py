"""捕获链对账（离线、只读来源）。

把两份**已封存**的 MQTT 采集账本（左/右两端）与两份观测 JSONL 变成一份可离线
复核的“消息对应证据”。不新建订阅、不修改来源库、不改授权、不连接任何代理。

合同（仅适用于具有下列顺序保证的捕获）
======================================
- 比较对象是一条有序转发链：每次独立发布至多在另一端对应一次；发布先后不
  倒序；允许丢失和单侧多出。**本模块不宣称任意 MQTT 部署都具备这项性质。**
- 配对只发生在“根发布”之间。内容条件**只有** ``topic`` 原字符串、``payload``
  原字节、``retain`` 相等；不做 JSON 解析、Unicode 规范化或文本转码。
  ``qos`` / ``message_id`` / ``session_id`` 是端内证据，不要求跨端一致。
- 时间条件：存在**同一个**整数 ``delta ∈ [a, b]``，对每一对都满足
  ``u ≤ R + delta − L ≤ v``。同一配对方案内所有边共用这个 delta。
- 配对方案是根之间的非交叉一对一关系：左端次序递增时右端次序严格递增。
- 未配对的根允许留下。

问题：求“在声明条件下能解释的最多发布数” K，并给出完整的最优 delta 集合、
每个根的全部可能对端、是否可以不配、以及哪些对是 forced。具体精确算法与
“为什么不会拼凑局部方案”的论证见 :mod:`offline_mqtt.capture_match`。

关于重传
========
观测里的 ``retry_of`` 是采集方给出的**确切**重传证据：只能指向本端更早、
``retry_of=null`` 的根记录，且两行的 ``capture_epoch`` / ``session_id`` /
``qos`` / ``message_id`` / ``topic`` / ``payload`` 字节 / ``retain`` 必须相同，
``qos`` 为 1 或 2，重传行 ``dup`` 必须为真。根与所有这样的重传构成**一次**
发布，使用根的时间与次序，并保留全部原始行定位。链式 ``retry_of`` 拒绝。

``retry_of=null`` 的每一行都是独立发布——即使 ``dup`` 为真、包号/主题/正文
完全相同，**也不擅自合并**；跨 epoch 的包号复用同样不合并。没有明确引用的
单侧重复只能作为独立业务出现参与对账，本模块不声称已证明它是网络重传。

来源读取
========
来源库通过 ``file:...?mode=ro`` **只读连接**打开，只执行 ``PRAGMA table_info``
与 ``SELECT``；不调用会建表或改日志模式的 :class:`offline_mqtt.store.MqttStore`。

规模边界
========
- 单端根发布上限 :data:`MAX_ROOTS_PER_SIDE`，原始行上限
  :data:`MAX_RAW_ROWS_PER_SIDE`（整份已封存捕获可装入内存）。
- delta 切点段 / 最优段 / 分析缓存上限见 :mod:`offline_mqtt.capture_match`。
- 任一上限触发都抛 :class:`ReconcileResourceLimit`，明确“未能完成”，绝不输出
  貌似完整的 K 或确定缺口。
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import sqlite3
from dataclasses import dataclass
from typing import Any, Optional

from . import capture_match
from .capture_match import MatchResourceLimit

# ---------------------------------------------------------------- 资源上限
MAX_ROOTS_PER_SIDE = 5000
MAX_RAW_ROWS_PER_SIDE = 200_000
MAX_EXPLAIN_SCENARIOS = 8         # explain 为一个根最多重建的方案数

REPORT_FORMAT = "offline_mqtt.capture_chain_reconciliation/v1"
MANIFEST_VERSION = 1

REQUIRED_LEDGER_COLUMNS = (
    "seq", "session_id", "topic", "payload", "qos", "retain", "dup",
    "message_id",
)

CONTRACT = {
    "comparison_object": "一条有序转发链（每次独立发布至多在另一端对应一次，"
                         "发布先后不颠倒；允许丢失与单侧多出）",
    "scope_note": "仅适用于具有该顺序保证的捕获；不宣称任意 MQTT 部署都有此性质",
    "pairing": "根发布之间、非交叉的一对一关系（左端次序递增 ⇒ 右端次序严格递增）",
    "content_equality": ["topic:原字符串", "payload:原字节", "retain:相等"],
    "not_used_for_matching": ["qos", "message_id", "session_id"],
    "time_model": "同一次比较只有一个共同整数 delta∈offset_ns 加在右端时钟上；"
                  "每对满足 transit_ns[0] ≤ R+delta−L ≤ transit_ns[1]",
    "retransmission_rule": "retry_of 必须指向本端更早、retry_of=null 的根，"
                           "capture_epoch/session_id/qos/message_id/topic/payload"
                           "字节/retain 全同，qos∈{1,2}，重传行 dup=true；"
                           "根与重传折叠为一次发布；拒绝链式 retry_of",
    "independent_duplicates": "retry_of=null 的每一行都是独立发布，即使 dup=true "
                              "或包号/主题/正文相同也不合并",
    "unmatched_meaning": "no_counterpart_in_capture 仅说明指定捕获与合同内没有对应"
                         "证据，不等于证明网络真实丢包",
}


# ------------------------------------------------------------------ 错误类型
class ReconcileError(ValueError):
    """对账错误的基类，携带带位置的错误列表。"""

    kind = "reconcile_error"

    def __init__(self, errors: list[dict], summary: str = "捕获链对账失败"):
        self.errors = errors
        detail = "; ".join(
            f"[{e.get('location', '?')}] {e.get('reason', '?')}: {e.get('detail', '')}"
            for e in errors
        )
        super().__init__(f"{summary}（{len(errors)} 条）：{detail}")


class ReconcileInputError(ReconcileError):
    """调用方输入或来源账本不合法。"""

    kind = "reconcile_input_error"


class ReconcileResourceLimit(ReconcileError):
    """触及明确资源上限，无法给出完整结论。"""

    kind = "resource_limit"


class ReportExistsError(ReconcileError):
    """目标报告已存在；绝不覆盖旧报告。"""

    kind = "report_exists"


# ---------------------------------------------------------------- 数据结构
@dataclass(frozen=True)
class CaptureSpec:
    capture_id: str
    db_path: str
    observations_path: str
    db_rel: str
    observations_rel: str


@dataclass(frozen=True)
class Root:
    """一次发布：根行 + 全部确切重传行（折叠后）。"""

    side: str
    index: int
    seq: int
    capture_epoch: str
    observed_ns: int
    session_id: str
    qos: int
    message_id: Optional[int]
    topic: str
    payload: bytes
    retain: bool
    raw_seqs: tuple
    capture_id: str

    @property
    def content_key(self) -> tuple:
        return (self.topic, self.payload, self.retain)

    @property
    def retry_seqs(self) -> tuple:
        return self.raw_seqs[1:]


@dataclass
class SideCapture:
    side: str
    capture_id: str
    spec: CaptureSpec
    db_sha256: str
    observations_sha256: str
    raw_rows: list
    roots: list
    raw_row_count: int

    @property
    def root_count(self) -> int:
        return len(self.roots)

    @property
    def retransmission_count(self) -> int:
        return sum(len(r.raw_seqs) - 1 for r in self.roots)


# --------------------------------------------------------------- 字节工具
def payload_evidence(payload: bytes) -> dict:
    """正文原字节的可复核表达：hex、长度，以及可解码时的 UTF-8 文本。"""
    data = bytes(payload)
    out = {
        "encoding": "hex",
        "hex": data.hex(),
        "length": len(data),
        "base64": base64.b64encode(data).decode("ascii"),
    }
    try:
        out["utf8"] = data.decode("utf-8")
    except UnicodeDecodeError:
        out["utf8"] = None
    return out


def _sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


# ------------------------------------------------------- 严格 JSON / JSONL
def _reject_dup_pairs(pairs):  # object_pairs_hook
    seen = set()
    out = {}
    for key, value in pairs:
        if key in seen:
            raise ValueError(f"重复 JSON 键 {key!r}")
        seen.add(key)
        out[key] = value
    return out


def load_manifest(manifest_path: str) -> dict:
    """解析并严格校验输入清单；相对路径按清单所在目录解析，错误定位到文件。"""
    errors: list[dict] = []
    manifest_file = os.path.abspath(manifest_path)

    def mloc(name: str) -> str:
        return f"{manifest_file}:{name}"
    try:
        with open(manifest_path, "r", encoding="utf-8") as fh:
            text = fh.read()
    except OSError as exc:
        raise ReconcileInputError([{
            "location": "manifest", "reason": "CANNOT_READ_FILE",
            "detail": f"{manifest_path}: {exc}"}])
    try:
        raw = json.loads(text, object_pairs_hook=_reject_dup_pairs)
    except ValueError as exc:
        raise ReconcileInputError([{
            "location": "manifest", "reason": "BAD_JSON",
            "detail": f"{manifest_path}: {exc}"}])

    if not isinstance(raw, dict):
        raise ReconcileInputError([{
            "location": "manifest", "reason": "NOT_AN_OBJECT",
            "detail": "清单必须是 JSON 对象"}])

    base = os.path.dirname(os.path.abspath(manifest_path))

    def resolve(rel: str) -> str:
        return rel if os.path.isabs(rel) else os.path.normpath(os.path.join(base, rel))

    required = {"version", "left", "right", "offset_ns", "transit_ns"}
    extra = set(raw) - required
    missing = required - set(raw)
    if extra:
        errors.append({"location": mloc("root"), "reason": "EXTRA_FIELD",
                       "detail": f"未知字段: {sorted(extra)}"})
    if missing:
        errors.append({"location": mloc("root"), "reason": "MISSING_FIELD",
                       "detail": f"缺字段: {sorted(missing)}"})
    if raw.get("version") != MANIFEST_VERSION or isinstance(raw.get("version"), bool):
        errors.append({"location": mloc("version"), "reason": "BAD_VERSION",
                       "detail": f"version 必须为整数 {MANIFEST_VERSION}"})

    captures: dict[str, CaptureSpec] = {}
    for side in ("left", "right"):
        entry = raw.get(side)
        loc = mloc(side)
        if not isinstance(entry, dict):
            errors.append({"location": loc, "reason": "NOT_AN_OBJECT",
                           "detail": f"{side} 必须是对象"})
            continue
        need = {"capture_id", "ledger", "observations"}
        extra2 = set(entry) - need
        missing2 = need - set(entry)
        if extra2:
            errors.append({"location": loc, "reason": "EXTRA_FIELD",
                           "detail": f"未知字段: {sorted(extra2)}"})
        if missing2:
            errors.append({"location": loc, "reason": "MISSING_FIELD",
                           "detail": f"缺字段: {sorted(missing2)}"})
        cid = entry.get("capture_id")
        ok_id = isinstance(cid, str) and cid != ""
        if not ok_id:
            errors.append({"location": f"{loc}.capture_id", "reason": "BAD_CAPTURE_ID",
                           "detail": "capture_id 必须是非空字符串"})
        for key in ("ledger", "observations"):
            if not isinstance(entry.get(key), str) or entry.get(key) == "":
                errors.append({"location": f"{loc}.{key}", "reason": "BAD_PATH",
                               "detail": f"{key} 必须是非空路径字符串"})
        if not ok_id:
            continue
        ledger_rel = entry.get("ledger", "")
        obs_rel = entry.get("observations", "")
        captures[side] = CaptureSpec(
            capture_id=cid,
            db_path=resolve(ledger_rel),
            observations_path=resolve(obs_rel),
            db_rel=ledger_rel,
            observations_rel=obs_rel,
        )

    if len(captures) == 2 and captures["left"].capture_id == captures["right"].capture_id:
        errors.append({"location": mloc("capture_id"), "reason": "DUPLICATE_CAPTURE_ID",
                       "detail": "left/right 的 capture_id 必须不同"})

    offset = _interval(raw.get("offset_ns"), "offset_ns", mloc("offset_ns"),
                       errors, nonneg=False)
    transit = _interval(raw.get("transit_ns"), "transit_ns", mloc("transit_ns"),
                        errors, nonneg=True)
    if errors:
        raise ReconcileInputError(errors)
    return {
        "path": os.path.abspath(manifest_path),
        "sha256": _sha256_file(manifest_path),
        "captures": captures,
        "offset_ns": offset,
        "transit_ns": transit,
    }


def _is_int(value: Any) -> bool:
    """严格整数：拒绝布尔值（``bool`` 是 ``int`` 的子类，必须单独排除）。"""
    return isinstance(value, int) and not isinstance(value, bool)


def _interval(value: Any, name: str, location: str,
              errors: list[dict], *, nonneg: bool) -> Optional[tuple]:
    if not isinstance(value, list) or len(value) != 2:
        errors.append({"location": location, "reason": "BAD_INTERVAL",
                       "detail": f"{name} 必须是含两个整数的数组"})
        return None
    lo, hi = value
    if not (_is_int(lo) and _is_int(hi)):
        errors.append({"location": location, "reason": "BOOL_OR_NON_INT",
                       "detail": f"{name} 的端点必须是整数（不接受布尔值）"})
        return None
    if lo > hi:
        errors.append({"location": location, "reason": "INTERVAL_REVERSED",
                       "detail": f"{name}=[{lo},{hi}] 下界大于上界"})
        return None
    if nonneg and lo < 0:
        errors.append({"location": location, "reason": "INTERVAL_NEGATIVE",
                       "detail": f"{name}=[{lo},{hi}] 必须满足 0<=u<=v"})
        return None
    return (lo, hi)


# -------------------------------------------------------- 只读 ledger 访问
def _open_ledger_readonly(path: str, location: str, errors: list[dict]):
    if not os.path.isfile(path):
        errors.append({"location": location, "reason": "DB_NOT_FOUND",
                       "detail": f"来源库不存在: {path}"})
        return None
    uri = "file:" + os.path.abspath(path) + "?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True)
    except sqlite3.Error as exc:
        errors.append({"location": location, "reason": "DB_CANNOT_OPEN",
                       "detail": f"{path}: {exc}"})
        return None
    conn.row_factory = sqlite3.Row
    return conn


def _load_ledger(path: str, side: str, errors: list[dict]) -> Optional[list]:
    location = f"{side}.ledger"
    conn = _open_ledger_readonly(path, location, errors)
    if conn is None:
        return None
    try:
        try:
            cols = {row["name"] for row in conn.execute("PRAGMA table_info(ledger)")}
        except sqlite3.Error as exc:
            errors.append({"location": location, "reason": "LEDGER_UNREADABLE",
                           "detail": f"{path}: {exc}"})
            return None
        if not cols:
            errors.append({"location": location, "reason": "LEDGER_MISSING",
                           "detail": f"{path} 中不存在 ledger 表"})
            return None
        missing = set(REQUIRED_LEDGER_COLUMNS) - cols
        if missing:
            errors.append({"location": location, "reason": "LEDGER_SCHEMA_MISMATCH",
                           "detail": f"ledger 缺列: {sorted(missing)}"})
            return None
        try:
            return [dict(r) for r in conn.execute(
                "SELECT seq, session_id, topic, payload, qos, retain, dup, "
                "message_id FROM ledger ORDER BY seq")]
        except sqlite3.Error as exc:
            errors.append({"location": location, "reason": "LEDGER_READ_FAILED",
                           "detail": f"{path}: {exc}"})
            return None
    finally:
        conn.close()


# ------------------------------------------------------------ 观测与折叠
_OBS_KEYS = {"event_seq", "capture_epoch", "observed_ns", "retry_of"}


def _read_observations(path: str, location: str, errors: list[dict]) -> list:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            lines = fh.read().splitlines()
    except OSError as exc:
        errors.append({"location": location, "reason": "CANNOT_READ_FILE",
                       "detail": f"{path}: {exc}"})
        return []
    rows = []
    for lineno, line in enumerate(lines, start=1):
        if line.strip() == "":
            continue
        try:
            rows.append((lineno, json.loads(line, object_pairs_hook=_reject_dup_pairs)))
        except ValueError as exc:
            errors.append({"location": f"{location}:{lineno}",
                           "reason": "BAD_JSON_LINE",
                           "detail": f"{path} 第 {lineno} 行: {exc}"})
    return rows


def _fold_capture(side: str, spec: CaptureSpec, ledger_rows: list,
                  errors: list[dict]) -> Optional[list]:
    """把一端 ledger + 观测折叠为根发布列表；任何违规都追加带位置错误。"""
    loc = f"{side}.observations"
    obs_rows = _read_observations(spec.observations_path, loc, errors)
    by_seq: dict[int, dict] = {}
    line_of: dict[int, int] = {}
    order: list[int] = []

    for lineno, row in obs_rows:
        rowloc = f"{loc}:{lineno}"
        if not isinstance(row, dict):
            errors.append({"location": rowloc, "reason": "NOT_AN_OBJECT",
                           "detail": "观测行必须是 JSON 对象"})
            continue
        extra = set(row) - _OBS_KEYS
        missing = _OBS_KEYS - set(row)
        if extra or missing:
            parts = []
            if missing:
                parts.append(f"缺字段 {sorted(missing)}")
            if extra:
                parts.append(f"额外字段 {sorted(extra)}")
            errors.append({"location": rowloc, "reason": "OBSERVATION_FIELDS",
                           "detail": "；".join(parts)})
            continue
        seq = row["event_seq"]
        if not _is_int(seq):
            errors.append({"location": rowloc, "reason": "BOOL_OR_NON_INT",
                           "detail": "event_seq 必须是整数（不接受布尔值）"})
            continue
        epoch = row["capture_epoch"]
        if not isinstance(epoch, str) or epoch == "":
            errors.append({"location": rowloc, "reason": "BAD_CAPTURE_EPOCH",
                           "detail": "capture_epoch 必须是非空字符串"})
            continue
        observed = row["observed_ns"]
        if not _is_int(observed):
            errors.append({"location": rowloc, "reason": "BOOL_OR_NON_INT",
                           "detail": "observed_ns 必须是有符号整数纳秒（不接受布尔值）"})
            continue
        retry_of = row["retry_of"]
        if retry_of is not None and not _is_int(retry_of):
            errors.append({"location": rowloc, "reason": "BAD_RETRY_OF",
                           "detail": "retry_of 必须是 null 或本端整数 event_seq"})
            continue
        if seq in by_seq:
            errors.append({"location": rowloc, "reason": "DUPLICATE_EVENT_SEQ",
                           "detail": f"event_seq={seq} 重复出现"})
            continue
        by_seq[seq] = {"capture_epoch": epoch, "observed_ns": observed,
                       "retry_of": retry_of}
        line_of[seq] = lineno
        order.append(seq)

    ledger_by_seq = {int(r["seq"]): r for r in ledger_rows}
    for seq in order:
        if seq not in ledger_by_seq:
            errors.append({"location": f"{loc}:{line_of[seq]}",
                           "reason": "UNKNOWN_EVENT_SEQ",
                           "detail": f"event_seq={seq} 不在 {side} ledger 中"})
    for row in ledger_rows:
        seq = int(row["seq"])
        if seq not in by_seq:
            errors.append({"location": loc, "reason": "MISSING_OBSERVATION",
                           "detail": f"{side} ledger.seq={seq} 没有对应观测"})
    if errors:
        return None

    # 以 ledger.seq 为本端顺序（不是文件顺序、也不是 ingested_at）：
    # 按 seq 升序观察时，observed_ns 必须不下降。
    last_observed = None
    for seq in sorted(order):
        rowloc = f"{loc}:{line_of[seq]}"
        observed = by_seq[seq]["observed_ns"]
        if last_observed is not None and observed < last_observed:
            errors.append({"location": rowloc, "reason": "OBSERVED_TIME_DECREASE",
                           "detail": f"event_seq={seq} 的 observed_ns={observed} 小于"
                                     f"seq 更早一行的 {last_observed}"
                                     f"（顺序以 ledger.seq 为准，不拿 ingested_at 代替）"})
        last_observed = observed
    if errors:
        return None

    # 重传：retry_of 必须指向更早、retry_of=null 的根，身份字段全同。
    children: dict[int, list[int]] = {}
    for seq in order:
        target = by_seq[seq]["retry_of"]
        if target is None:
            continue
        rowloc = f"{loc}:{line_of[seq]}"
        if target not in by_seq:
            errors.append({"location": rowloc, "reason": "RETRY_TARGET_UNKNOWN",
                           "detail": f"retry_of={target} 不是本端观测中的 event_seq"})
            continue
        if target >= seq:
            errors.append({"location": rowloc, "reason": "RETRY_NOT_EARLIER",
                           "detail": f"retry_of={target} 不早于本行 event_seq={seq}"})
            continue
        if by_seq[target]["retry_of"] is not None:
            errors.append({"location": rowloc, "reason": "RETRY_CHAIN",
                           "detail": f"retry_of={target} 本身也是重传行；不接受链式 retry_of"})
            continue
        child = ledger_by_seq[seq]
        root = ledger_by_seq[target]
        same = (
            by_seq[seq]["capture_epoch"] == by_seq[target]["capture_epoch"]
            and child["session_id"] == root["session_id"]
            and int(child["qos"]) == int(root["qos"])
            and child["message_id"] == root["message_id"]
            and child["topic"] == root["topic"]
            and bytes(child["payload"]) == bytes(root["payload"])
            and bool(child["retain"]) == bool(root["retain"])
        )
        if not same:
            errors.append({"location": rowloc, "reason": "RETRY_IDENTITY_MISMATCH",
                           "detail": f"重传行 {seq} 与根 {target} 的 capture_epoch/"
                                     f"session_id/qos/message_id/topic/payload/retain "
                                     f"必须全部相同"})
            continue
        if int(child["qos"]) not in (1, 2):
            errors.append({"location": rowloc, "reason": "RETRY_QOS_NOT_PERSISTENT",
                           "detail": f"重传行 {seq} 的 qos={child['qos']}；重传只允许 QoS 1/2"})
            continue
        if not bool(child["dup"]):
            errors.append({"location": rowloc, "reason": "RETRY_DUP_NOT_SET",
                           "detail": f"重传行 {seq} 的 dup 不为真"})
            continue
        children.setdefault(target, []).append(seq)
    if errors:
        return None

    roots: list[Root] = []
    # 端内次序以 ledger.seq 为准（不是观测文件的行顺序）。
    for seq in sorted(order):
        if by_seq[seq]["retry_of"] is not None:
            continue
        row = ledger_by_seq[seq]
        raw_seqs = (seq,) + tuple(sorted(children.get(seq, ())))
        roots.append(Root(
            side=side, index=len(roots), seq=seq,
            capture_epoch=by_seq[seq]["capture_epoch"],
            observed_ns=by_seq[seq]["observed_ns"],
            session_id=row["session_id"], qos=int(row["qos"]),
            message_id=row["message_id"], topic=row["topic"],
            payload=bytes(row["payload"]), retain=bool(row["retain"]),
            raw_seqs=raw_seqs, capture_id=spec.capture_id,
        ))
    accounted = sum(len(r.raw_seqs) for r in roots)
    if accounted != len(order):
        errors.append({"location": loc, "reason": "ROW_ACCOUNTING",
                       "detail": "重传折叠后的原始行数与观测行数不一致"})
        return None
    return roots


def load_capture(side: str, spec: CaptureSpec) -> SideCapture:
    """读取并校验一端来源（只读 ledger + 观测），返回折叠后的捕获。"""
    errors: list[dict] = []
    ledger_rows = _load_ledger(spec.db_path, side, errors)
    if errors or ledger_rows is None:
        raise ReconcileInputError(errors, f"{side} 来源读取失败")
    if len(ledger_rows) > MAX_RAW_ROWS_PER_SIDE:
        raise ReconcileResourceLimit([{
            "location": f"{side}.ledger", "reason": "RAW_ROWS_LIMIT",
            "detail": f"原始行 {len(ledger_rows)} 超过上限 {MAX_RAW_ROWS_PER_SIDE}"}])
    roots = _fold_capture(side, spec, ledger_rows, errors)
    if errors or roots is None:
        raise ReconcileInputError(errors, f"{side} 观测校验失败")
    if len(roots) > MAX_ROOTS_PER_SIDE:
        raise ReconcileResourceLimit([{
            "location": f"{side}.observations", "reason": "ROOTS_LIMIT",
            "detail": f"独立发布 {len(roots)} 超过上限 {MAX_ROOTS_PER_SIDE}"}])
    return SideCapture(
        side=side, capture_id=spec.capture_id, spec=spec,
        db_sha256=_sha256_file(spec.db_path),
        observations_sha256=_sha256_file(spec.observations_path),
        raw_rows=ledger_rows, roots=roots, raw_row_count=len(ledger_rows),
    )


# ------------------------------------------------------------ 报告构建
def _root_evidence(root: Root) -> dict:
    return {
        "capture_id": root.capture_id,
        "side": root.side,
        "root_index": root.index,
        "root_seq": root.seq,
        "capture_epoch": root.capture_epoch,
        "observed_ns": root.observed_ns,
        "session_id": root.session_id,
        "qos": root.qos,
        "message_id": root.message_id,
        "topic": root.topic,
        "payload": payload_evidence(root.payload),
        "retain": root.retain,
        "raw_rows": [
            {"seq": seq,
             "role": "root" if seq == root.seq else "retransmission",
             "retry_of": None if seq == root.seq else root.seq}
            for seq in root.raw_seqs
        ],
    }


def _accounting(side: SideCapture, independent: int) -> dict:
    return {
        "capture_id": side.capture_id,
        "raw_rows": side.raw_row_count,
        "retransmission_rows": side.retransmission_count,
        "independent_publications": independent,
        "folded_rows_checked": sum(len(r.raw_seqs) for r in side.roots),
    }


def _root_relations(left: SideCapture, right: SideCapture,
                    result: capture_match.MatchResult) -> tuple[list, list]:
    """每端原始行 → 根 / 重传关系 / 对账结论（每根只消耗一次）。"""
    left_out = []
    for root in left.roots:
        candidates = result.left_candidates[root.index]
        left_out.append({
            **_root_evidence(root),
            "counterpart_capture_id": right.capture_id,
            "candidate_root_positions": list(candidates),
            "candidate_root_seqs": [right.roots[j].seq for j in candidates],
            "candidate_count": len(candidates),
            "candidate_delta_by_position": {
                str(j): result.left_candidate_delta[(root.index, j)]
                for j in candidates},
            "forced_pair": _forced_for(root.index, result, side="left"),
            "can_be_unmatched": result.left_can_be_unmatched[root.index],
            "can_be_unmatched_delta": result.left_unmatched_delta.get(root.index),
            "counterpart_status": _status(root.index, result, side="left"),
        })
    right_out = []
    for root in right.roots:
        candidates = result.right_candidates[root.index]
        right_out.append({
            **_root_evidence(root),
            "counterpart_capture_id": left.capture_id,
            "candidate_root_positions": list(candidates),
            "candidate_root_seqs": [left.roots[i].seq for i in candidates],
            "candidate_count": len(candidates),
            "candidate_delta_by_position": {
                str(i): result.right_candidate_delta[(i, root.index)]
                for i in candidates},
            "forced_pair": _forced_for(root.index, result, side="right"),
            "can_be_unmatched": result.right_can_be_unmatched[root.index],
            "can_be_unmatched_delta": result.right_unmatched_delta.get(root.index),
            "counterpart_status": _status(root.index, result, side="right"),
        })
    return left_out, right_out


def _forced_for(index: int, result: capture_match.MatchResult, *, side: str) -> bool:
    if side == "left":
        return any(i == index for i, _ in result.forced_pairs)
    return any(j == index for _, j in result.forced_pairs)


def _status(index: int, result: capture_match.MatchResult, *, side: str) -> str:
    if side == "left":
        candidates = result.left_candidates[index]
        forced = _forced_for(index, result, side="left")
        unmatched = result.left_can_be_unmatched[index]
    else:
        candidates = result.right_candidates[index]
        forced = _forced_for(index, result, side="right")
        unmatched = result.right_can_be_unmatched[index]
    if forced:
        return "forced_pair"
    if candidates:
        if len(candidates) > 1:
            return "ambiguous"
        return "single_candidate_but_unmatchable" if unmatched else "single_candidate"
    if unmatched:
        return "no_counterpart_in_capture"
    if unmatched:
        return "no_counterpart_in_capture"
    # 既无候选又不能不配：只可能发生在 K 为 0 或该根所在侧为空等边界。
    return "no_counterpart_in_capture" if result.K == 0 else "constrained_out"


def _build_summary(manifest: dict, left: SideCapture, right: SideCapture,
                   result: capture_match.MatchResult) -> dict:
    return {
        "K": result.K,
        "optimal_delta_intervals": [list(x) for x in result.optimal_intervals],
        "counts": {
            "left": _accounting(left, left.root_count),
            "right": _accounting(right, right.root_count),
            "matched_pairs": result.K,
            "paired_roots_left": sum(1 for c in result.left_candidates if c),
            "paired_roots_right": sum(1 for c in result.right_candidates if c),
            "forced_pairs": len(result.forced_pairs),
            "uncertain_roots_left": sum(1 for i in range(left.root_count)
                                        if not _forced_for(i, result, side="left")),
            "uncertain_roots_right": sum(1 for j in range(right.root_count)
                                         if not _forced_for(j, result, side="right")),
        },
        "conditions": {
            "offset_ns": list(manifest["offset_ns"]),
            "transit_ns": list(manifest["transit_ns"]),
        },
        "contract": CONTRACT,
        "algorithm_stats": result.stats,
    }


def _build_witness(left: SideCapture, right: SideCapture,
                   result: capture_match.MatchResult) -> dict:
    pairs = []
    for i, j in result.witness_pairs:
        pairs.append({
            "left_root_position": i,
            "right_root_position": j,
            "left_seq": left.roots[i].seq,
            "right_seq": right.roots[j].seq,
            "left_observed_ns": left.roots[i].observed_ns,
            "right_observed_ns": right.roots[j].observed_ns,
            "transit_ns": right.roots[j].observed_ns + result.witness_delta
                          - left.roots[i].observed_ns,
            "topic": left.roots[i].topic,
            "payload": payload_evidence(left.roots[i].payload),
            "retain": left.roots[i].retain,
        })
    return {
        "delta": result.witness_delta,
        "pair_count": len(pairs),
        "pairs": pairs,
        "reaches_K": len(pairs) == result.K,
    }


def reconcile(manifest_path: str) -> dict:
    """执行一次完整对账，返回报告 dict（不写文件）。"""
    manifest = load_manifest(manifest_path)
    captures = manifest["captures"]
    left = load_capture("left", captures["left"])
    right = load_capture("right", captures["right"])
    try:
        result = capture_match.solve(left.roots, right.roots,
                                     manifest["offset_ns"], manifest["transit_ns"])
    except MatchResourceLimit as exc:
        raise ReconcileResourceLimit([{
            "location": "reconcile", "reason": "MATCH_RESOURCE_LIMIT",
            "detail": str(exc)}])

    left_relations, right_relations = _root_relations(left, right, result)
    witness = _build_witness(left, right, result)
    forced = [
        {
            "left_root_position": i,
            "right_root_position": j,
            "left_seq": left.roots[i].seq,
            "right_seq": right.roots[j].seq,
            "left_observed_ns": left.roots[i].observed_ns,
            "right_observed_ns": right.roots[j].observed_ns,
            "delta_interval": [[lo, hi]
                               for lo, hi in result.optimal_intervals],
        }
        for i, j in result.forced_pairs
    ]
    retransmissions = {
        side: [
            {
                "root_seq": root.seq,
                "root_position": root.index,
                "retry_seqs": list(root.raw_seqs[1:]),
                "capture_epoch": root.capture_epoch,
                "session_id": root.session_id,
                "qos": root.qos,
                "message_id": root.message_id,
                "topic": root.topic,
                "payload": payload_evidence(root.payload),
                "retain": root.retain,
            }
            for root in cap.roots if len(root.raw_seqs) > 1
        ]
        for side, cap in (("left", left), ("right", right))
    }
    report = {
        "format": REPORT_FORMAT,
        "format_version": 1,
        "generated_by": "offline_mqtt.capture_reconcile.reconcile",
        "captures": {
            side: {
                "capture_id": cap.capture_id,
                "ledger_path": cap.spec.db_rel,
                "ledger_sha256": cap.db_sha256,
                "observations_path": cap.spec.observations_rel,
                "observations_sha256": cap.observations_sha256,
                "raw_rows": cap.raw_row_count,
                "roots": cap.root_count,
                "retransmission_rows": cap.retransmission_count,
            }
            for side, cap in (("left", left), ("right", right))
        },
        "input": {
            "manifest_path": manifest["path"],
            "manifest_sha256": manifest["sha256"],
            "offset_ns": list(manifest["offset_ns"]),
            "transit_ns": list(manifest["transit_ns"]),
        },
        "contract": CONTRACT,
        "summary": _build_summary(manifest, left, right, result),
        "optimal_witness": witness,
        "forced_pairs": forced,
        "retransmissions": retransmissions,
        "roots": {"left": left_relations, "right": right_relations},
        "notes": [
            "本报告只是指定捕获与合同内的对应证据；no_counterpart_in_capture "
            "不等于证明网络真实丢包。",
            "同一正文的独立重复发布不按摘要去重；重传只由 retry_of 引用折叠。",
            "报告不含可写来源连接；来源库与观测仅以 sha256 记录。",
        ],
    }
    return report


# --------------------------------------------------------------- 报告写入
def _canonical_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2)


def write_report(report: dict, target_path: str) -> str:
    """把报告原子写入目标；目标已存在则拒绝，绝不覆盖。"""
    if os.path.exists(target_path):
        raise ReportExistsError([{
            "location": "target", "reason": "TARGET_EXISTS",
            "detail": f"目标报告已存在，拒绝覆盖: {target_path}"}])
    directory = os.path.dirname(os.path.abspath(target_path)) or "."
    os.makedirs(directory, exist_ok=True)
    tmp_path = target_path + ".partial"
    try:
        with open(tmp_path, "w", encoding="utf-8") as fh:
            fh.write(_canonical_json(report))
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        # os.link 在目标已存在时失败（EEXIST），因此不会覆盖。
        try:
            os.link(tmp_path, target_path)
        except FileExistsError:
            raise ReportExistsError([{
                "location": "target", "reason": "TARGET_EXISTS",
                "detail": f"目标报告已存在，拒绝覆盖: {target_path}"}])
        except OSError:
            # 某些文件系统不支持硬链接：退回排他创建。
            try:
                with open(target_path, "x", encoding="utf-8") as fh:
                    fh.write(_canonical_json(report))
                    fh.write("\n")
            except FileExistsError:
                raise ReportExistsError([{
                    "location": "target", "reason": "TARGET_EXISTS",
                    "detail": f"目标报告已存在，拒绝覆盖: {target_path}"}])
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
    return os.path.abspath(target_path)


# ------------------------------------------------------------ 报告读取/查询
class ReportError(ValueError):
    """报告缺失、损坏或查询参数非法。"""


def load_report(report_path: str) -> dict:
    if not os.path.isfile(report_path):
        raise ReportError(f"报告不存在: {report_path}")
    try:
        with open(report_path, "r", encoding="utf-8") as fh:
            report = json.load(fh)
    except (OSError, ValueError) as exc:
        raise ReportError(f"报告不可读: {report_path}: {exc}")
    if not isinstance(report, dict) or report.get("format") != REPORT_FORMAT:
        raise ReportError(f"不是本模块生成的报告: {report_path}")
    return report


def summarize_report(report: dict) -> dict:
    summary = report.get("summary", {})
    return {
        "format": report.get("format"),
        "captures": report.get("captures"),
        "input_conditions": report.get("input"),
        "K": summary.get("K"),
        "optimal_delta_intervals": summary.get("optimal_delta_intervals"),
        "counts": summary.get("counts"),
        "forced_pairs": report.get("forced_pairs"),
        "optimal_witness": {
            "delta": report.get("optimal_witness", {}).get("delta"),
            "pair_count": report.get("optimal_witness", {}).get("pair_count"),
        },
        "algorithm_stats": summary.get("algorithm_stats"),
        "contract": report.get("contract"),
        "notes": report.get("notes"),
    }


def _find_root(report: dict, side: str, seq: Optional[int],
               position: Optional[int]) -> dict:
    roots = report.get("roots", {}).get(side, [])
    if seq is not None:
        for root in roots:
            if root.get("root_seq") == seq:
                return root
        raise ReportError(f"{side} 捕获中没有 seq={seq} 的根")
    if position is not None:
        if 0 <= position < len(roots):
            return roots[position]
        raise ReportError(f"{side} 根位置 {position} 超出范围 0..{len(roots) - 1}")
    raise ReportError("需要 --seq 或 --position 指定一个根")


def explain_report(report: dict, side: str, seq: Optional[int] = None,
                   position: Optional[int] = None) -> dict:
    """针对一个根展示：候选、可不配结论，以及两个达到 K 的完整方案。"""
    if side not in ("left", "right"):
        raise ReportError("side 必须是 left 或 right")
    root = _find_root(report, side, seq, position)
    summary = report.get("summary", {})
    K = summary.get("K", 0)
    other_side = "right" if side == "left" else "left"
    other_roots = report.get("roots", {}).get(other_side, [])
    optimal_deltas = summary.get("optimal_delta_intervals", [])

    scenarios = _explain_scenarios(report, side, root)

    explanation = {
        "capture_id": root.get("capture_id"),
        "side": side,
        "root_evidence": {k: v for k, v in root.items()
                          if k not in ("candidate_root_positions",
                                       "candidate_root_seqs")},
        "candidate_root_positions": root.get("candidate_root_positions", []),
        "candidate_root_seqs": root.get("candidate_root_seqs", []),
        "candidate_count": root.get("candidate_count", 0),
        "forced_pair": root.get("forced_pair", False),
        "can_be_unmatched": root.get("can_be_unmatched", False),
        "counterpart_status": root.get("counterpart_status"),
        "K": K,
        "optimal_delta_intervals": optimal_deltas,
        "interpretation": _interpretation(root),
        "scenarios": scenarios,
        "scenario_note": (
            "每个方案是完整的 K 对配对，内部共用一个 delta；不同方案可以使用"
            "不同 delta。方案由报告内保存的根证据重建，复核时不需要来源库。"),
        "scenario_verification": [
            {"delta": sc["delta"], **verify_scenario(report, sc)}
            for sc in scenarios
        ],
        "scenario_coverage": {
            "candidate_edges_covered": len([
                s for s in scenarios if s["root_outcome"] == "paired"]),
            "unmatched_covered": any(s["root_outcome"] == "unmatched"
                                     for s in scenarios),
            "max_scenarios": MAX_EXPLAIN_SCENARIOS,
        },
        "other_side_root_count": len(other_roots),
    }
    return explanation


def _interpretation(root: dict) -> dict:
    status = root.get("counterpart_status")
    if status == "forced_pair":
        reason = "该对在所有最优方案中都出现，是唯一确定对应。"
    elif status == "single_candidate":
        reason = ("只有一个候选对端，且每个最优方案都必须使用它；这是唯一确定对应。")
    elif status == "ambiguous":
        reason = ("存在多个候选对端，或该根在其他最优方案中可不配；"
                  "不能确定具体对应。")
    elif status == "single_candidate_but_unmatchable":
        reason = ("只有一个候选对端，但存在不配该根的最优方案；"
                  "因此不能称确定对应。")
    elif status == "constrained_out":
        reason = "受合同的时间/顺序限制，该根在最优方案中无法被解释为任何对端。"
    else:
        reason = ("任何最优方案都不配该根：仅说明指定捕获与合同内没有对应证据，"
                  "不等于证明网络真实丢包。")
    return {
        "status": status,
        "reason": reason,
        "evidence_boundary": "结论限于本次输入捕获与声明的 offset/transit/顺序合同。",
    }




class _ReportRoot:
    """从报告里复原的最小根视图；只用于 explain 的离线重建。"""

    __slots__ = ("index", "seq", "observed_ns", "topic", "payload", "retain")

    def __init__(self, item: dict):
        self.index = item["root_index"]
        self.seq = item["root_seq"]
        self.observed_ns = item["observed_ns"]
        self.topic = item["topic"]
        self.payload = bytes.fromhex(item["payload"]["hex"])
        self.retain = item["retain"]

    @property
    def content_key(self):
        return (self.topic, self.payload, self.retain)


def _report_roots(report: dict, side: str) -> list:
    roots = [_ReportRoot(item) for item in report.get("roots", {}).get(side, [])]
    roots.sort(key=lambda r: r.index)
    return roots


def _matching_at(report: dict, delta: int, *, edge: Optional[tuple] = None,
                 avoid: Optional[int] = None, side: str = "left",
                 contain: Optional[int] = None):
    """在报告的证据上重建指定 delta 的最优配对，可要求包含某边/根或回避某根。"""
    left = _report_roots(report, "left")
    right = _report_roots(report, "right")
    masks = capture_match.masks_at(left, right,
                                   tuple(report["input"]["offset_ns"]),
                                   tuple(report["input"]["transit_ns"]), delta)
    n, m = len(left), len(right)
    if capture_match.lcs_size(masks) != report["summary"]["K"]:
        return None
    if edge is not None:
        return capture_match.matching_with_forced_edge(masks, n, m, edge)
    if avoid is not None:
        if side == "left":
            return capture_match.matching_without_left(masks, n, m, avoid)
        return capture_match.matching_without_right(masks, n, m, avoid)
    if contain is not None:
        if side == "left":
            for j in range(m):
                if (masks[contain] >> j) & 1:
                    pairs = capture_match.matching_with_forced_edge(
                        masks, n, m, (contain, j))
                    if pairs is not None:
                        return pairs
        else:
            for i in range(n):
                if (masks[i] >> contain) & 1:
                    pairs = capture_match.matching_with_forced_edge(
                        masks, n, m, (i, contain))
                    if pairs is not None:
                        return pairs
        return None
    return capture_match.full_matching(masks, n, m)


def _scenario_from_pairs(report: dict, delta: int, pairs: list,
                         side: str, root_pos: int) -> dict:
    touched = any((i if side == "left" else j) == root_pos for i, j in pairs)
    return {
        "delta": delta,
        "pair_count": len(pairs),
        "reaches_K": len(pairs) == report["summary"]["K"],
        "root_outcome": "paired" if touched else "unmatched",
        "pairs": _pairs_to_evidence(report, pairs),
    }


def _explain_scenarios(report: dict, side: str, root: dict) -> list[dict]:
    """针对一个根给出“达到 K、内部共用同一 delta”的完整方案。

    对每个候选对端各给一个方案（因此不同候选可见），再加上“该根不配”的方案
    （若存在）。每个方案都从报告内保存的根证据在指定 delta 下重建，不读来源库；
    所有方案都调用 :func:`verify_scenario` 复核。上限 :data:`MAX_EXPLAIN_SCENARIOS`。
    """
    root_pos = root["root_index"]
    planned: list[tuple[int, tuple]] = []
    for key, delta in sorted((root.get("candidate_delta_by_position") or {}).items()):
        j = int(key)
        edge = (root_pos, j) if side == "left" else (j, root_pos)
        planned.append((int(delta), edge))
    if root.get("can_be_unmatched"):
        delta = root.get("can_be_unmatched_delta")
        if delta is None:
            delta = _first_delta_without_root(report, side, root_pos)
        if delta is not None:
            planned.append((int(delta), None))

    scenarios: list[dict] = []
    seen: set = set()
    for delta, edge in planned:
        if len(scenarios) >= MAX_EXPLAIN_SCENARIOS:
            break
        if edge is None:
            pairs = _matching_at(report, delta, avoid=root_pos, side=side)
        else:
            pairs = _matching_at(report, delta, edge=edge)
        if pairs is None:
            continue
        signature = tuple((i, j) for i, j in pairs)
        if signature in seen:
            continue
        seen.add(signature)
        scenarios.append(_scenario_from_pairs(report, delta, pairs, side, root_pos))
    return scenarios


def _pairs_to_evidence(report: dict, pairs: list[tuple[int, int]]) -> list[dict]:
    left_roots = report.get("roots", {}).get("left", [])
    right_roots = report.get("roots", {}).get("right", [])
    out = []
    for i, j in pairs:
        lroot = left_roots[i]
        rroot = right_roots[j]
        out.append({
            "left_root_position": i,
            "right_root_position": j,
            "left_seq": lroot.get("root_seq"),
            "right_seq": rroot.get("root_seq"),
            "left_observed_ns": lroot.get("observed_ns"),
            "right_observed_ns": rroot.get("observed_ns"),
            "topic": lroot.get("topic"),
            "payload": lroot.get("payload"),
            "retain": lroot.get("retain"),
        })
    return out


def verify_scenario(report: dict, scenario: dict) -> dict:
    """独立复核一个方案：delta 唯一、时间条件、内容相等、非交叉、计数为 K。"""
    delta = scenario["delta"]
    pairs = scenario["pairs"]
    lo, hi = report["input"]["transit_ns"]
    problems = []
    last_right = -1
    seen_left = set()
    seen_right = set()
    for pair in pairs:
        left = report["roots"]["left"][pair["left_root_position"]]
        right = report["roots"]["right"][pair["right_root_position"]]
        transit = right["observed_ns"] + delta - left["observed_ns"]
        if not (lo <= transit <= hi):
            problems.append(f"时延 {transit} 不在 [{lo},{hi}]")
        if (left["topic"] != right["topic"]
                or left["payload"]["hex"] != right["payload"]["hex"]
                or left["retain"] != right["retain"]):
            problems.append(f"内容不等: {left['root_seq']} vs {right['root_seq']}")
        if pair["right_root_position"] <= last_right:
            problems.append("配对不同交叉（右侧次序未严格递增）")
        last_right = pair["right_root_position"]
        if pair["left_root_position"] in seen_left:
            problems.append("左根被重复使用")
        if pair["right_root_position"] in seen_right:
            problems.append("右根被重复使用")
        seen_left.add(pair["left_root_position"])
        seen_right.add(pair["right_root_position"])
    if len(pairs) != report["summary"]["K"]:
        problems.append(f"方案配对数 {len(pairs)} ≠ K {report['summary']['K']}")
    return {"ok": not problems, "problems": problems, "delta": delta,
            "pair_count": len(pairs)}


def verify_report(report: dict) -> dict:
    """不读来源库，复核报告的所有最优 delta 区间与 witness 的自洽性。"""
    problems: list[str] = []
    summary = report.get("summary", {})
    K = summary.get("K")
    witness = report.get("optimal_witness", {})
    if witness.get("pair_count") != K:
        problems.append("witness 配对数不等于 K")
    intervals = summary.get("optimal_delta_intervals", [])
    for idx in range(1, len(intervals)):
        if intervals[idx - 1][1] + 1 >= intervals[idx][0]:
            problems.append("最优 delta 区间未按严格递增且不相邻合并")
    for root_side in ("left", "right"):
        for root in report.get("roots", {}).get(root_side, []):
            for _, delta in (root.get("candidate_delta_by_position") or {}).items():
                if not any(lo <= delta <= hi for lo, hi in intervals):
                    problems.append(
                        f"{root_side} 根 seq={root.get('root_seq')} 候选 "
                        f"delta={delta} 不在最优区间内")
            delta = root.get("can_be_unmatched_delta")
            if delta is not None and not any(lo <= delta <= hi for lo, hi in intervals):
                problems.append(
                    f"{root_side} 根 seq={root.get('root_seq')} 可不配 "
                    f"delta={delta} 不在最优区间内")
    for pair in witness.get("pairs", []):
        if pair["transit_ns"] < report["input"]["transit_ns"][0] or \
                pair["transit_ns"] > report["input"]["transit_ns"][1]:
            problems.append("witness 中有时延超出 transit_ns")
    return {"ok": not problems, "problems": problems,
            "witness_verified": verify_scenario(report, {
                "delta": witness.get("delta"),
                "pairs": witness.get("pairs", []),
            })}


def find_counterpart(report: dict, side: str, seq: Optional[int] = None,
                     position: Optional[int] = None) -> dict:
    """按“某个根”反查对端证据；用于关掉来源后的人工抽查。

    返回该根的全部候选、can_be_unmatched、forced_pair 结论，以及每个候选的
    一个达到 K 的完整方案；不读取来源库。
    """
    return explain_report(report, side, seq=seq, position=position)
