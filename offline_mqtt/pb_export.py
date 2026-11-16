"""离线外发编排：只读来源账本 → 按描述符删除字段 → 可重新接入的导出目录。

交付物（输出目录必须事先不存在；全部成功才出现完整结果）：

- ``messages.jsonl``：每行一条可交给现有接入 API 的 PUBLISH 记录
  （``payload_base64`` + 来源 ``event_seq`` + 本次使用的 ``message_type``）；
- ``report.json``：描述符/配置摘要、选中与改动记录数、各删除路径命中的
  线上出现数、每条记录的输入/输出长度与删除的原始半开字节区间；
- ``COMPLETE.json``：完成标记，含前两者的 sha256 与大小。任何中途失败
  （坏字段、未知字段、资源超限、写出失败）都不会留下它，``inspect`` 与
  读取接口只认带完整标记的导出。

来源账本以 ``file:...?mode=ro`` 只读打开，只执行 ``PRAGMA table_info`` 与
``SELECT``；不创建订阅、不改授权、不改接入器行为。选中记录按 ``ledger.seq``
次序逐条流式处理，不把全部载荷装入内存列表。
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import time
import urllib.parse
from typing import Dict, List, Optional, Sequence, Tuple

from .pb_descriptor import (
    LoadedDescriptors,
    build_plan,
    load_descriptor_set,
    normalize_paths,
)
from .pb_errors import (
    CONFIG_ERROR,
    IO_ERROR,
    LEDGER_ERROR,
    TARGET_EXISTS,
    ExportError,
)
from .pb_wire import DEFAULT_LIMITS, Limits, build_schema, rewrite_payload

MESSAGES_FILE = "messages.jsonl"
REPORT_FILE = "report.json"
COMPLETE_FILE = "COMPLETE.json"
FORMAT_VERSION = 1

LEDGER_REQUIRED_COLUMNS = {
    "seq", "session_id", "topic", "payload", "qos", "retain", "dup", "message_id",
}


# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------

class TopicRule:
    __slots__ = ("topic", "message_type", "remove_paths", "plan", "schema", "hits")

    def __init__(self, topic, message_type, remove_paths, plan, schema):
        self.topic = topic
        self.message_type = message_type
        self.remove_paths = remove_paths      # 规范化后的字段号元组列表
        self.plan = plan
        self.schema = schema
        self.hits: Dict[Tuple[int, ...], int] = {p: 0 for p in remove_paths}


def _config_fail(detail: str) -> None:
    raise ExportError(CONFIG_ERROR, detail)


def _require_keys(obj: dict, allowed: set, required: set, where: str) -> None:
    unknown = set(obj) - allowed
    if unknown:
        _config_fail(f"{where}: 未知字段 {sorted(unknown)}")
    missing = required - set(obj)
    if missing:
        _config_fail(f"{where}: 缺少字段 {sorted(missing)}")


def _positive_int(value, where: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        _config_fail(f"{where} 必须是正整数，实际: {value!r}")
    return value


def load_config(config_path: str) -> dict:
    """读取并做结构校验（描述符相关校验在 prepare 阶段进行）。"""
    try:
        with open(config_path, "rb") as fh:
            raw = fh.read()
    except OSError as exc:
        raise ExportError(CONFIG_ERROR, f"配置文件读取失败: {exc}") from None
    try:
        cfg = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExportError(CONFIG_ERROR, f"配置文件不是合法 JSON: {exc}") from None
    if not isinstance(cfg, dict):
        _config_fail("配置必须是 JSON 对象")
    _require_keys(cfg, {"version", "topics", "limits"}, {"topics"}, "配置")
    if "version" in cfg and cfg["version"] != 1:
        _config_fail(f"不支持的配置版本: {cfg['version']!r}")
    topics = cfg["topics"]
    if not isinstance(topics, list) or not topics:
        _config_fail("topics 必须是非空数组")
    seen_topics = set()
    for i, entry in enumerate(topics):
        where = f"topics[{i}]"
        if not isinstance(entry, dict):
            _config_fail(f"{where} 必须是对象")
        _require_keys(entry, {"topic", "message_type", "remove_paths"},
                      {"topic", "message_type", "remove_paths"}, where)
        topic = entry["topic"]
        if not isinstance(topic, str) or not topic:
            _config_fail(f"{where}.topic 必须是非空字符串（精确主题）")
        if topic in seen_topics:
            _config_fail(f"{where}.topic 重复: {topic!r}")
        seen_topics.add(topic)
        if not isinstance(entry["message_type"], str) or not entry["message_type"]:
            _config_fail(f"{where}.message_type 必须是非空字符串")
        if not isinstance(entry["remove_paths"], list):
            _config_fail(f"{where}.remove_paths 必须是数组")
    limits = cfg.get("limits", {})
    if limits is not None:
        if not isinstance(limits, dict):
            _config_fail("limits 必须是对象")
        _require_keys(limits, {"max_bytes", "max_depth", "max_occurrences"},
                      set(), "limits")
    cfg["_raw_sha256"] = hashlib.sha256(raw).hexdigest()
    return cfg


class Prepared:
    """描述符 + 配置编译后的可执行形态。"""

    def __init__(self, descriptors: LoadedDescriptors, config: dict,
                 rules: List[TopicRule], limits: Limits):
        self.descriptors = descriptors
        self.config = config
        self.rules = rules
        self.limits = limits


def prepare(descriptors: LoadedDescriptors, config: dict,
            limits_override: Optional[dict] = None) -> Prepared:
    lim = dict(config.get("limits") or {})
    limits = Limits(
        max_bytes=_positive_int(lim["max_bytes"], "limits.max_bytes")
        if "max_bytes" in lim else DEFAULT_LIMITS.max_bytes,
        max_depth=_positive_int(lim["max_depth"], "limits.max_depth")
        if "max_depth" in lim else DEFAULT_LIMITS.max_depth,
        max_occurrences=_positive_int(lim["max_occurrences"], "limits.max_occurrences")
        if "max_occurrences" in lim else DEFAULT_LIMITS.max_occurrences,
    )
    if limits_override:
        merged = limits._asdict()
        for key, value in limits_override.items():
            if key not in merged:
                _config_fail(f"未知 limits 覆盖项: {key!r}")
            merged[key] = _positive_int(value, f"limits.{key}")
        limits = Limits(**merged)
    rules: List[TopicRule] = []
    schema_cache: dict = {}
    for entry in config["topics"]:
        desc = descriptors.find_message(entry["message_type"])
        paths = normalize_paths(entry["remove_paths"], desc)
        plan = build_plan(paths)
        schema = build_schema(desc, schema_cache)
        rules.append(TopicRule(entry["topic"], entry["message_type"],
                               paths, plan, schema))
    return Prepared(descriptors, config, rules, limits)


# ---------------------------------------------------------------------------
# 来源账本（只读）
# ---------------------------------------------------------------------------

def open_ledger_readonly(ledger_path: str) -> sqlite3.Connection:
    """以不可变只读方式打开已封存账本；不在来源旁产生任何文件。

    ``immutable=1`` 的前提是来源已 checkpoint（无热 WAL）。若旁边存在非空
    ``-wal``，说明账本未完全封存，此时只读打开既可能漏读也未被允许——
    明确报错，请调用方先完成 checkpoint 再外发。
    """
    if not os.path.isfile(ledger_path):
        raise ExportError(LEDGER_ERROR, f"来源账本不存在: {ledger_path}")
    wal_path = ledger_path + "-wal"
    if os.path.isfile(wal_path) and os.path.getsize(wal_path) > 0:
        raise ExportError(
            LEDGER_ERROR,
            "来源账本存在未 checkpoint 的 WAL，尚未封存；请先完成 checkpoint",
        )
    uri = ("file:" + urllib.parse.quote(os.path.abspath(ledger_path))
           + "?mode=ro&immutable=1")
    try:
        conn = sqlite3.connect(uri, uri=True)
    except sqlite3.Error as exc:
        raise ExportError(LEDGER_ERROR, f"来源账本只读打开失败: {exc}") from None
    try:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(ledger)")}
    except sqlite3.Error as exc:
        conn.close()
        raise ExportError(LEDGER_ERROR, f"来源账本结构读取失败: {exc}") from None
    if not cols:
        conn.close()
        raise ExportError(LEDGER_ERROR, "来源账本缺少 ledger 表")
    missing = LEDGER_REQUIRED_COLUMNS - cols
    if missing:
        conn.close()
        raise ExportError(LEDGER_ERROR, f"来源账本 ledger 表缺少列: {sorted(missing)}")
    return conn


def iter_selected_events(conn: sqlite3.Connection, topics: Sequence[str]):
    """按 ledger.seq 次序流式产出选中主题的记录；不按任何键折叠。"""
    placeholders = ",".join("?" for _ in topics)
    sql = (
        "SELECT seq, session_id, topic, payload, qos, retain, dup, message_id "
        f"FROM ledger WHERE topic IN ({placeholders}) ORDER BY seq"
    )
    try:
        cur = conn.execute(sql, list(topics))
        for row in cur:
            yield row
    except sqlite3.Error as exc:
        raise ExportError(LEDGER_ERROR, f"来源账本读取失败: {exc}") from None


# ---------------------------------------------------------------------------
# 导出
# ---------------------------------------------------------------------------

def _sha256_of(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_complete(out_dir: str, selected: int, modified: int) -> dict:
    files = {}
    for name in (MESSAGES_FILE, REPORT_FILE):
        full = os.path.join(out_dir, name)
        files[name] = {"sha256": _sha256_of(full), "size": os.path.getsize(full)}
    doc = {
        "status": "complete",
        "format_version": FORMAT_VERSION,
        "files": files,
        "selected_records": selected,
        "modified_records": modified,
    }
    tmp = os.path.join(out_dir, COMPLETE_FILE + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=1)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, os.path.join(out_dir, COMPLETE_FILE))
    return doc


def export_ledger(ledger_path: str, descriptor_path: str, config_path: str,
                  out_dir: str, *, limits_override: Optional[dict] = None) -> dict:
    """执行离线外发；任何失败都抛 :class:`ExportError` 且不留完整输出。

    ``limits_override`` 若给出，是按键覆盖配置 ``limits`` 的字典
    （如 ``{"max_bytes": 4096}``）。
    """
    try:
        with open(descriptor_path, "rb") as fh:
            descriptor_bytes = fh.read()
    except OSError as exc:
        raise ExportError(IO_ERROR, f"描述符文件读取失败: {exc}") from None
    descriptors = load_descriptor_set(descriptor_bytes)
    config = load_config(config_path)
    prepared = prepare(descriptors, config, limits_override)
    rules_by_topic = {r.topic: r for r in prepared.rules}

    conn = open_ledger_readonly(ledger_path)
    try:
        if os.path.lexists(out_dir):
            raise ExportError(TARGET_EXISTS, f"输出目录已存在: {out_dir}")
        try:
            os.makedirs(out_dir)
        except FileExistsError:
            raise ExportError(TARGET_EXISTS, f"输出目录已存在: {out_dir}") from None
        except OSError as exc:
            raise ExportError(IO_ERROR, f"输出目录创建失败: {exc}") from None

        try:
            summary = _export_into(conn, prepared, rules_by_topic,
                                   ledger_path, config_path, out_dir)
        except Exception:
            # 任何失败：移除整个输出目录，不留可被 inspect 认为完整的结果
            shutil.rmtree(out_dir, ignore_errors=True)
            raise
        return summary
    finally:
        conn.close()


def _export_into(conn: sqlite3.Connection, prepared: Prepared,
                 rules_by_topic: Dict[str, TopicRule],
                 ledger_path: str, config_path: str, out_dir: str) -> dict:
    msg_tmp = os.path.join(out_dir, MESSAGES_FILE + ".tmp")
    rep_tmp = os.path.join(out_dir, REPORT_FILE + ".tmp")
    selected = 0
    modified = 0
    first_seq: Optional[int] = None
    last_seq: Optional[int] = None

    try:
        msg_fh = open(msg_tmp, "w", encoding="utf-8")
        rep_fh = open(rep_tmp, "w", encoding="utf-8")
    except OSError as exc:
        raise ExportError(IO_ERROR, f"输出文件创建失败: {exc}") from None

    try:
        header = {
            "tool": "offline_mqtt.pb_export",
            "format_version": FORMAT_VERSION,
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "descriptor": {
                "sha256": prepared.descriptors.sha256,
                "files": prepared.descriptors.file_summaries,
            },
            "config": {
                "sha256": config_sha256(prepared.config),
                "topics": [
                    {"topic": r.topic, "message_type": r.message_type,
                     "remove_paths": [list(p) for p in r.remove_paths]}
                    for r in prepared.rules
                ],
            },
            "limits": prepared.limits._asdict(),
            "source": {
                "ledger": os.path.abspath(ledger_path),
                "config": os.path.abspath(config_path),
                "selected_topics": [r.topic for r in prepared.rules],
            },
        }
        rep_fh.write(json.dumps(header, ensure_ascii=False)[:-1])  # 去掉结尾 }
        rep_fh.write(',"records":[\n')

        for row in iter_selected_events(conn, list(rules_by_topic)):
            (seq, session_id, topic, payload, qos, retain, dup, message_id) = row
            if payload is None or not isinstance(payload, (bytes, bytearray, memoryview)):
                raise ExportError(LEDGER_ERROR, "ledger.payload 不是 BLOB",
                                  event_seq=int(seq), topic=topic)
            rule = rules_by_topic[topic]
            result = rewrite_payload(rule.schema, bytes(payload), rule.plan,
                                     prepared.limits, rule.hits, event_seq=int(seq))
            record = {
                "type": "PUBLISH",
                "session_id": session_id,
                "topic": topic,
                "payload_base64": _b64(result.output),
                "qos": int(qos),
                "retain": bool(retain),
                "dup": bool(dup),
                "message_id": message_id if message_id is None else int(message_id),
                "event_seq": int(seq),
                "message_type": rule.message_type,
            }
            msg_fh.write(json.dumps(record, ensure_ascii=False))
            msg_fh.write("\n")
            rep_record = {
                "event_seq": int(seq),
                "topic": topic,
                "message_type": rule.message_type,
                "input_length": len(payload),
                "output_length": len(result.output),
                "deleted_intervals": [[s, e] for s, e in result.intervals],
            }
            rep_fh.write(",\n" if selected else "")
            rep_fh.write(json.dumps(rep_record, ensure_ascii=False))
            selected += 1
            if result.changed:
                modified += 1
            last_seq = int(seq)
            if first_seq is None:
                first_seq = int(seq)

        rep_fh.write("\n],")
        tail = {
            "selected_records": selected,
            "modified_records": modified,
            "first_event_seq": first_seq,
            "last_event_seq": last_seq,
            "path_hits": {
                r.topic: {".".join(str(x) for x in p): r.hits[p]
                          for p in r.remove_paths}
                for r in prepared.rules
            },
        }
        rep_fh.write(json.dumps(tail, ensure_ascii=False)[1:-1])  # 去掉首尾 {}
        rep_fh.write("}\n")
        msg_fh.flush()
        os.fsync(msg_fh.fileno())
        rep_fh.flush()
        os.fsync(rep_fh.fileno())
        msg_fh.close()
        rep_fh.close()
    except ExportError:
        msg_fh.close()
        rep_fh.close()
        raise
    except OSError as exc:
        msg_fh.close()
        rep_fh.close()
        raise ExportError(IO_ERROR, f"输出写出失败: {exc}") from None

    os.replace(msg_tmp, os.path.join(out_dir, MESSAGES_FILE))
    os.replace(rep_tmp, os.path.join(out_dir, REPORT_FILE))
    complete = _write_complete(out_dir, selected, modified)
    return {
        "status": "complete",
        "out_dir": os.path.abspath(out_dir),
        "selected_records": selected,
        "modified_records": modified,
        "first_event_seq": first_seq,
        "last_event_seq": last_seq,
        "path_hits": {
            r.topic: {".".join(str(x) for x in p): r.hits[p]
                      for p in r.remove_paths}
            for r in prepared.rules
        },
        "complete": complete,
    }


def config_sha256(config: dict) -> str:
    return config.get("_raw_sha256", "")


def _b64(data: bytes) -> str:
    import base64
    return base64.b64encode(data).decode("ascii")
