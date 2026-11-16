"""导出目录的完整性核验与 ``inspect`` 操作。

``inspect`` 只依赖导出目录本身（消息文件 + 报告 + 完成标记），不需要
来源账本、配置或描述符仍在场，即可核实：导出条数、每条输出正文与报告
中长度/序号的对应关系、改动计数的一致性。
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
from typing import Optional

from .pb_errors import EXPORT_INCOMPLETE, ExportError
from .pb_export import COMPLETE_FILE, FORMAT_VERSION, MESSAGES_FILE, REPORT_FILE


def _incomplete(detail: str) -> None:
    raise ExportError(EXPORT_INCOMPLETE, detail)


def verify_complete(export_dir: str) -> dict:
    """核验完成标记与两个交付文件的 sha256；通过则返回完成标记文档。"""
    if not os.path.isdir(export_dir):
        _incomplete(f"导出目录不存在: {export_dir}")
    complete_path = os.path.join(export_dir, COMPLETE_FILE)
    if not os.path.isfile(complete_path):
        _incomplete("缺少完成标记 COMPLETE.json（导出可能中途失败）")
    try:
        with open(complete_path, "r", encoding="utf-8") as fh:
            doc = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        _incomplete(f"完成标记无法解析: {exc}")
    if not isinstance(doc, dict) or doc.get("status") != "complete":
        _incomplete("完成标记状态不是 complete")
    if doc.get("format_version") != FORMAT_VERSION:
        _incomplete(f"不支持的导出格式版本: {doc.get('format_version')!r}")
    files = doc.get("files")
    if not isinstance(files, dict):
        _incomplete("完成标记缺少 files 摘要")
    for name in (MESSAGES_FILE, REPORT_FILE):
        meta = files.get(name)
        if not isinstance(meta, dict):
            _incomplete(f"完成标记缺少 {name} 的摘要")
        full = os.path.join(export_dir, name)
        if not os.path.isfile(full):
            _incomplete(f"交付文件缺失: {name}")
        if os.path.getsize(full) != meta.get("size"):
            _incomplete(f"{name} 大小与完成标记不符")
        h = hashlib.sha256()
        with open(full, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        if h.hexdigest() != meta.get("sha256"):
            _incomplete(f"{name} 的 sha256 与完成标记不符")
    return doc


def inspect_export(export_dir: str) -> dict:
    """离线核实导出目录；任何不一致都抛 EXPORT_INCOMPLETE。"""
    complete = verify_complete(export_dir)
    report_path = os.path.join(export_dir, REPORT_FILE)
    try:
        with open(report_path, "r", encoding="utf-8") as fh:
            report = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        _incomplete(f"报告无法解析: {exc}")

    records = report.get("records")
    if not isinstance(records, list):
        _incomplete("报告缺少 records 数组")
    selected = report.get("selected_records")
    modified = report.get("modified_records")
    if selected != len(records):
        _incomplete(
            f"报告 selected_records={selected} 与 records 条数 {len(records)} 不符"
        )
    if complete.get("selected_records") != selected:
        _incomplete("完成标记与报告的选中条数不符")

    # 逐行对齐消息文件与报告 records
    line_count = 0
    computed_modified = 0
    first_seq: Optional[int] = None
    last_seq: Optional[int] = None
    prev_seq: Optional[int] = None
    messages_path = os.path.join(export_dir, MESSAGES_FILE)
    with open(messages_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            if line_count >= len(records):
                _incomplete("消息文件行数多于报告 records 条数")
            try:
                msg = json.loads(line)
            except json.JSONDecodeError as exc:
                _incomplete(f"消息文件第 {line_count + 1} 行不是合法 JSON: {exc}")
            rec = records[line_count]
            if msg.get("type") != "PUBLISH":
                _incomplete(f"第 {line_count + 1} 行不是 PUBLISH 记录")
            try:
                payload = base64.b64decode(msg["payload_base64"], validate=True)
            except Exception:
                _incomplete(f"第 {line_count + 1} 行 payload_base64 无法解码")
            for key in ("event_seq", "topic", "message_type"):
                if msg.get(key) != rec.get(key):
                    _incomplete(
                        f"第 {line_count + 1} 行 {key} 与报告不符: "
                        f"{msg.get(key)!r} != {rec.get(key)!r}"
                    )
            if len(payload) != rec.get("output_length"):
                _incomplete(
                    f"第 {line_count + 1} 行输出长度 {len(payload)} "
                    f"与报告 {rec.get('output_length')} 不符"
                )
            intervals = rec.get("deleted_intervals")
            if not isinstance(intervals, list):
                _incomplete(f"第 {line_count + 1} 行报告缺少 deleted_intervals")
            in_len = rec.get("input_length")
            out_len = rec.get("output_length")
            if not isinstance(in_len, int) or not isinstance(out_len, int):
                _incomplete(f"第 {line_count + 1} 行报告长度字段非法")
            for iv in intervals:
                if (not isinstance(iv, list) or len(iv) != 2
                        or not all(isinstance(x, int) for x in iv)
                        or not (0 <= iv[0] < iv[1] <= in_len)):
                    _incomplete(f"第 {line_count + 1} 行删除区间非法: {iv!r}")
            removed = sum(e - s for s, e in intervals)
            if intervals:
                computed_modified += 1
            if out_len > in_len or in_len - removed > out_len:
                _incomplete(f"第 {line_count + 1} 行长度关系不自洽")
            seq = msg.get("event_seq")
            if prev_seq is not None and not (isinstance(seq, int) and seq > prev_seq):
                _incomplete("event_seq 未按升序排列")
            prev_seq = seq
            if first_seq is None:
                first_seq = seq
            last_seq = seq
            line_count += 1
    if line_count != len(records):
        _incomplete(
            f"消息文件行数 {line_count} 与报告 records 条数 {len(records)} 不符"
        )
    if modified != computed_modified:
        _incomplete(
            f"报告 modified_records={modified} 与含删除区间的记录数 "
            f"{computed_modified} 不符"
        )
    if complete.get("modified_records") != modified:
        _incomplete("完成标记与报告的改动条数不符")

    return {
        "ok": True,
        "export_dir": os.path.abspath(export_dir),
        "selected_records": selected,
        "modified_records": modified,
        "checked_records": line_count,
        "first_event_seq": first_seq,
        "last_event_seq": last_seq,
        "topics": sorted({r["topic"] for r in records}) if records else [],
        "path_hits": report.get("path_hits", {}),
        "limits": report.get("limits", {}),
        "descriptor_sha256": report.get("descriptor", {}).get("sha256"),
        "config_sha256": report.get("config", {}).get("sha256"),
    }
