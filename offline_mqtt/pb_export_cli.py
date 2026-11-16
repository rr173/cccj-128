"""离线外发 CLI：``export`` / ``inspect``。

用法::

    python -m offline_mqtt.cli export LEDGER --descriptor FDS --config CONFIG --out DIR
    python -m offline_mqtt.cli inspect EXPORT_DIR

退出码：0=成功；2=描述符/配置/正文输入非法；3=资源超限（未完成）；
4=输出目录已存在；5=来源账本或写出 IO 失败；6=导出目录不完整或核验不符。
"""

from __future__ import annotations

import json
import sys

from .pb_errors import ExportError
from .pb_export import export_ledger
from .pb_inspect import inspect_export


def _print_json(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def cmd_export(ledger: str, descriptor: str, config: str, out: str,
               limits_override: dict | None = None) -> int:
    try:
        summary = export_ledger(ledger, descriptor, config, out,
                                limits_override=limits_override)
    except ExportError as exc:
        _print_json({"status": "error", "error": exc.to_dict()})
        return exc.exit_code
    _print_json(summary)
    return 0


def cmd_inspect(export_dir: str) -> int:
    try:
        result = inspect_export(export_dir)
    except ExportError as exc:
        _print_json({"status": "error", "error": exc.to_dict()})
        return exc.exit_code
    _print_json(result)
    return 0
