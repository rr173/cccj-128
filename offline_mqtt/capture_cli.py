"""捕获链对账 CLI：``reconcile`` / ``summary`` / ``explain``。

三条命令都只读报告文件（``reconcile`` 还读清单与来源库，全部只读）：
- ``reconcile MANIFEST --out REPORT``：执行对账并写出新报告；目标已存在则拒绝；
- ``summary REPORT``：打印 K、最优 delta 区间、计数与 forced 对；
- ``explain REPORT --side left --seq N``：打印某根的全部候选与可达 K 的完整方案。

退出码：0=成功；2=输入/来源错误；3=资源不足（明确“未能完成”）；4=目标已存在。
"""

from __future__ import annotations

import argparse
import json
import sys

from .capture_reconcile import (
    ReportError,
    ReportExistsError,
    ReconcileInputError,
    ReconcileResourceLimit,
    ReconcileError,
    explain_report,
    load_manifest,
    load_report,
    reconcile,
    summarize_report,
    verify_report,
    write_report,
)

EXIT_OK = 0
EXIT_INPUT_ERROR = 2
EXIT_RESOURCE_LIMIT = 3
EXIT_TARGET_EXISTS = 4


def _emit(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True))


def _emit_error(exc: ReconcileError) -> None:
    _emit({"status": "error", "error_kind": exc.kind, "errors": exc.errors})


def cmd_reconcile(manifest: str, target: str, dry_run: bool) -> int:
    try:
        report = reconcile(manifest)
    except ReconcileResourceLimit as exc:
        _emit_error(exc)
        return EXIT_RESOURCE_LIMIT
    except ReconcileError as exc:
        _emit_error(exc)
        return EXIT_INPUT_ERROR
    if dry_run:
        _emit(summarize_report(report))
        return EXIT_OK
    try:
        path = write_report(report, target)
    except ReportExistsError as exc:
        _emit_error(exc)
        return EXIT_TARGET_EXISTS
    except OSError as exc:
        _emit({"status": "error", "error_kind": "report_write_error",
               "errors": [{"location": "target", "reason": "WRITE_FAILED",
                           "detail": str(exc)}]})
        return EXIT_INPUT_ERROR
    _emit({**summarize_report(report), "report_path": path})
    return EXIT_OK


def cmd_summary(report_path: str) -> int:
    try:
        report = load_report(report_path)
    except ReportError as exc:
        _emit({"status": "error", "error_kind": "report_error",
               "errors": [{"location": "report", "reason": "REPORT_UNREADABLE",
                           "detail": str(exc)}]})
        return EXIT_INPUT_ERROR
    verification = verify_report(report)
    _emit({**summarize_report(report), "verification": verification})
    return EXIT_OK if verification["ok"] else EXIT_INPUT_ERROR


def cmd_explain(report_path: str, side: str, seq, position) -> int:
    try:
        report = load_report(report_path)
        explanation = explain_report(report, side, seq=seq, position=position)
    except ReportError as exc:
        _emit({"status": "error", "error_kind": "report_error",
               "errors": [{"location": "explain", "reason": "EXPLAIN_FAILED",
                           "detail": str(exc)}]})
        return EXIT_INPUT_ERROR
    _emit(explanation)
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="offline_mqtt.reconcile")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_rec = sub.add_parser("reconcile", help="由清单生成新的对账报告")
    p_rec.add_argument("manifest")
    p_rec.add_argument("--out", required=False, default=None,
                       help="新报告路径（必须不存在）")
    p_rec.add_argument("--dry-run", action="store_true",
                       help="只计算并打印摘要，不写报告")

    p_sum = sub.add_parser("summary", help="打印已有报告的摘要与自检")
    p_sum.add_argument("report")

    p_exp = sub.add_parser("explain", help="解释单个根的全部候选与可达 K 的方案")
    p_exp.add_argument("report")
    p_exp.add_argument("--side", choices=("left", "right"), required=True)
    p_exp.add_argument("--seq", type=int, default=None, help="根的 ledger.seq")
    p_exp.add_argument("--position", type=int, default=None, help="根的端内位置")

    args = parser.parse_args(argv)
    if args.cmd == "reconcile":
        if not args.dry_run and not args.out:
            parser.error("reconcile 需要 --out（或用 --dry-run）")
        return cmd_reconcile(args.manifest, args.out, args.dry_run)
    if args.cmd == "summary":
        return cmd_summary(args.report)
    return cmd_explain(args.report, args.side, args.seq, args.position)


if __name__ == "__main__":
    sys.exit(main())
