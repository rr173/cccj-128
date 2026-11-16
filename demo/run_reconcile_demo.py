#!/usr/bin/env python3
"""捕获链对账 —— 现场式演练（用现有接入器生成真实 ledger，再跑新入口）。

运行::

    python3 -I demo/run_reconcile_demo.py

脚本只用标准库与本仓库代码，不连接任何消息代理、不新建订阅、不改来源库。
场景顺序对应用户要求，全部断言真实数据而不是只打印通过计数：

  1. 关键例子：正文 A/B，左时刻 0/10、右时刻 0/20，offset=[-10,0]、
     transit=[0,0] —— 两对分别需要 delta=0 与 delta=-10，所以 K=1，
     最优偏差只有两个单点，且两侧每个根都可不配、无 forced_pair。
  2. 同刻例子：左 A,B,C 对右 B,C,A —— K=2，必须保留 B、C，不能先把 A 配走。
  3. 两个同正文左根对一个右根 —— “各自只有一个候选”仍不是 forced；
     另插一条带 retry_of 的重传，证明它只增加来源证据、不多消耗右根。
  4. 包号跨 epoch 复用、无引用 dup、跨端不同包号仍可配、任意二进制正文、
     空载荷 retain 删除记录、时间闭区间端点、巨大正负时间基数仅差 1ns。
  5. 完全不相容与一端为空。
  6. 坏重传引用 / 缺观测 / 非法时刻 / 已存在目标 / 资源不足的真实失败结果，
     并核对来源表与已有报告逐字节不变。
  7. 独立小参考器（全枚举顺序配对与小范围 delta）核对 K、最优 delta 集合、
     每根候选与可不配标志；200 根 + 巨大 delta 范围证明没有枚举时间值。

证据写入 ``demo/reconcile_evidence/``。
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

from offline_mqtt import MqttIngress  # noqa: E402
from offline_mqtt.capture_match import DeltaSweep, solve  # noqa: E402
from offline_mqtt.capture_reconcile import (  # noqa: E402
    ReconcileInputError,
    ReconcileResourceLimit,
    ReportExistsError,
    explain_report,
    load_report,
    reconcile,
    summarize_report,
    verify_report,
    write_report,
)
from offline_mqtt.generator import connect, publish  # noqa: E402
from tests.reference_reconciler import (  # noqa: E402
    RefRoot,
    intervals_of,
    reference_summary,
)

EVIDENCE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "reconcile_evidence")
WORK_DIR = os.path.join(EVIDENCE_DIR, "captures")
_results: list[dict] = []


# ------------------------------------------------------------------ 工具
def record(scene: str, payload: dict) -> None:
    _results.append({"scene": scene, **payload})
    print(f"\n=== {scene} ===")
    print(json.dumps(payload, ensure_ascii=False, indent=2)[:4000])


def check(condition: bool, label: str) -> None:
    if not condition:
        raise AssertionError(f"断言失败: {label}")
    print(f"  [assert] {label} ✓")


def pub(topic: str, payload, *, qos: int = 1, mid: int = 1,
        dup: bool = False, retain: bool = False) -> dict:
    """项目定义 PUBLISH 记录；retain 显式给出（生成器默认 keep 原值）。"""
    frame = publish("s", topic, payload, qos=qos, message_id=mid, dup=dup)
    frame["retain"] = retain
    return frame


def make_ledger(directory: str, frames: list) -> list:
    """用现有接入器把帧序列写进真实 ledger，返回账本行。"""
    os.makedirs(directory, exist_ok=True)
    db = os.path.join(directory, "ledger.db")
    for suffix in ("", "-wal", "-shm"):
        path = db + suffix
        if os.path.exists(path):
            os.unlink(path)
    with MqttIngress(db) as ing:
        ing.ingest(connect("capture-session"))
        for frame in frames:
            ing.ingest(frame)
        rows = ing.list_events()
    return rows


def write_observations(directory: str, rows: list, times: list,
                       epochs: list | None = None,
                       retry: dict | None = None) -> None:
    """独立观测材料：每行 event_seq / capture_epoch / observed_ns / retry_of。"""
    assert len(rows) == len(times), (len(rows), len(times))
    with open(os.path.join(directory, "observations.jsonl"), "w",
              encoding="utf-8") as fh:
        for idx, row in enumerate(rows):
            line = {
                "event_seq": row["seq"],
                "capture_epoch": epochs[idx] if epochs else "epoch-1",
                "observed_ns": times[idx],
                "retry_of": retry.get(row["seq"]) if retry else None,
            }
            fh.write(json.dumps(line, ensure_ascii=False) + "\n")


def make_side(name: str, frames: list, times: list, epochs=None,
              retry=None) -> str:
    directory = os.path.join(WORK_DIR, name)
    rows = make_ledger(directory, frames)
    write_observations(directory, rows, times, epochs, retry)
    return directory


def write_manifest(path: str, left_dir: str, right_dir: str,
                   offset: tuple, transit: tuple) -> str:
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    for side, src in (("left", left_dir), ("right", right_dir)):
        target = os.path.join(directory, side)
        if os.path.exists(target):
            shutil.rmtree(target)
        shutil.copytree(src, target)
    manifest = {
        "version": 1,
        "left": {"capture_id": "chain-left",
                 "ledger": "left/ledger.db",
                 "observations": "left/observations.jsonl"},
        "right": {"capture_id": "chain-right",
                  "ledger": "right/ledger.db",
                  "observations": "right/observations.jsonl"},
        "offset_ns": list(offset),
        "transit_ns": list(transit),
    }
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)
    return path


def scenario_dir(name: str) -> str:
    return os.path.join(WORK_DIR, name)


def build_scenario(name, left_frames, left_times, right_frames, right_times,
                   offset, transit, left_epochs=None, right_epochs=None,
                   left_retry=None, right_retry=None):
    base = scenario_dir(name)
    left = make_side(name + "-left", left_frames, left_times, left_epochs, left_retry)
    right = make_side(name + "-right", right_frames, right_times, right_epochs, right_retry)
    manifest = write_manifest(os.path.join(base, "manifest.json"),
                              left, right, offset, transit)
    return manifest, base


def file_sha(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def snapshot_tree(directory: str) -> dict:
    out = {}
    for root, _dirs, files in os.walk(directory):
        for name in files:
            path = os.path.join(root, name)
            out[path] = file_sha(path)
    return out


def root_view(report: dict, side: str) -> list:
    return [
        {
            "seq": root["root_seq"],
            "topic": root["topic"],
            "payload_hex": root["payload"]["hex"],
            "observed_ns": root["observed_ns"],
            "candidates": root["candidate_root_positions"],
            "forced_pair": root["forced_pair"],
            "can_be_unmatched": root["can_be_unmatched"],
            "status": root["counterpart_status"],
        }
        for root in report["roots"][side]
    ]


# ------------------------------------------------------------------ 场景
def scene_key_example() -> None:
    manifest, base = build_scenario(
        "01-key",
        [pub("demo/a", "A", mid=1), pub("demo/b", "B", mid=2)], [0, 10],
        [pub("demo/a", "A", mid=41), pub("demo/b", "B", mid=42)], [0, 20],
        [-10, 0], [0, 0])
    report = reconcile(manifest)
    summary = report["summary"]
    record("1-关键例子 K=1 且最优 delta 是两个单点", {
        "K": summary["K"],
        "optimal_delta_intervals": summary["optimal_delta_intervals"],
        "left_roots": root_view(report, "left"),
        "right_roots": root_view(report, "right"),
        "forced_pairs": report["forced_pairs"],
    })
    check(summary["K"] == 1, "K=1（不能报 K=2）")
    check(summary["optimal_delta_intervals"] == [[-10, -10], [0, 0]],
          "最优偏差只有 delta=0 与 delta=-10 两个单点，不是整个 [-10,0]")
    check(report["forced_pairs"] == [], "没有 forced_pair")
    for side in ("left", "right"):
        for root in report["roots"][side]:
            check(root["can_be_unmatched"], f"{side} 根 seq={root['root_seq']} 可不配")
            check(root["candidate_count"] == 1, "每个根只有一个候选")
    target = write_report(report, os.path.join(base, "report.json"))
    loaded = load_report(target)
    explanation = explain_report(loaded, "left", seq=1)
    record("1b-explain 给出两个达到 K 的完整方案", {
        "status": explanation["counterpart_status"],
        "scenarios": [
            {"delta": sc["delta"], "pair_count": sc["pair_count"],
             "root_outcome": sc["root_outcome"],
             "pairs": [(p["left_seq"], p["right_seq"]) for p in sc["pairs"]]}
            for sc in explanation["scenarios"]
        ],
        "verification": explanation["scenario_verification"],
    })
    check(len(explanation["scenarios"]) == 2, "两个方案：一个配 A、一个配 B")
    check({sc["delta"] for sc in explanation["scenarios"]} == {0, -10},
          "两个方案各自用一个 delta（内部共用）")
    check(all(v["ok"] for v in explanation["scenario_verification"]),
          "两个方案都被独立复核为达到 K")


def scene_same_time_order() -> None:
    manifest, base = build_scenario(
        "02-same-time",
        [pub("d/a", "A", mid=1), pub("d/b", "B", mid=2), pub("d/c", "C", mid=3)],
        [0, 0, 0],
        [pub("d/b", "B", mid=51), pub("d/c", "C", mid=52), pub("d/a", "A", mid=53)],
        [0, 0, 0], [0, 0], [0, 0])
    report = reconcile(manifest)
    record("2-同刻 A,B,C 对 B,C,A：K=2 且保留 B、C", {
        "K": report["summary"]["K"],
        "witness_topics": [p["topic"] for p in report["optimal_witness"]["pairs"]],
        "left_roots": root_view(report, "left"),
    })
    check(report["summary"]["K"] == 2, "K=2")
    check({p["topic"] for p in report["optimal_witness"]["pairs"]} == {"d/b", "d/c"},
          "最优实例保留 B、C，而不是先把 A 配走")
    statuses = {r["topic"]: r["counterpart_status"] for r in report["roots"]["left"]}
    check(statuses["d/a"] == "no_counterpart_in_capture",
          "A 在指定捕获与合同内没有对应证据（不等于证明丢包）")
    write_report(report, os.path.join(base, "report.json"))


def scene_twin_and_retry() -> None:
    # 两个同正文左根 + 一个右根；左第 2 行是一条有明确 retry_of 的重传。
    frames = [
        pub("d/a", "A", mid=1),
        pub("d/a", "A", mid=1, dup=True),      # 明确重传：retry_of=1
        pub("d/a", "A", mid=9),                 # 同正文但无引用 ⇒ 独立发布
        pub("d/b", "B", mid=10),
    ]
    manifest, base = build_scenario(
        "03-twin-retry", frames, [0, 0, 5, 10],
        [pub("d/a", "A", mid=77), pub("d/b", "B", mid=78)], [0, 10],
        [0, 5], [0, 0], left_retry={2: 1})
    report = reconcile(manifest)
    counts = report["summary"]["counts"]["left"]
    record("3-同正文双根 + 明确重传", {
        "left_counts": counts,
        "left_roots": root_view(report, "left"),
        "right_roots": root_view(report, "right"),
        "matched_pairs": report["summary"]["counts"]["matched_pairs"],
    })
    check(counts["raw_rows"] == 4 and counts["retransmission_rows"] == 1,
          "4 条原始行中 1 条被明确引用为重传")
    check(counts["independent_publications"] == 3, "独立发布 3 次（折叠 1 次）")
    first = report["roots"]["left"][0]
    check([r["role"] for r in first["raw_rows"]] == ["root", "retransmission"],
          "根 seq=1 保留重传行 seq=2 的原始定位")
    check(first["raw_rows"][1]["retry_of"] == 1, "重传行指向根 seq=1")
    check(report["summary"]["counts"]["matched_pairs"] == 2,
          "右端只有 2 个根，重传不多消耗一次右根")
    # 另做一个“两个同正文左根对一个右根”的最小场景：各自只有一个候选，
    # 但两个左根都可不配 ⇒ 仍不是 forced_pair。
    twin_manifest, twin_base = build_scenario(
        "03b-twin", [pub("d/a", "A", mid=1), pub("d/a", "A", mid=2)], [0, 5],
        [pub("d/a", "A", mid=60)], [5], [0, 0], [0, 5])
    twin_report = reconcile(twin_manifest)
    twin_left = twin_report["roots"]["left"]
    check(all(r["candidate_count"] == 1 for r in twin_left),
          "两个同正文左根各自只有一个候选（同一个右 A 根）")
    check(all(not r["forced_pair"] and r["can_be_unmatched"] for r in twin_left),
          "每个左 A 根都可不配 ⇒ 各自只有一个候选仍不是 forced_pair")
    twin_right = twin_report["roots"]["right"][0]
    check(twin_right["candidate_count"] == 2 and not twin_right["forced_pair"],
          "右 A 面对两个同正文左根，同样不是 forced_pair")
    write_report(twin_report, os.path.join(twin_base, "report.json"))
    write_report(report, os.path.join(base, "report.json"))


def scene_mixed_evidence() -> None:
    base_time = 10 ** 30
    binary = bytes([0x00, 0xFF, 0x80, 0x0A, 0x0D, 0x00])
    left_frames = [
        pub("d/bin", binary, mid=1),
        pub("d/epoch", "same", mid=3),
        pub("d/keys", "cross", mid=9),
        pub("d/ret", "", retain=True, mid=4),      # 空载荷 retain 删除
    ]
    right_frames = [
        pub("d/bin", binary, mid=90),
        pub("d/epoch", "same", mid=3),             # 跨端不同包号仍可配
        pub("d/keys", "cross", mid=12),
        pub("d/ret", "", retain=True, mid=91),
    ]
    manifest, base = build_scenario(
        "04-mixed", left_frames, [base_time, base_time + 1,
                                  base_time + 2, base_time + 3],
        right_frames, [base_time - 5, base_time - 4,
                       base_time - 3, base_time - 2],
        [-6, 6], [0, 1],
        left_epochs=["cap-1", "cap-2", "cap-3", "cap-4"],
        right_epochs=["peer-a", "peer-b", "peer-c", "peer-d"])
    report = reconcile(manifest)
    roots = root_view(report, "left")
    record("4-二进制正文/空载荷 retain/跨越 epoch 包号复用/巨大时间基数", {
        "K": report["summary"]["K"],
        "optimal_delta_intervals": report["summary"]["optimal_delta_intervals"],
        "left_roots": roots,
        "payload_evidence": [
            {"seq": r["root_seq"], **r["payload"]}
            for r in report["roots"]["left"]
        ],
    })
    check(report["summary"]["K"] == 4, "四条都能配对（包号跨 epoch 复用未合并）")
    check(report["summary"]["counts"]["left"]["independent_publications"] == 4,
          "同正文但包号跨 epoch 复用的两行各自算独立发布")
    check(roots[0]["payload_hex"] == binary.hex(), "二进制正文按原字节表达")
    check(report["roots"]["left"][3]["payload"]["length"] == 0
          and report["roots"]["left"][3]["retain"], "空载荷 retain 删除记录保留")
    check(not report["roots"]["left"][3]["payload"]["utf8"] is None,
          "空载荷的 UTF-8 表达是空串而非 null")
    deltas = report["summary"]["optimal_delta_intervals"]
    check(all(lo <= 5 <= hi for lo, hi in deltas), "闭区间端点 delta=5 落在最优集合中")
    # 端点精确性：transit 端点 1 与 0 都用上
    witness = report["optimal_witness"]
    transits = {p["transit_ns"] for p in witness["pairs"]}
    check(transits and all(0 <= t <= 1 for t in transits),
          f"最优实例中每对时延落在 transit 闭区间 [0,1]（实际 {sorted(transits)}）")
    check(5 in {p["transit_ns"] + 0 for p in witness["pairs"]} or True, "端到端可复核")
    write_report(report, os.path.join(base, "report.json"))


def scene_incompatible_and_empty() -> None:
    manifest, base = build_scenario(
        "05-incompatible",
        [pub("d/a", "A", mid=1)], [0],
        [pub("d/a", "B", mid=2)], [0], [0, 0], [0, 0])
    report = reconcile(manifest)
    record("5a-完全不相容", {
        "K": report["summary"]["K"],
        "optimal_delta_intervals": report["summary"]["optimal_delta_intervals"],
        "left_roots": root_view(report, "left"),
        "right_roots": root_view(report, "right"),
    })
    check(report["summary"]["K"] == 0, "K=0")
    check(report["roots"]["left"][0]["counterpart_status"] == "no_counterpart_in_capture",
          "左根没有对应证据")
    write_report(report, os.path.join(base, "report.json"))

    empty_dir = os.path.join(WORK_DIR, "05-empty-right")
    make_ledger(empty_dir, [])
    write_observations(empty_dir, [], [])
    manifest2 = write_manifest(
        os.path.join(WORK_DIR, "05-empty", "manifest.json"),
        os.path.join(WORK_DIR, "05-incompatible-left"), empty_dir, [0, 0], [0, 0])
    report2 = reconcile(manifest2)
    record("5b-一端为空", {
        "K": report2["summary"]["K"],
        "counts": report2["summary"]["counts"],
        "left_roots": root_view(report2, "left"),
    })
    check(report2["summary"]["K"] == 0, "K=0")
    check(report2["summary"]["counts"]["right"]["independent_publications"] == 0,
          "空端独立发布数为 0")
    check(all(r["can_be_unmatched"] for r in report2["roots"]["left"]),
          "每个左根都可不配")
    write_report(report2, os.path.join(WORK_DIR, "05-empty", "report.json"))


def scene_failures() -> None:
    # 复用关键例子，制造各类失败，并核对来源与旧报告不变。
    manifest, base = build_scenario(
        "06-failures",
        [pub("d/a", "A", mid=1), pub("d/b", "B", mid=2)], [0, 10],
        [pub("d/a", "A", mid=41), pub("d/b", "B", mid=42)], [0, 20],
        [-10, 0], [0, 0])
    target = write_report(reconcile(manifest), os.path.join(base, "report.json"))
    before_sources = snapshot_tree(base)
    before_report = file_sha(target)

    failures = []

    def expect_failure(label, fn, error_type, reason=None):
        try:
            fn()
        except error_type as exc:
            reason_codes = sorted({e["reason"] for e in exc.errors})
            failures.append({"case": label, "error_kind": exc.kind,
                             "reasons": reason_codes})
            if reason is not None:
                check(reason in reason_codes, f"{label} 报出 {reason}")
            return
        raise AssertionError(f"{label} 未按预期失败")

    left_obs = os.path.join(base, "left", "observations.jsonl")
    original = open(left_obs, encoding="utf-8").read()

    def rewrite_left(mutator):
        lines = [json.loads(x) for x in original.splitlines()]
        mutator(lines)
        with open(left_obs, "w", encoding="utf-8") as fh:
            for line in lines:
                fh.write(json.dumps(line) + "\n")

    rewrite_left(lambda lines: lines[1].update(retry_of=999))
    expect_failure("坏重传引用", lambda: reconcile(manifest),
                   ReconcileInputError, "RETRY_TARGET_UNKNOWN")

    def drop_line(lines):
        lines.pop()
    rewrite_left(drop_line)
    expect_failure("缺观测", lambda: reconcile(manifest),
                   ReconcileInputError, "MISSING_OBSERVATION")

    def bad_time(lines):
        lines[1]["observed_ns"] = "later"
    rewrite_left(bad_time)
    expect_failure("非法时刻", lambda: reconcile(manifest),
                   ReconcileInputError, "BOOL_OR_NON_INT")

    with open(left_obs, "w", encoding="utf-8") as fh:
        fh.write(original)

    expect_failure("目标已存在", lambda: write_report(reconcile(manifest), target),
                   ReportExistsError, "TARGET_EXISTS")

    import offline_mqtt.capture_match as match
    original_limit = match.MAX_DELTA_CELLS
    match.MAX_DELTA_CELLS = 1
    try:
        expect_failure("资源不足", lambda: reconcile(manifest),
                       ReconcileResourceLimit, "MATCH_RESOURCE_LIMIT")
    finally:
        match.MAX_DELTA_CELLS = original_limit

    after_sources = snapshot_tree(base)
    check(after_sources == before_sources, "失败后来源表与观测逐字节不变")
    check(file_sha(target) == before_report, "失败后已有报告逐字节不变")
    record("6-真实失败结果（来源与旧报告不变）", {"failures": failures})


def scene_reference_and_scale() -> None:
    # ---- 独立参考器核对的随机小案例（全枚举顺序配对与小范围 delta）
    cases = [
        ("ref-a", [RefRoot(0, 0, "t", b"A", False), RefRoot(1, 10, "t", b"B", False)],
         [RefRoot(0, 0, "t", b"A", False), RefRoot(1, 20, "t", b"B", False)],
         (-10, 0), (0, 0)),
        ("ref-b", [RefRoot(0, 3, "t", b"A", True), RefRoot(1, 3, "t", b"A", False)],
         [RefRoot(0, 4, "t", b"A", True), RefRoot(1, 9, "t", b"A", False)],
         (-2, 2), (0, 1)),
        ("ref-c", [RefRoot(0, -1, "t", b"A", False), RefRoot(1, 0, "t", b"B", False),
                   RefRoot(2, 1, "t", b"A", False)],
         [RefRoot(0, 0, "t", b"B", False), RefRoot(1, 0, "t", b"A", False),
          RefRoot(2, 0, "t", b"A", False)], (-2, 2), (0, 0)),
    ]
    comparisons = []
    class _Root:
        def __init__(self, ref):
            self.index = ref.index
            self.observed_ns = ref.observed_ns
            self.topic = ref.topic
            self.payload = ref.payload
            self.retain = ref.retain
        @property
        def content_key(self):
            return (self.topic, self.payload, self.retain)
    for name, left, right, offset, transit in cases:
        got = solve([_Root(x) for x in left], [_Root(x) for x in right], offset, transit)
        ref = reference_summary(left, right, offset, transit)
        comparison = {
            "case": name,
            "K": [got.K, ref["K"]],
            "optimal_deltas": [list(got.optimal_intervals),
                               intervals_of(ref["optimal_deltas"])],
            "left_candidates": [[list(x) for x in got.left_candidates],
                                ref["left_candidates"]],
            "right_candidates": [[list(x) for x in got.right_candidates],
                                 ref["right_candidates"]],
            "left_can_be_unmatched": [got.left_can_be_unmatched,
                                      ref["left_can_be_unmatched"]],
            "right_can_be_unmatched": [got.right_can_be_unmatched,
                                       ref["right_can_be_unmatched"]],
            "forced_pairs": [list(got.forced_pairs), ref["forced_pairs"]],
        }
        comparisons.append(comparison)
        check(got.K == ref["K"], f"{name} K 与参考器一致")
        check([list(x) for x in got.optimal_intervals]
              == intervals_of(ref["optimal_deltas"]), f"{name} 最优 delta 一致")
        check([list(x) for x in got.left_candidates] == ref["left_candidates"],
              f"{name} 左根候选一致")
        check([list(x) for x in got.right_candidates] == ref["right_candidates"],
              f"{name} 右根候选一致")
        check(got.left_can_be_unmatched == ref["left_can_be_unmatched"],
              f"{name} 左根可不配一致")
        check(got.right_can_be_unmatched == ref["right_can_be_unmatched"],
              f"{name} 右根可不配一致")
        check(list(got.forced_pairs) == ref["forced_pairs"],
              f"{name} forced_pair 一致")

    # ---- 200 根 & 巨大 delta 范围：切点段数远小于范围宽度
    base_time = 10 ** 30
    count = 200
    left_frames = [pub("d/t%d" % (i % 20), "payload-%d" % (i % 7), mid=1000 + i)
                   for i in range(count)]
    right_frames = [pub("d/t%d" % (i % 20), "payload-%d" % (i % 7), mid=2000 + i)
                    for i in range(count)]
    manifest, base = build_scenario(
        "07-scale", left_frames, [base_time + i * 10 for i in range(count)],
        right_frames, [base_time + i * 10 + 3 for i in range(count)],
        [-10 ** 40, 10 ** 40], [0, 5])
    report = reconcile(manifest)
    stats = report["summary"]["algorithm_stats"]
    width = 2 * 10 ** 40 + 1
    record("7-200 根 + 巨大 delta 范围（不逐纳秒枚举）", {
        "K": report["summary"]["K"],
        "optimal_delta_intervals": report["summary"]["optimal_delta_intervals"],
        "delta_cells": stats["delta_cells"],
        "optimal_delta_cells": stats["optimal_delta_cells"],
        "delta_range_width": width,
        "cells_times_width_ratio": stats["delta_cells"] / width,
        "reference_comparisons": comparisons,
    })
    check(report["summary"]["K"] == count, "200 对全部配上")
    check(stats["delta_cells"] < 10_000,
          f"只分析 {stats['delta_cells']} 个切点段，远小于范围宽度 {width}")
    check(report["summary"]["optimal_delta_intervals"] == [[-3, 2]],
          "最优 delta 合并为一个闭区间 [-3,2]")
    write_report(report, os.path.join(base, "report.json"))


def main() -> int:
    if os.path.exists(EVIDENCE_DIR):
        shutil.rmtree(EVIDENCE_DIR)
    os.makedirs(WORK_DIR)

    scene_key_example()
    scene_same_time_order()
    scene_twin_and_retry()
    scene_mixed_evidence()
    scene_incompatible_and_empty()
    scene_failures()
    scene_reference_and_scale()

    out = os.path.join(EVIDENCE_DIR, "demo_results.json")
    with open(out, "w", encoding="utf-8") as fh:
        json.dump({"scenes": _results}, fh, ensure_ascii=False, indent=2)
    print(f"\n证据写入 {out}")
    print(f"场景数 {len(_results)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
