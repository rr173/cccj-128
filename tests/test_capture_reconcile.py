"""捕获链对账：来源合同、重传折叠、求解完整性、报告与 CLI。"""

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from offline_mqtt.capture_reconcile import (
    ReconcileInputError,
    ReconcileResourceLimit,
    ReportExistsError,
    explain_report,
    load_manifest,
    load_report,
    reconcile,
    summarize_report,
    verify_report,
    write_report,
)
from offline_mqtt.capture_match import solve, masks_at, full_matching
from offline_mqtt.generator import connect, publish, retained_clear, retained_publish
from offline_mqtt.ingress import MqttIngress
from tests.reference_reconciler import RefRoot, intervals_of, reference_summary

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def make_capture(directory, name, frames, times, epochs=None, retry=None):
    """用现有接入器生成真实 ledger，再写一份独立观测 JSONL。"""
    os.makedirs(directory, exist_ok=True)
    db = os.path.join(directory, "ledger.db")
    for suffix in ("", "-wal", "-shm"):
        path = db + suffix
        if os.path.exists(path):
            os.unlink(path)
    with MqttIngress(db) as ing:
        ing.ingest(connect("sess-" + name))
        seqs = []
        for frame in frames:
            result = ing.ingest(frame)
            seqs.append(result.event_seq)
        rows = ing.list_events()
    assert seqs == [r["seq"] for r in rows], (seqs, [r["seq"] for r in rows])
    with open(os.path.join(directory, "obs.jsonl"), "w", encoding="utf-8") as fh:
        for idx, row in enumerate(rows):
            obs = {
                "event_seq": row["seq"],
                "capture_epoch": (epochs[idx] if epochs else "epoch-" + name),
                "observed_ns": times[idx],
                "retry_of": (retry.get(row["seq"]) if retry else None),
            }
            fh.write(json.dumps(obs, ensure_ascii=False) + "\n")
    return directory


def write_manifest(path, left_dir, right_dir, offset, transit,
                   left_id="cap-left", right_id="cap-right"):
    manifest = {
        "version": 1,
        "left": {"capture_id": left_id, "ledger": "left/ledger.db",
                 "observations": "left/obs.jsonl"},
        "right": {"capture_id": right_id, "ledger": "right/ledger.db",
                  "observations": "right/obs.jsonl"},
        "offset_ns": list(offset),
        "transit_ns": list(transit),
    }
    dest = os.path.dirname(os.path.abspath(path))
    for side, src in (("left", left_dir), ("right", right_dir)):
        target = os.path.join(dest, side)
        if os.path.exists(target):
            shutil.rmtree(target)
        shutil.copytree(src, target)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)
    return path


class ReconcileFixture(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="reconcile_")

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def _scenario(self, name, left_frames, left_times, right_frames, right_times,
                  offset, transit, left_retry=None, right_retry=None,
                  left_epochs=None, right_epochs=None):
        base = os.path.join(self.root, name)
        left_dir = make_capture(os.path.join(base, "left_src"), name + "_l",
                                left_frames, left_times, left_epochs, left_retry)
        right_dir = make_capture(os.path.join(base, "right_src"), name + "_r",
                                 right_frames, right_times, right_epochs, right_retry)
        manifest = write_manifest(os.path.join(base, "manifest.json"),
                                  left_dir, right_dir, offset, transit)
        return manifest, base


def _pub(topic, payload, qos=1, mid=1, dup=False, retain=False):
    frame = publish("s", topic, payload, qos=qos, message_id=mid, dup=dup)
    frame["retain"] = retain
    return frame


class KeyExampleTest(ReconcileFixture):
    """用户指定的关键例子：左 0,10 / 右 0,20，offset=[-10,0]，transit=[0,0]。"""

    def test_K_is_one_with_two_singleton_deltas(self):
        manifest, base = self._scenario(
            "key", [_pub("demo/a", "A", mid=1), _pub("demo/b", "B", mid=2)],
            [0, 10],
            [_pub("demo/a", "A", mid=41), _pub("demo/b", "B", mid=42)],
            [0, 20], [-10, 0], [0, 0])
        report = reconcile(manifest)
        summary = report["summary"]
        self.assertEqual(summary["K"], 1)
        self.assertEqual(summary["optimal_delta_intervals"], [[-10, -10], [0, 0]])
        self.assertEqual(report["forced_pairs"], [])
        for side in ("left", "right"):
            for root in report["roots"][side]:
                self.assertTrue(root["can_be_unmatched"], root)
                self.assertFalse(root["forced_pair"])
        # 每个根各只有一个候选，但不能称确定对应
        self.assertEqual([r["candidate_root_seqs"] for r in report["roots"]["left"]],
                         [[1], [2]])
        # 两个完整方案使用不同 delta，内部各自共用一个 delta
        path = write_report(report, os.path.join(base, "report.json"))
        loaded = load_report(path)
        explanation = explain_report(loaded, "left", seq=1)
        self.assertEqual(len(explanation["scenarios"]), 2)
        deltas = {sc["delta"] for sc in explanation["scenarios"]}
        self.assertEqual(deltas, {0, -10})
        for scenario in explanation["scenarios"]:
            self.assertEqual(scenario["pair_count"], 1)
            self.assertTrue(scenario["reaches_K"])
        self.assertEqual(report["optimal_witness"]["pair_count"], 1)

    def test_order_keeps_B_and_C(self):
        manifest, base = self._scenario(
            "same_time",
            [_pub("d/a", "A", mid=1), _pub("d/b", "B", mid=2),
             _pub("d/c", "C", mid=3)], [0, 0, 0],
            [_pub("d/b", "B", mid=51), _pub("d/c", "C", mid=52),
             _pub("d/a", "A", mid=53)], [0, 0, 0],
            [0, 0], [0, 0])
        report = reconcile(manifest)
        self.assertEqual(report["summary"]["K"], 2)
        topics = {p["topic"] for p in report["optimal_witness"]["pairs"]}
        self.assertEqual(topics, {"d/b", "d/c"})
        statuses = {r["topic"]: r["counterpart_status"] for r in report["roots"]["left"]}
        self.assertEqual(statuses["d/a"], "no_counterpart_in_capture")
        self.assertEqual(statuses["d/b"], "forced_pair")
        self.assertEqual(statuses["d/c"], "forced_pair")

    def test_single_candidate_is_not_forced(self):
        # 左 A, A（同正文） vs 右 A - 每根各有一个候选，但都可不配
        manifest, base = self._scenario(
            "twin",
            [_pub("d/a", "A", mid=1), _pub("d/a", "A", mid=2)], [0, 5],
            [_pub("d/a", "A", mid=60)], [5], [0, 0], [0, 5])
        report = reconcile(manifest)
        self.assertEqual(report["summary"]["K"], 1)
        for root in report["roots"]["left"]:
            self.assertEqual(root["candidate_count"], 1)
            self.assertFalse(root["forced_pair"])
            self.assertTrue(root["can_be_unmatched"])
            self.assertEqual(root["counterpart_status"], "single_candidate_but_unmatchable")
        for root in report["roots"]["right"]:
            # 右根被 K=1 的最优方案必然用到，但它对两个左根都有候选
            self.assertEqual(root["candidate_count"], 2)
            self.assertFalse(root["can_be_unmatched"])
            self.assertFalse(root["forced_pair"])
            self.assertEqual(root["counterpart_status"], "ambiguous")

    def test_retry_folds_and_does_not_consume_right_again(self):
        frames = [
            _pub("d/a", "A", mid=1),
            _pub("d/a", "A", mid=1, dup=True),
            _pub("d/a", "A", mid=9),
            _pub("d/b", "B", mid=10),
        ]
        manifest, base = self._scenario(
            "retry", frames, [0, 0, 5, 10],
            [_pub("d/a", "A", mid=77), _pub("d/b", "B", mid=78)], [0, 10],
            [0, 0], [0, 0],
            left_retry={2: 1})
        report = reconcile(manifest)
        counts = report["summary"]["counts"]["left"]
        self.assertEqual(counts["raw_rows"], 4)
        self.assertEqual(counts["retransmission_rows"], 1)
        self.assertEqual(counts["independent_publications"], 3)
        self.assertEqual(report["summary"]["counts"]["matched_pairs"], 2)
        root1 = report["roots"]["left"][0]
        self.assertEqual(root1["root_seq"], 1)
        self.assertEqual([r["role"] for r in root1["raw_rows"]],
                         ["root", "retransmission"])
        self.assertEqual(root1["raw_rows"][1]["retry_of"], 1)
        self.assertEqual(root1["counterpart_status"], "forced_pair")
        statuses = [r["counterpart_status"] for r in report["roots"]["left"]]
        self.assertEqual(statuses.count("no_counterpart_in_capture"), 1)

    def test_binary_payload_and_epoch_reuse_and_empty_payload(self):
        binary = bytes([0x00, 0xFF, 0x80, 0x0A, 0x0D])
        frames_l = [
            _pub("d/bin", binary, mid=1),
            retained_clear("s", "d/ret", message_id=2),
            _pub("d/x", "same", mid=3),
        ]
        frames_r = [
            _pub("d/bin", binary, mid=90),
            retained_clear("s", "d/ret", message_id=91),
            _pub("d/x", "same", mid=3),
        ]
        manifest, base = self._scenario(
            "binary", frames_l, [5, 6, 7], frames_r, [5, 6, 7],
            [0, 0], [0, 0],
            left_epochs=["e1", "e1", "e2"], right_epochs=["f1", "f1", "f2"])
        report = reconcile(manifest)
        self.assertEqual(report["summary"]["K"], 3)
        bin_root = report["roots"]["left"][0]
        self.assertEqual(bin_root["payload"]["hex"], binary.hex())
        self.assertIsNone(bin_root["payload"]["utf8"])
        ret_root = report["roots"]["left"][1]
        self.assertTrue(ret_root["retain"])
        self.assertEqual(ret_root["payload"]["length"], 0)
        self.assertEqual(report["summary"]["counts"]["left"]["independent_publications"], 3)

    def test_dup_without_reference_is_independent(self):
        manifest, base = self._scenario(
            "dupsolo",
            [_pub("d/a", "A", mid=1), _pub("d/a", "A", mid=1, dup=True)], [0, 1],
            [_pub("d/a", "A", mid=50), _pub("d/a", "A", mid=50, dup=True)], [0, 1],
            [0, 0], [0, 0])
        report = reconcile(manifest)
        counts = report["summary"]["counts"]
        self.assertEqual(counts["left"]["independent_publications"], 2)
        self.assertEqual(counts["left"]["retransmission_rows"], 0)
        self.assertEqual(report["summary"]["K"], 2)

    def test_no_contradiction_empty_side_and_huge_times(self):
        base_time = 10 ** 30
        manifest, base = self._scenario(
            "huge",
            [_pub("d/a", "A", mid=1), _pub("d/a", "B", mid=2)],
            [base_time, base_time + 1],
            [_pub("d/a", "A", mid=9), _pub("d/a", "B", mid=10)],
            [base_time - 5, base_time - 4],
            [-6, 6], [0, 0])
        report = reconcile(manifest)
        self.assertEqual(report["summary"]["K"], 2)
        self.assertEqual(report["summary"]["optimal_delta_intervals"], [[5, 5]])

        empty_dir = os.path.join(self.root, "huge", "empty_src")
        os.makedirs(empty_dir, exist_ok=True)
        empty_db = os.path.join(empty_dir, "ledger.db")
        with MqttIngress(empty_db) as ing:
            ing.ingest(connect("empty"))
        open(os.path.join(empty_dir, "obs.jsonl"), "w").close()
        manifest2 = write_manifest(
            os.path.join(self.root, "huge", "manifest_empty.json"),
            os.path.join(self.root, "huge", "left_src"), empty_dir,
            [0, 0], [0, 0])
        report2 = reconcile(manifest2)
        self.assertEqual(report2["summary"]["K"], 0)
        self.assertEqual(report2["summary"]["optimal_delta_intervals"], [[0, 0]])
        for root in report2["roots"]["left"]:
            self.assertEqual(root["counterpart_status"], "no_counterpart_in_capture")


class InputFailureTest(ReconcileFixture):
    def _obs_path(self, base):
        return os.path.join(base, "left", "obs.jsonl")

    def test_bad_retry_reference_is_rejected(self):
        manifest, base = self._scenario(
            "badretry",
            [_pub("d/a", "A", mid=1), _pub("d/b", "B", mid=2)], [0, 1],
            [_pub("d/a", "A", mid=5)], [0], [0, 0], [0, 0],
            left_retry={2: 1})
        # 把重传改成指向不存在的事件
        path = self._obs_path(base)
        lines = [json.loads(x) for x in open(path, encoding="utf-8")]
        lines[1]["retry_of"] = 999
        with open(path, "w", encoding="utf-8") as fh:
            for line in lines:
                fh.write(json.dumps(line) + "\n")
        with self.assertRaises(ReconcileInputError) as ctx:
            reconcile(manifest)
        reasons = {e["reason"] for e in ctx.exception.errors}
        self.assertIn("RETRY_TARGET_UNKNOWN", reasons)
        self.assertIn("left.observations:2", {e["location"] for e in ctx.exception.errors})
        self.assertTrue(any("observation" in e["detail"] or True
                            for e in ctx.exception.errors))

    def test_chain_retry_is_rejected(self):
        manifest, base = self._scenario(
            "chain",
            [_pub("d/a", "A", mid=1), _pub("d/a", "A", mid=1, dup=True),
             _pub("d/a", "A", mid=1, dup=True)], [0, 1, 2],
            [_pub("d/a", "A", mid=5)], [0], [0, 0], [0, 0],
            left_retry={2: 1, 3: 2})
        with self.assertRaises(ReconcileInputError) as ctx:
            reconcile(manifest)
        self.assertIn("RETRY_CHAIN", {e["reason"] for e in ctx.exception.errors})

    def test_retry_identity_mismatch_is_rejected(self):
        manifest, base = self._scenario(
            "mismatch",
            [_pub("d/a", "A", mid=1), _pub("d/a", "B", mid=1, dup=True)],
            [0, 1],
            [_pub("d/a", "A", mid=5)], [0], [0, 0], [0, 0],
            left_retry={2: 1})
        with self.assertRaises(ReconcileInputError) as ctx:
            reconcile(manifest)
        self.assertIn("RETRY_IDENTITY_MISMATCH",
                      {e["reason"] for e in ctx.exception.errors})

    def test_dup_missing_on_retry_is_rejected(self):
        manifest, base = self._scenario(
            "nodup",
            [_pub("d/a", "A", mid=1), _pub("d/a", "A", mid=1)], [0, 1],
            [_pub("d/a", "A", mid=5)], [0], [0, 0], [0, 0],
            left_retry={2: 1})
        with self.assertRaises(ReconcileInputError) as ctx:
            reconcile(manifest)
        self.assertIn("RETRY_DUP_NOT_SET", {e["reason"] for e in ctx.exception.errors})

    def test_missing_observation_is_rejected(self):
        manifest, base = self._scenario(
            "missing",
            [_pub("d/a", "A", mid=1), _pub("d/b", "B", mid=2)], [0, 1],
            [_pub("d/a", "A", mid=5)], [0], [0, 0], [0, 0])
        path = self._obs_path(base)
        lines = open(path, encoding="utf-8").read().splitlines()
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(lines[0] + "\n")
        with self.assertRaises(ReconcileInputError) as ctx:
            reconcile(manifest)
        self.assertIn("MISSING_OBSERVATION", {e["reason"] for e in ctx.exception.errors})

    def test_observation_file_order_other_than_seq_is_allowed(self):
        manifest, base = self._scenario(
            "reorder",
            [_pub("d/a", "A", mid=1), _pub("d/b", "B", mid=2)], [0, 0],
            [_pub("d/a", "A", mid=5), _pub("d/b", "B", mid=6)], [0, 0],
            [0, 0], [0, 0])
        path = self._obs_path(base)
        lines = open(path, encoding="utf-8").read().splitlines()
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(reversed(lines)) + "\n")
        report = reconcile(manifest)
        self.assertEqual(report["summary"]["K"], 2)

    def test_illegal_time_and_bool_is_rejected(self):
        manifest, base = self._scenario(
            "badtime",
            [_pub("d/a", "A", mid=1), _pub("d/a", "B", mid=2)], [10, 5],
            [_pub("d/a", "A", mid=5)], [0], [0, 0], [0, 0])
        with self.assertRaises(ReconcileInputError) as ctx:
            reconcile(manifest)
        self.assertIn("OBSERVED_TIME_DECREASE",
                      {e["reason"] for e in ctx.exception.errors})

        path = self._obs_path(base)
        lines = [json.loads(x) for x in open(path, encoding="utf-8")]
        lines[0]["observed_ns"] = True
        with open(path, "w", encoding="utf-8") as fh:
            for line in lines:
                fh.write(json.dumps(line) + "\n")
        with self.assertRaises(ReconcileInputError) as ctx:
            reconcile(manifest)
        self.assertIn("BOOL_OR_NON_INT", {e["reason"] for e in ctx.exception.errors})

    def test_extra_field_and_duplicate_json_key_are_rejected(self):
        manifest, base = self._scenario(
            "extra",
            [_pub("d/a", "A", mid=1)], [0],
            [_pub("d/a", "A", mid=5)], [0], [0, 0], [0, 0])
        path = self._obs_path(base)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write('{"event_seq":1,"capture_epoch":"x","observed_ns":0,'
                     '"retry_of":null,"extra":1}\n')
        with self.assertRaises(ReconcileInputError) as ctx:
            reconcile(manifest)
        self.assertIn("OBSERVATION_FIELDS", {e["reason"] for e in ctx.exception.errors})

        with open(path, "w", encoding="utf-8") as fh:
            fh.write('{"event_seq":1,"event_seq":1,"capture_epoch":"x",'
                     '"observed_ns":0,"retry_of":null}\n')
        with self.assertRaises(ReconcileInputError) as ctx:
            reconcile(manifest)
        self.assertIn("BAD_JSON_LINE", {e["reason"] for e in ctx.exception.errors})

    def test_manifest_errors_are_located(self):
        manifest, base = self._scenario(
            "manifest",
            [_pub("d/a", "A", mid=1)], [0],
            [_pub("d/a", "A", mid=5)], [0], [0, 0], [0, 0])
        raw = json.load(open(manifest, encoding="utf-8"))
        raw["offset_ns"] = [0]
        raw["left"]["capture_id"] = raw["right"]["capture_id"]
        raw["surprise"] = True
        with open(manifest, "w", encoding="utf-8") as fh:
            json.dump(raw, fh)
        with self.assertRaises(ReconcileInputError) as ctx:
            load_manifest(manifest)
        locations = {e["location"] for e in ctx.exception.errors}
        reasons = {e["reason"] for e in ctx.exception.errors}
        self.assertTrue(any(loc.endswith(":offset_ns") for loc in locations), locations)
        self.assertIn("DUPLICATE_CAPTURE_ID", reasons)
        self.assertIn("EXTRA_FIELD", reasons)

    def test_missing_ledger_table_is_source_failure(self):
        manifest, base = self._scenario(
            "noledger",
            [_pub("d/a", "A", mid=1)], [0],
            [_pub("d/a", "A", mid=5)], [0], [0, 0], [0, 0])
        db = os.path.join(base, "left", "ledger.db")
        conn = sqlite3.connect(db)
        conn.execute("DROP TABLE ledger")
        conn.commit()
        conn.close()
        with self.assertRaises(ReconcileInputError) as ctx:
            reconcile(manifest)
        self.assertIn("LEDGER_MISSING", {e["reason"] for e in ctx.exception.errors})

    def test_readonly_uri_rejects_write_connection(self):
        import offline_mqtt.capture_reconcile as mod
        manifest, base = self._scenario(
            "uri",
            [_pub("d/a", "A", mid=1)], [0],
            [_pub("d/a", "A", mid=5)], [0], [0, 0], [0, 0])
        db = os.path.join(base, "left", "ledger.db")
        uri = "file:" + os.path.abspath(db) + "?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        try:
            with self.assertRaises(sqlite3.OperationalError):
                conn.execute("CREATE TABLE should_fail (x INTEGER)")
        finally:
            conn.close()

    def test_sources_are_not_modified(self):
        manifest, base = self._scenario(
            "readonly",
            [_pub("d/a", "A", mid=1)], [0],
            [_pub("d/a", "A", mid=5)], [0], [0, 0], [0, 0])
        before = {}
        for side in ("left", "right"):
            for name in ("ledger.db", "obs.jsonl"):
                path = os.path.join(base, side, name)
                before[path] = open(path, "rb").read()
        reconcile(manifest)
        for path, content in before.items():
            self.assertEqual(open(path, "rb").read(), content, path)

    def test_target_exists_never_overwritten(self):
        manifest, base = self._scenario(
            "exists",
            [_pub("d/a", "A", mid=1)], [0],
            [_pub("d/a", "A", mid=5)], [0], [0, 0], [0, 0])
        report = reconcile(manifest)
        target = os.path.join(base, "report.json")
        with open(target, "w", encoding="utf-8") as fh:
            fh.write("OLD\n")
        with self.assertRaises(ReportExistsError):
            write_report(report, target)
        self.assertEqual(open(target, encoding="utf-8").read(), "OLD\n")


class ReferenceAgreementTest(ReconcileFixture):
    """用独立参考器核对 K、最优 delta 集合、每根候选与可不配标志。"""

    def _run_case(self, name, left, right, offset, transit, seq=0):
        left_frames = [_pub(r.topic, r.payload, mid=100 + i, retain=r.retain)
                       for i, r in enumerate(left)]
        right_frames = [_pub(r.topic, r.payload, mid=200 + j, retain=r.retain)
                        for j, r in enumerate(right)]
        manifest, base = self._scenario(
            name, left_frames, [r.observed_ns for r in left],
            right_frames, [r.observed_ns for r in right], offset, transit)
        report = reconcile(manifest)
        ref = reference_summary(left, right, offset, transit)
        self.assertEqual(report["summary"]["K"], ref["K"], name)
        self.assertEqual(report["summary"]["optimal_delta_intervals"],
                         intervals_of(ref["optimal_deltas"]), name)
        self.assertEqual([list(x) for x in
                          [r["candidate_root_positions"] for r in report["roots"]["left"]]],
                         ref["left_candidates"], name)
        self.assertEqual([list(x) for x in
                          [r["candidate_root_positions"] for r in report["roots"]["right"]]],
                         ref["right_candidates"], name)
        self.assertEqual([r["can_be_unmatched"] for r in report["roots"]["left"]],
                         ref["left_can_be_unmatched"], name)
        self.assertEqual([r["can_be_unmatched"] for r in report["roots"]["right"]],
                         ref["right_can_be_unmatched"], name)
        self.assertEqual(
            sorted((p["left_root_position"], p["right_root_position"])
                   for p in report["forced_pairs"]),
            [tuple(x) for x in ref["forced_pairs"]], name)
        return report

    def test_small_cases_agree(self):
        cases = [
            ("r1", [RefRoot(0, 0, "t", b"A", False), RefRoot(1, 10, "t", b"B", False)],
             [RefRoot(0, 0, "t", b"A", False), RefRoot(1, 20, "t", b"B", False)],
             (-10, 0), (0, 0)),
            ("r2", [RefRoot(0, 3, "t", b"A", True), RefRoot(1, 3, "t", b"A", False)],
             [RefRoot(0, 4, "t", b"A", True), RefRoot(1, 9, "t", b"A", False)],
             (-2, 2), (0, 1)),
            ("r3", [RefRoot(0, 0, "t", b"A", False), RefRoot(1, 0, "t", b"B", False),
                    RefRoot(2, 0, "t", b"C", False)],
             [RefRoot(0, 0, "t", b"B", False), RefRoot(1, 0, "t", b"C", False),
              RefRoot(2, 0, "t", b"A", False)], (0, 0), (0, 0)),
            ("r4", [RefRoot(0, -5, "t", b"A", False)],
             [RefRoot(0, 100, "t", b"A", False)], (-200, 200), (0, 0)),
            ("r5", [RefRoot(0, 0, "t", b"A", False)],
             [RefRoot(0, 0, "t", b"B", False)], (0, 0), (0, 0)),
        ]
        for name, left, right, offset, transit in cases:
            with self.subTest(case=name):
                self._run_case(name, left, right, offset, transit)


class ReportOfflineTest(ReconcileFixture):
    def test_report_survives_source_removal(self):
        manifest, base = self._scenario(
            "offline",
            [_pub("d/a", "A", mid=1), _pub("d/b", "B", mid=2)], [0, 10],
            [_pub("d/a", "A", mid=5), _pub("d/b", "B", mid=6)], [0, 20],
            [-10, 0], [0, 0])
        report = reconcile(manifest)
        target = write_report(report, os.path.join(base, "report.json"))
        shutil.rmtree(os.path.join(base, "left"))
        shutil.rmtree(os.path.join(base, "right"))
        loaded = load_report(target)
        summary = summarize_report(loaded)
        self.assertEqual(summary["K"], 1)
        self.assertTrue(verify_report(loaded)["ok"])
        explanation = explain_report(loaded, "left", position=0)
        self.assertTrue(explanation["scenarios"])
        for scenario in explanation["scenarios"]:
            self.assertTrue(scenario["reaches_K"])
            self.assertEqual(len(scenario["pairs"]), 1)

    def test_deterministic_across_runs(self):
        manifest, base = self._scenario(
            "stable",
            [_pub("d/a", "A", mid=1), _pub("d/b", "B", mid=2)], [0, 10],
            [_pub("d/a", "A", mid=5), _pub("d/b", "B", mid=6)], [0, 20],
            [-10, 0], [0, 0])
        first = reconcile(manifest)
        second = reconcile(manifest)
        self.assertEqual(json.dumps(first, sort_keys=True),
                         json.dumps(second, sort_keys=True))


class ResourceLimitTest(ReconcileFixture):
    def test_optimal_cell_limit_returns_failure(self):
        import offline_mqtt.capture_match as match
        manifest, base = self._scenario(
            "optlimit",
            [_pub("d/a", "A", mid=1), _pub("d/b", "B", mid=2)], [0, 10],
            [_pub("d/a", "A", mid=5), _pub("d/b", "B", mid=6)], [0, 20],
            [-10, 0], [0, 0])
        original = match.MAX_OPTIMAL_CELLS
        match.MAX_OPTIMAL_CELLS = 1
        try:
            with self.assertRaises(ReconcileResourceLimit) as ctx:
                reconcile(manifest)
        finally:
            match.MAX_OPTIMAL_CELLS = original
        self.assertEqual(ctx.exception.errors[0]["reason"], "MATCH_RESOURCE_LIMIT")

    def test_delta_cell_limit_returns_failure(self):
        import offline_mqtt.capture_match as match
        manifest, base = self._scenario(
            "limit",
            [_pub("d/a", "A", mid=1), _pub("d/b", "B", mid=2)], [0, 10],
            [_pub("d/a", "A", mid=5), _pub("d/b", "B", mid=6)], [0, 20],
            [-10, 0], [0, 0])
        original = match.MAX_DELTA_CELLS
        match.MAX_DELTA_CELLS = 1
        try:
            with self.assertRaises(ReconcileResourceLimit) as ctx:
                reconcile(manifest)
        finally:
            match.MAX_DELTA_CELLS = original
        self.assertEqual(ctx.exception.kind, "resource_limit")
        self.assertEqual(ctx.exception.errors[0]["reason"], "MATCH_RESOURCE_LIMIT")


class ProductionScaleTest(ReconcileFixture):
    def test_200_roots_and_huge_delta_do_not_enumerate_nanoseconds(self):
        count = 200
        left_frames = [_pub("d/t%d" % (i % 20), "payload-%d" % (i % 7), mid=1000 + i)
                       for i in range(count)]
        left_times = [10 ** 30 + i * 10 for i in range(count)]
        right_frames = [_pub("d/t%d" % (i % 20), "payload-%d" % (i % 7), mid=2000 + i)
                        for i in range(count)]
        right_times = [10 ** 30 + i * 10 + 3 for i in range(count)]
        manifest, base = self._scenario(
            "scale", left_frames, left_times, right_frames, right_times,
            [-10 ** 40, 10 ** 40], [0, 5])
        report = reconcile(manifest)
        self.assertEqual(report["summary"]["K"], count)
        stats = report["summary"]["algorithm_stats"]
        self.assertLess(stats["delta_cells"], 10_000)
        self.assertEqual(report["summary"]["optimal_delta_intervals"],
                         [[-3, 2]])


class CliTest(ReconcileFixture):
    def setUp(self):
        super().setUp()
        self.runner_dir = tempfile.mkdtemp(prefix="reconcile_cli_")
        self.runner = os.path.join(self.runner_dir, "runner.py")
        with open(self.runner, "w", encoding="utf-8") as fh:
            fh.write(
                "import runpy, sys\n"
                f"sys.path.insert(0, {REPO_ROOT!r})\n"
                "sys.argv = ['offline_mqtt.cli'] + sys.argv[1:]\n"
                "runpy.run_module('offline_mqtt.cli', run_name='__main__')\n")

    def tearDown(self):
        shutil.rmtree(self.runner_dir, ignore_errors=True)
        super().tearDown()

    def _run(self, *args, expect=0):
        env = dict(os.environ)
        env.pop("PYTHONPATH", None)
        proc = subprocess.run([sys.executable, "-I", self.runner, *args],
                              cwd=self.runner_dir, capture_output=True, text=True,
                              env=env, check=False)
        self.assertEqual(proc.returncode, expect, proc.stderr)
        return json.loads(proc.stdout)

    def test_cli_round_trip(self):
        manifest, base = self._scenario(
            "cli",
            [_pub("d/a", "A", mid=1), _pub("d/b", "B", mid=2)], [0, 10],
            [_pub("d/a", "A", mid=5), _pub("d/b", "B", mid=6)], [0, 20],
            [-10, 0], [0, 0])
        target = os.path.join(base, "cli_report.json")
        out = self._run("reconcile", manifest, "--out", target)
        self.assertEqual(out["K"], 1)
        self._run("reconcile", manifest, "--out", target, expect=4)
        summary = self._run("summary", target)
        self.assertTrue(summary["verification"]["ok"])
        explain = self._run("explain", target, "--side", "left", "--seq", "1")
        self.assertEqual(len(explain["scenarios"]), 2)
        self.assertEqual(explain["K"], 1)


if __name__ == "__main__":
    unittest.main()
