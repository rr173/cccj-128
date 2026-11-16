"""端到端：只读来源账本 → 导出目录 → inspect → 读取接口 → 重新接入空库。"""

import base64
import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
import unittest

from offline_mqtt import _protobuf_vendor  # noqa: F401

from google.protobuf import message_factory

from offline_mqtt.ingress import MqttIngress
from offline_mqtt.pb_errors import (
    CONFIG_ERROR,
    EXPORT_INCOMPLETE,
    INVALID_UTF8,
    IO_ERROR,
    RESOURCE_LIMIT,
    TARGET_EXISTS,
    TRUNCATED,
    UNKNOWN_FIELD,
    ExportError,
)
from offline_mqtt.pb_export import (
    COMPLETE_FILE,
    MESSAGES_FILE,
    REPORT_FILE,
    export_ledger,
)
from offline_mqtt.pb_inspect import inspect_export
from offline_mqtt.pb_reader import iter_export
from tests._pb_fixtures import build_demo_descriptor_set


def _sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


class ExportTestBase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="pb_export_test_")
        self.addCleanup(shutil.rmtree, self.dir, True)
        self.descriptor_path = os.path.join(self.dir, "descriptor.bin")
        with open(self.descriptor_path, "wb") as fh:
            fh.write(build_demo_descriptor_set())
        self.ledger_path = os.path.join(self.dir, "ledger.db")
        self.out_dir = os.path.join(self.dir, "out")

    # ------------------------------------------------------------- helpers
    def _write_config(self, topics, limits=None, name="config.json"):
        cfg = {"version": 1, "topics": topics}
        if limits:
            cfg["limits"] = limits
        path = os.path.join(self.dir, name)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(cfg, fh)
        return path

    def _make_payloads(self):
        """用官方动态消息生成载荷（独立于被测重写器）。"""
        from offline_mqtt.pb_descriptor import load_descriptor_set
        loaded = load_descriptor_set(build_demo_descriptor_set())

        def cls(name):
            return message_factory.GetMessageClass(
                loaded.pool.FindMessageTypeByName(name))

        m1 = cls("demo.M1")(secret="id-001", n=7, note="keep-me")
        m1_b = cls("demo.M1")(secret="id-002", n=8)
        outer = cls("demo.Outer")(
            sub=cls("demo.Inner")(secret="top-secret", label="lbl"),
            keep="kept")
        nums = cls("demo.Nums")(samples=[1, 2, 3], tail=9, secret="s")
        return {
            "m1": m1.SerializeToString(),
            "m1_b": m1_b.SerializeToString(),
            "outer": outer.SerializeToString(),
            "nums": nums.SerializeToString(),
        }

    def _seed_ledger(self, path=None):
        """用现有接入器入账：配置主题 + 未配置主题 + dup/retain/多会话。"""
        path = path or self.ledger_path
        payloads = self._make_payloads()
        with MqttIngress(path) as ing:
            ing.ingest({"type": "CONNECT", "client_id": "dev-1"})
            ing.ingest({"type": "CONNECT", "client_id": "dev-2"})
            ing.ingest({"type": "PUBLISH", "session_id": "dev-1",
                        "topic": "factory/device", "payload": payloads["m1"],
                        "qos": 1, "message_id": 1})
            ing.ingest({"type": "PUBLISH", "session_id": "dev-1",
                        "topic": "factory/device", "payload": payloads["m1_b"],
                        "qos": 0})
            ing.ingest({"type": "PUBLISH", "session_id": "dev-1",
                        "topic": "factory/outer", "payload": payloads["outer"],
                        "qos": 0, "retain": True})
            ing.ingest({"type": "PUBLISH", "session_id": "dev-2",
                        "topic": "factory/nums", "payload": payloads["nums"],
                        "qos": 2, "message_id": 9, "dup": True})
            # 相同正文再发一次（dup 重传）：必须各自入账、不折叠
            ing.ingest({"type": "PUBLISH", "session_id": "dev-2",
                        "topic": "factory/nums", "payload": payloads["nums"],
                        "qos": 2, "message_id": 9, "dup": True})
            # 未列入配置的主题：不交付
            ing.ingest({"type": "PUBLISH", "session_id": "dev-1",
                        "topic": "other/topic", "payload": b"\x0a\x01x",
                        "qos": 0})
        return payloads

    def _default_config(self):
        return self._write_config([
            {"topic": "factory/device", "message_type": "demo.M1",
             "remove_paths": [[1]]},
            {"topic": "factory/outer", "message_type": "demo.Outer",
             "remove_paths": [[1, 1]]},
            {"topic": "factory/nums", "message_type": "demo.Nums",
             "remove_paths": [[3]]},
        ])

    def _read_jsonl(self, path):
        with open(path, "r", encoding="utf-8") as fh:
            return [json.loads(line) for line in fh if line.strip()]


class TestExportHappyPath(ExportTestBase):
    def test_export_inspect_reingest(self):
        payloads = self._seed_ledger()
        config = self._default_config()
        src_hash = _sha256(self.ledger_path)

        summary = export_ledger(self.ledger_path, self.descriptor_path,
                                config, self.out_dir)
        self.assertEqual(summary["status"], "complete")
        # 选中 5 条：device 2 + outer 1 + nums 2；other/topic 不交付
        self.assertEqual(summary["selected_records"], 5)
        self.assertEqual(summary["modified_records"], 5)

        # 来源未动
        self.assertEqual(_sha256(self.ledger_path), src_hash)

        # inspect 通过
        result = inspect_export(self.out_dir)
        self.assertTrue(result["ok"])
        self.assertEqual(result["selected_records"], 5)

        # 报告内容
        report = json.load(open(os.path.join(self.out_dir, REPORT_FILE)))
        self.assertEqual(report["selected_records"], 5)
        self.assertEqual(report["path_hits"]["factory/device"], {"1": 2})
        self.assertEqual(report["path_hits"]["factory/outer"], {"1.1": 1})
        self.assertEqual(report["path_hits"]["factory/nums"], {"3": 2})
        seqs = [r["event_seq"] for r in report["records"]]
        self.assertEqual(seqs, sorted(seqs))

        # 读取接口 → 重新接入空库
        records = list(iter_export(self.out_dir))
        self.assertEqual(len(records), 5)
        new_db = os.path.join(self.dir, "reingest.db")
        with MqttIngress(new_db) as ing:
            for rec in records:
                res = ing.ingest(rec.to_ingest_record())
                self.assertTrue(res.accepted, res)
            events = ing.list_events()
        self.assertEqual(len(events), 5)
        # topic/qos/retain/dup/message_id/session_id 保持
        for rec, ev in zip(records, events):
            self.assertEqual(ev["topic"], rec.topic)
            self.assertEqual(bytes(ev["payload"]), rec.payload)
            self.assertEqual(ev["qos"], rec.qos)
            self.assertEqual(bool(ev["retain"]), rec.retain)
            self.assertEqual(bool(ev["dup"]), rec.dup)
            self.assertEqual(ev["message_id"], rec.message_id)
            self.assertEqual(ev["session_id"], rec.session_id)

        # 删除语义：m1 的 secret 消失、note 保留；nums 的 secret 消失
        from offline_mqtt.pb_descriptor import load_descriptor_set
        loaded = load_descriptor_set(build_demo_descriptor_set())
        m1_cls = message_factory.GetMessageClass(
            loaded.pool.FindMessageTypeByName("demo.M1"))
        first = m1_cls.FromString(records[0].payload)
        self.assertEqual(first.secret, "")
        self.assertEqual(first.n, 7)
        self.assertEqual(first.note, "keep-me")
        self.assertNotIn(b"id-001", records[0].payload)
        self.assertNotIn(b"id-002", records[1].payload)
        outer_cls = message_factory.GetMessageClass(
            loaded.pool.FindMessageTypeByName("demo.Outer"))
        third = outer_cls.FromString(records[2].payload)
        self.assertEqual(third.keep, "kept")
        self.assertEqual(third.sub.label, "lbl")
        self.assertEqual(third.sub.secret, "")
        self.assertNotIn(b"top-secret", records[2].payload)
        nums_cls = message_factory.GetMessageClass(
            loaded.pool.FindMessageTypeByName("demo.Nums"))
        fourth = nums_cls.FromString(records[3].payload)
        self.assertEqual(list(fourth.samples), [1, 2, 3])
        self.assertEqual(fourth.secret, "")

    def test_every_occurrence_removed_on_the_wire(self):
        # singular 字段先旧后新两次出现：两处都必须消失
        config = self._write_config([
            {"topic": "t/m1", "message_type": "demo.M1", "remove_paths": [[1]]},
        ])
        payload = bytes.fromhex("0a036f6c64" "0a036e6577" "1001")  # old,new,n=1
        with MqttIngress(self.ledger_path) as ing:
            ing.ingest({"type": "PUBLISH", "session_id": "s",
                        "topic": "t/m1", "payload": payload, "qos": 0})
        export_ledger(self.ledger_path, self.descriptor_path, config, self.out_dir)
        (rec,) = iter_export(self.out_dir)
        self.assertEqual(rec.payload, bytes.fromhex("1001"))
        self.assertNotIn(b"old", rec.payload)
        self.assertNotIn(b"new", rec.payload)

    def test_unconfigured_topic_not_exported(self):
        self._seed_ledger()
        config = self._write_config([
            {"topic": "factory/nums", "message_type": "demo.Nums",
             "remove_paths": [[3]]},
        ])
        summary = export_ledger(self.ledger_path, self.descriptor_path,
                                config, self.out_dir)
        self.assertEqual(summary["selected_records"], 2)
        topics = {r.topic for r in iter_export(self.out_dir)}
        self.assertEqual(topics, {"factory/nums"})

    def test_duplicate_publishes_not_collapsed(self):
        self._seed_ledger()
        config = self._write_config([
            {"topic": "factory/nums", "message_type": "demo.Nums",
             "remove_paths": [[3]]},
        ])
        summary = export_ledger(self.ledger_path, self.descriptor_path,
                                config, self.out_dir)
        # 两条完全相同正文的 dup 发布各自保留
        self.assertEqual(summary["selected_records"], 2)
        report = json.load(open(os.path.join(self.out_dir, REPORT_FILE)))
        self.assertEqual(len({r["event_seq"] for r in report["records"]}), 2)


class TestExportFailures(ExportTestBase):
    def test_target_exists(self):
        self._seed_ledger()
        config = self._default_config()
        os.makedirs(self.out_dir)
        with self.assertRaises(ExportError) as cm:
            export_ledger(self.ledger_path, self.descriptor_path,
                          config, self.out_dir)
        self.assertEqual(cm.exception.category, TARGET_EXISTS)
        self.assertEqual(cm.exception.exit_code, 4)

    def test_corrupt_last_record_aborts_all(self):
        self._seed_ledger()
        config = self._default_config()
        # 在账本末尾追加一条坏正文（模拟封存来源受损）：非法 UTF-8 string
        conn = sqlite3.connect(self.ledger_path)
        conn.execute(
            "INSERT INTO ledger(session_id, topic, payload, qos, retain, dup,"
            " message_id, ingested_at) VALUES('dev-9','factory/nums',X'1A01FF',"
            " 0,0,0,NULL,0)")
        conn.commit()
        conn.close()
        src_hash = _sha256(self.ledger_path)
        with self.assertRaises(ExportError) as cm:
            export_ledger(self.ledger_path, self.descriptor_path,
                          config, self.out_dir)
        self.assertEqual(cm.exception.category, INVALID_UTF8)
        # 不留下完整结果
        self.assertFalse(os.path.exists(self.out_dir))
        with self.assertRaises(ExportError) as cm2:
            inspect_export(self.out_dir)
        self.assertEqual(cm2.exception.category, EXPORT_INCOMPLETE)
        # 来源未动
        self.assertEqual(_sha256(self.ledger_path), src_hash)

    def test_unknown_field_aborts(self):
        config = self._write_config([
            {"topic": "t/m1", "message_type": "demo.M1", "remove_paths": [[1]]},
        ])
        with MqttIngress(self.ledger_path) as ing:
            ing.ingest({"type": "PUBLISH", "session_id": "s", "topic": "t/m1",
                        "payload": bytes.fromhex("0a0161" "4a0100"), "qos": 0})
        with self.assertRaises(ExportError) as cm:
            export_ledger(self.ledger_path, self.descriptor_path,
                          config, self.out_dir)
        self.assertEqual(cm.exception.category, UNKNOWN_FIELD)
        self.assertEqual(cm.exception.event_seq, 1)
        self.assertEqual(cm.exception.field_path, (9,))
        self.assertFalse(os.path.exists(self.out_dir))

    def test_truncated_aborts(self):
        config = self._write_config([
            {"topic": "t/m1", "message_type": "demo.M1", "remove_paths": [[1]]},
        ])
        with MqttIngress(self.ledger_path) as ing:
            ing.ingest({"type": "PUBLISH", "session_id": "s", "topic": "t/m1",
                        "payload": bytes.fromhex("1081"), "qos": 0})
        with self.assertRaises(ExportError) as cm:
            export_ledger(self.ledger_path, self.descriptor_path,
                          config, self.out_dir)
        self.assertEqual(cm.exception.category, TRUNCATED)
        self.assertFalse(os.path.exists(self.out_dir))

    def test_resource_limit_aborts(self):
        self._seed_ledger()
        config = self._default_config()
        with self.assertRaises(ExportError) as cm:
            export_ledger(self.ledger_path, self.descriptor_path, config,
                          self.out_dir,
                          limits_override={"max_bytes": 4})
        self.assertEqual(cm.exception.category, RESOURCE_LIMIT)
        self.assertEqual(cm.exception.exit_code, 3)
        self.assertFalse(os.path.exists(self.out_dir))

    def test_map_key_path_rejected(self):
        self._seed_ledger()
        # 单独删 map 的 key：配置阶段就必须拒绝
        bad_config = self._write_config([
            {"topic": "t/map", "message_type": "demo.WithMap",
             "remove_paths": [[1, 1]]},
        ], name="bad.json")
        with self.assertRaises(ExportError) as cm:
            export_ledger(self.ledger_path, self.descriptor_path,
                          bad_config, self.out_dir)
        self.assertEqual(cm.exception.category, CONFIG_ERROR)
        self.assertIn("key", cm.exception.detail)

    def test_bad_descriptor_dependency(self):
        from google.protobuf import descriptor_pb2
        fds = descriptor_pb2.FileDescriptorSet()
        f = fds.file.add()
        f.name = "x.proto"; f.package = "x"; f.syntax = "proto3"
        f.dependency.append("missing.proto")
        f.message_type.add().name = "M"
        bad_desc = os.path.join(self.dir, "bad.bin")
        with open(bad_desc, "wb") as fh:
            fh.write(fds.SerializeToString())
        self._seed_ledger()
        config = self._default_config()
        with self.assertRaises(ExportError) as cm:
            export_ledger(self.ledger_path, bad_desc, config, self.out_dir)
        self.assertIn("missing.proto", cm.exception.detail)
        self.assertFalse(os.path.exists(self.out_dir))

    def test_unknown_message_type_in_config(self):
        self._seed_ledger()
        config = self._write_config([
            {"topic": "factory/nums", "message_type": "demo.Nope",
             "remove_paths": [[1]]},
        ])
        with self.assertRaises(ExportError) as cm:
            export_ledger(self.ledger_path, self.descriptor_path,
                          config, self.out_dir)
        self.assertEqual(cm.exception.category, CONFIG_ERROR)

    def test_missing_ledger(self):
        config = self._default_config()
        with self.assertRaises(ExportError):
            export_ledger(os.path.join(self.dir, "nope.db"),
                          self.descriptor_path, config, self.out_dir)

    def test_unwritable_output_parent_io_error(self):
        # 实际写出失败：输出目录的父目录只读 → IO 错误（退出码 5），不留残骸
        self._seed_ledger()
        config = self._default_config()
        ro_dir = os.path.join(self.dir, "readonly")
        os.makedirs(ro_dir)
        os.chmod(ro_dir, 0o555)
        self.addCleanup(os.chmod, ro_dir, 0o755)
        with self.assertRaises(ExportError) as cm:
            export_ledger(self.ledger_path, self.descriptor_path, config,
                          os.path.join(ro_dir, "out"))
        self.assertEqual(cm.exception.category, IO_ERROR)
        self.assertEqual(cm.exception.exit_code, 5)
        self.assertEqual(os.listdir(ro_dir), [])

    def test_source_not_modified_and_no_side_files(self):
        self._seed_ledger()
        # 封存：checkpoint 后记录目录快照
        conn = sqlite3.connect(self.ledger_path)
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()
        config = self._default_config()
        before = _sha256(self.ledger_path)
        export_ledger(self.ledger_path, self.descriptor_path, config, self.out_dir)
        self.assertEqual(_sha256(self.ledger_path), before)
        # 不在来源旁产生 wal/shm
        self.assertFalse(os.path.exists(self.ledger_path + "-wal"))
        self.assertFalse(os.path.exists(self.ledger_path + "-shm"))

    def test_existing_neighbor_files_untouched(self):
        self._seed_ledger()
        sentinel = os.path.join(self.dir, "sentinel.txt")
        with open(sentinel, "w") as fh:
            fh.write("do-not-touch")
        config = self._default_config()
        export_ledger(self.ledger_path, self.descriptor_path, config, self.out_dir)
        with open(sentinel) as fh:
            self.assertEqual(fh.read(), "do-not-touch")


class TestExportEdgePayloads(ExportTestBase):
    def test_empty_payload_and_untouched_record(self):
        # 空载荷是合法空 message；无命中字段的记录原样导出且不计入改动
        config = self._write_config([
            {"topic": "t/m1", "message_type": "demo.M1", "remove_paths": [[1]]},
        ])
        with MqttIngress(self.ledger_path) as ing:
            ing.ingest({"type": "PUBLISH", "session_id": "s", "topic": "t/m1",
                        "payload": b"", "qos": 0})
            ing.ingest({"type": "PUBLISH", "session_id": "s", "topic": "t/m1",
                        "payload": bytes.fromhex("1005"), "qos": 0})  # 只有 n=5
        summary = export_ledger(self.ledger_path, self.descriptor_path,
                                config, self.out_dir)
        self.assertEqual(summary["selected_records"], 2)
        self.assertEqual(summary["modified_records"], 0)
        records = list(iter_export(self.out_dir))
        self.assertEqual(records[0].payload, b"")
        self.assertEqual(records[1].payload, bytes.fromhex("1005"))
        result = inspect_export(self.out_dir)
        self.assertTrue(result["ok"])

    def test_empty_selection_is_valid(self):
        # 配置的主题在账本中没有记录：合法的零条导出
        config = self._write_config([
            {"topic": "t/absent", "message_type": "demo.M1",
             "remove_paths": [[1]]},
        ])
        with MqttIngress(self.ledger_path) as ing:
            ing.ingest({"type": "PUBLISH", "session_id": "s", "topic": "t/m1",
                        "payload": bytes.fromhex("1005"), "qos": 0})
        summary = export_ledger(self.ledger_path, self.descriptor_path,
                                config, self.out_dir)
        self.assertEqual(summary["selected_records"], 0)
        self.assertEqual(summary["modified_records"], 0)
        self.assertEqual(list(iter_export(self.out_dir)), [])
        self.assertTrue(inspect_export(self.out_dir)["ok"])


class TestInspectAndReaderGuards(ExportTestBase):
    def test_inspect_requires_complete_marker(self):
        self._seed_ledger()
        config = self._default_config()
        export_ledger(self.ledger_path, self.descriptor_path, config, self.out_dir)
        os.unlink(os.path.join(self.out_dir, COMPLETE_FILE))
        with self.assertRaises(ExportError) as cm:
            inspect_export(self.out_dir)
        self.assertEqual(cm.exception.category, EXPORT_INCOMPLETE)
        with self.assertRaises(ExportError):
            list(iter_export(self.out_dir))

    def test_inspect_detects_tampered_messages(self):
        self._seed_ledger()
        config = self._default_config()
        export_ledger(self.ledger_path, self.descriptor_path, config, self.out_dir)
        with open(os.path.join(self.out_dir, MESSAGES_FILE), "a") as fh:
            fh.write("\n")
        with self.assertRaises(ExportError):
            inspect_export(self.out_dir)

    def test_inspect_works_after_source_and_config_removed(self):
        self._seed_ledger()
        config = self._default_config()
        export_ledger(self.ledger_path, self.descriptor_path, config, self.out_dir)
        # 移走来源与配置后，inspect 仍能核实导出
        os.unlink(self.ledger_path)
        os.unlink(config)
        result = inspect_export(self.out_dir)
        self.assertTrue(result["ok"])
        self.assertEqual(result["selected_records"], 5)
        records = list(iter_export(self.out_dir))
        self.assertEqual(len(records), 5)


if __name__ == "__main__":
    unittest.main()
