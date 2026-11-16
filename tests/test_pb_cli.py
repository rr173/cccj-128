"""export / inspect CLI 的跨进程行为与退出码。"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

from offline_mqtt import _protobuf_vendor  # noqa: F401

from google.protobuf import message_factory

from offline_mqtt.ingress import MqttIngress
from offline_mqtt.pb_descriptor import load_descriptor_set
from tests._pb_fixtures import build_demo_descriptor_set

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def run_cli(*args):
    return subprocess.run(
        [sys.executable, "-m", "offline_mqtt.cli", *args],
        cwd=REPO_ROOT, capture_output=True, text=True)


class TestPbCli(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="pb_cli_test_")
        self.addCleanup(shutil.rmtree, self.dir, True)
        self.desc = os.path.join(self.dir, "desc.bin")
        with open(self.desc, "wb") as fh:
            fh.write(build_demo_descriptor_set())
        loaded = load_descriptor_set(build_demo_descriptor_set())
        cls = message_factory.GetMessageClass(
            loaded.pool.FindMessageTypeByName("demo.M1"))
        payload = cls(secret="id-x", n=3).SerializeToString()
        self.db = os.path.join(self.dir, "ledger.db")
        with MqttIngress(self.db) as ing:
            ing.ingest({"type": "PUBLISH", "session_id": "s", "topic": "t/m1",
                        "payload": payload, "qos": 1, "message_id": 7})
        self.cfg = os.path.join(self.dir, "cfg.json")
        with open(self.cfg, "w") as fh:
            json.dump({"topics": [{"topic": "t/m1", "message_type": "demo.M1",
                                   "remove_paths": [[1]]}]}, fh)
        self.out = os.path.join(self.dir, "out")

    def test_export_then_inspect(self):
        proc = run_cli("export", self.db, "--descriptor", self.desc,
                       "--config", self.cfg, "--out", self.out)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        summary = json.loads(proc.stdout)
        self.assertEqual(summary["selected_records"], 1)
        self.assertEqual(summary["modified_records"], 1)

        proc = run_cli("inspect", self.out)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        result = json.loads(proc.stdout)
        self.assertTrue(result["ok"])
        self.assertEqual(result["checked_records"], 1)

    def test_export_target_exists_exit4(self):
        os.makedirs(self.out)
        proc = run_cli("export", self.db, "--descriptor", self.desc,
                       "--config", self.cfg, "--out", self.out)
        self.assertEqual(proc.returncode, 4)
        self.assertEqual(json.loads(proc.stdout)["error"]["category"],
                         "target_exists")

    def test_export_bad_config_exit2(self):
        with open(self.cfg, "w") as fh:
            json.dump({"topics": [{"topic": "t/m1", "message_type": "demo.M1",
                                   "remove_paths": [[9]]}]}, fh)
        proc = run_cli("export", self.db, "--descriptor", self.desc,
                       "--config", self.cfg, "--out", self.out)
        self.assertEqual(proc.returncode, 2)
        self.assertEqual(json.loads(proc.stdout)["error"]["category"],
                         "config_error")
        self.assertFalse(os.path.exists(self.out))

    def test_export_limit_override_exit3(self):
        proc = run_cli("export", self.db, "--descriptor", self.desc,
                       "--config", self.cfg, "--out", self.out,
                       "--max-bytes", "2")
        self.assertEqual(proc.returncode, 3)
        self.assertEqual(json.loads(proc.stdout)["error"]["category"],
                         "resource_limit")

    def test_inspect_incomplete_exit6(self):
        os.makedirs(self.out)  # 空目录：没有完成标记
        proc = run_cli("inspect", self.out)
        self.assertEqual(proc.returncode, 6)
        self.assertEqual(json.loads(proc.stdout)["error"]["category"],
                         "export_incomplete")

    def test_error_json_has_no_payload_echo(self):
        # 坏正文：错误输出只含类别/序号/字段路径/字节位置，不回显正文
        with MqttIngress(self.db) as ing:
            ing.ingest({"type": "PUBLISH", "session_id": "s", "topic": "t/m1",
                        "payload": bytes.fromhex("4a0100"), "qos": 0})
        proc = run_cli("export", self.db, "--descriptor", self.desc,
                       "--config", self.cfg, "--out", self.out)
        self.assertEqual(proc.returncode, 2)
        err = json.loads(proc.stdout)["error"]
        self.assertEqual(err["category"], "unknown_field")
        self.assertEqual(err["event_seq"], 2)
        self.assertEqual(err["field_path"], [9])
        self.assertEqual(err["byte_offset"], 0)
        self.assertNotIn("4a0100", proc.stdout)


if __name__ == "__main__":
    unittest.main()
