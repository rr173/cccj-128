"""CLI 子进程验证：check-topic / check-scope / csubscribe / cpoll 与退出码约定。

沿用 test_restart 的做法：独立临时目录里的 runner 脚本 + ``python -I``，
保证跨进程、隔离模式下结论与库内状态一致。
"""

import json
import os
import subprocess
import sys
import tempfile
import unittest

from offline_mqtt.generator import connect, publish, retained_publish
from offline_mqtt.ingress import MqttIngress
from tests._helpers import TempDbMixin

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_RUNNER_SOURCE = (
    "import runpy, sys\n"
    f"sys.path.insert(0, {REPO_ROOT!r})\n"
    "sys.argv = ['offline_mqtt.cli'] + sys.argv[1:]\n"
    "runpy.run_module('offline_mqtt.cli', run_name='__main__')\n"
)

POLICY = {
    "version": 1,
    "principals": [
        {"principal_id": "team-plant", "groups": ["plant-team"]},
        {"principal_id": "team-payroll", "groups": []},
    ],
    "rules": [
        {"rule_id": "a1", "subject_type": "principal", "subject_id": "team-plant",
         "effect": "allow", "filter": "plant/#"},
        {"rule_id": "d1", "subject_type": "group", "subject_id": "plant-team",
         "effect": "deny", "filter": "plant/private/#"},
        {"rule_id": "pay", "subject_type": "principal", "subject_id": "team-payroll",
         "effect": "allow", "filter": "payroll/#"},
    ],
}

BAD_POLICY = {"version": 1,
              "principals": [{"principal_id": "a", "groups": []}],
              "rules": [{"rule_id": "x", "subject_type": "group",
                         "subject_id": "missing-group", "effect": "allow",
                         "filter": "Bad/#"}]}


class CliAdmissionTest(TempDbMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self._runner_dir = tempfile.mkdtemp(prefix="mqtt_cli_")
        self.runner_path = os.path.join(self._runner_dir, "cli_runner.py")
        with open(self.runner_path, "w", encoding="utf-8") as fh:
            fh.write(_RUNNER_SOURCE)
        self.policy_path = os.path.join(self._runner_dir, "policy.json")
        self.bad_policy_path = os.path.join(self._runner_dir, "bad_policy.json")
        with open(self.policy_path, "w", encoding="utf-8") as fh:
            json.dump(POLICY, fh, ensure_ascii=False)
        with open(self.bad_policy_path, "w", encoding="utf-8") as fh:
            json.dump(BAD_POLICY, fh, ensure_ascii=False)

    def tearDown(self):
        for name in os.listdir(self._runner_dir):
            os.unlink(os.path.join(self._runner_dir, name))
        os.rmdir(self._runner_dir)
        super().tearDown()

    def _run(self, *args, expect_code=0):
        env = dict(os.environ)
        env.pop("PYTHONPATH", None)
        proc = subprocess.run(
            [sys.executable, "-I", self.runner_path, *args],
            cwd=self._runner_dir, capture_output=True, text=True, env=env, check=False,
        )
        self.assertEqual(proc.returncode, expect_code, proc.stderr)
        return json.loads(proc.stdout)

    def test_check_topic_allow_and_deny(self):
        ok = self._run("check-topic", self.policy_path, "team-plant", "plant/temp")
        self.assertTrue(ok["allowed"])
        self.assertEqual(ok["allow_rule_ids"], ["a1"])
        denied = self._run("check-topic", self.policy_path, "team-plant",
                           "plant/private/x")
        self.assertFalse(denied["allowed"])
        self.assertEqual(denied["deny_rule_ids"], ["d1"])
        self.assertEqual(denied["reason"], "DENY_RULE_MATCHED")

    def test_check_scope_safe_and_unsafe(self):
        safe = self._run("check-scope", self.policy_path, "team-plant", "plant/temp/#")
        self.assertTrue(safe["safe"])
        unsafe = self._run("check-scope", self.policy_path, "team-plant", "plant/#")
        self.assertFalse(unsafe["safe"])
        self.assertTrue(unsafe["witness"]["witness_topic"].startswith("plant/private"))

    def test_unknown_principal_is_input_error_exit_2(self):
        out = self._run("check-scope", self.policy_path, "ghost", "#", expect_code=2)
        self.assertEqual(out["status"], "error")
        self.assertEqual(out["error_kind"], "admission_input_error")

    def test_bad_policy_is_input_error_exit_2(self):
        out = self._run("check-topic", self.bad_policy_path, "a", "x", expect_code=2)
        self.assertEqual(out["error_kind"], "policy_input_error")
        reasons = {e["reason"] for e in out["errors"]}
        self.assertIn("UNKNOWN_GROUP_REF", reasons)
        self.assertIn("INVALID_LEVEL_CHAR", reasons)

    def test_csubscribe_grant_and_deny_and_cpoll_filtering(self):
        with MqttIngress(self.db_path) as ing:
            ing.ingest(connect("s"))
            ing.ingest(retained_publish("s", "plant/temp/cur", "retained-ok", message_id=1))
            ing.ingest(retained_publish("s", "Plant/Upper", "non-namespace", message_id=2))

        # 越权请求：拒绝且退出码仍是 0（结论在 JSON），库中不新增订阅
        denied = self._run("csubscribe", self.db_path, self.policy_path,
                           "team-plant", "plant/private/#")
        self.assertFalse(denied["granted"])
        self.assertIsNone(denied["subscription_id"])
        with MqttIngress(self.db_path) as ing:
            self.assertEqual(
                ing.store.conn.execute("SELECT COUNT(*) FROM subscriptions").fetchone()[0], 0)

        # 获准请求：创建真实订阅，返回实际 subscription_id
        granted = self._run("csubscribe", self.db_path, self.policy_path,
                            "team-plant", "plant/temp/#")
        self.assertTrue(granted["granted"])
        sub_id = granted["subscription_id"]
        self.assertIsInstance(sub_id, int)

        # cpoll 只交付采集命名空间内的 retained，大写主题被剔除
        polled = self._run("cpoll", self.db_path, str(sub_id))
        topics = [m["topic"] for m in polled["messages"]]
        self.assertIn("plant/temp/cur", topics)
        self.assertNotIn("Plant/Upper", topics)

        # 发布一条普通实时消息并受控读回（落在获准过滤器 plant/temp/# 范围内）
        with MqttIngress(self.db_path) as ing:
            ing.ingest(publish("s", "plant/temp/live", "live-payload",
                               qos=1, message_id=7))
        polled2 = self._run("cpoll", self.db_path, str(sub_id))
        self.assertEqual([m["topic"] for m in polled2["messages"]],
                         ["plant/temp/live"])

    def test_csubscribe_denied_leaves_no_cursor_or_retained_rows(self):
        with MqttIngress(self.db_path) as ing:
            ing.ingest(connect("s"))
            ing.ingest(retained_publish("s", "plant/temp", "v", message_id=1))
        self._run("csubscribe", self.db_path, self.policy_path,
                  "team-plant", "plant/private/x")
        with MqttIngress(self.db_path) as ing:
            for table in ("subscriptions", "subscription_retained", "delivery_cursor"):
                count = ing.store.conn.execute(
                    f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                self.assertEqual(count, 0, f"{table} 不应有残留行")


if __name__ == "__main__":
    unittest.main()
