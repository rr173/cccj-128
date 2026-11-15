"""进程重启：重新打开同一数据库后账本、保留值、订阅游标完整可用。

包含两类验证：
1. 同进程内关闭并重新打开接入器（模拟状态全部来自磁盘）；
2. 通过 ``subprocess`` 以 ``python -I`` 启动独立进程跑 CLI，真正跨进程验证。
"""

import json
import os
import subprocess
import sys
import tempfile
import unittest

from offline_mqtt.generator import (
    connect,
    publish,
    retained_clear,
    retained_publish,
    subscribe,
)
from offline_mqtt.ingress import MqttIngress
from tests._helpers import TempDbMixin

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 独立进程入口：python -I 不自动注入 cwd，需显式把仓库根目录放进 sys.path。
_RUNNER_SOURCE = (
    "import runpy, sys\n"
    f"sys.path.insert(0, {REPO_ROOT!r})\n"
    "sys.argv = ['offline_mqtt.cli'] + sys.argv[1:]\n"
    "runpy.run_module('offline_mqtt.cli', run_name='__main__')\n"
)


class ReopenInProcessTest(TempDbMixin, unittest.TestCase):
    def test_full_state_survives_reopen(self):
        with MqttIngress(self.db_path) as ing:
            ing.ingest(connect("s1"))
            ing.ingest(retained_publish("s1", "t/a", "v1", message_id=1))
            ing.ingest(retained_publish("s1", "t/a", "v2", message_id=2))
            ing.ingest(publish("s1", "t/a", "live", qos=1, message_id=3))
            sub = ing.ingest(subscribe("s1", ["t/#"], message_id=9))
            sub_id = sub.subscription_id
            first = ing.poll(sub_id)
            self.assertEqual([m.payload for m in first.messages], [b"v2"])

        # 重新打开：全部状态来自磁盘文件
        with MqttIngress(self.db_path) as ing2:
            retained = ing2.list_retained()
            self.assertEqual(len(retained), 1)
            self.assertEqual(retained[0]["payload"], b"v2")
            self.assertEqual(retained[0]["version"], 2)
            self.assertEqual(len(ing2.list_events()), 3)

            # 保留视图已标记投递，重启后不重复投；实时游标停在订阅截断点
            poll = ing2.poll(sub_id)
            self.assertTrue(poll.retained_exhausted)
            self.assertEqual(poll.messages, [])

            ing2.ingest(publish("s1", "t/a", "after-restart", qos=1, message_id=4))
            poll = ing2.poll(sub_id)
            self.assertEqual([m.payload for m in poll.messages], [b"after-restart"])

    def test_clear_survives_reopen(self):
        with MqttIngress(self.db_path) as ing:
            ing.ingest(connect("s1"))
            ing.ingest(retained_publish("s1", "t/a", "v1", message_id=1))
            ing.ingest(retained_clear("s1", "t/a", message_id=2))
        with MqttIngress(self.db_path) as ing2:
            self.assertEqual(ing2.list_retained(), [])
            self.assertEqual(len(ing2.list_events()), 2)
            sub = ing2.ingest(subscribe("s1", ["t/#"]))
            self.assertEqual(ing2.poll(sub.subscription_id).messages, [])


class SubprocessRestartTest(TempDbMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        # 运行器脚本放在独立临时目录，与数据库/输入目录分离
        self._runner_dir = tempfile.mkdtemp(prefix="mqtt_runner_")
        self.runner_path = os.path.join(self._runner_dir, "cli_runner.py")
        with open(self.runner_path, "w", encoding="utf-8") as fh:
            fh.write(_RUNNER_SOURCE)

    def tearDown(self):
        for name in os.listdir(self._runner_dir):
            os.unlink(os.path.join(self._runner_dir, name))
        os.rmdir(self._runner_dir)
        super().tearDown()

    def _run_cli(self, *args):
        env = dict(os.environ)
        env.pop("PYTHONPATH", None)
        # -I 隔离模式：解释器不加载 cwd/环境变量中的任何隐式模块
        proc = subprocess.run(
            [sys.executable, "-I", self.runner_path, *args],
            cwd=self._runner_dir,
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return json.loads(proc.stdout)

    def test_seed_dump_poll_across_processes(self):
        # 进程 1：播种并建立订阅
        seed = self._run_cli("seed", self.db_path, "--sub", "sensors/#")
        self.assertEqual(seed["retained"], {"sensors/temp": "v2", "sensors/hum": "h1"})
        self.assertEqual(seed["quarantine_count"], 0)
        sub_id = seed["subscription_id"]
        self.assertIsNotNone(sub_id)
        ledger_count = seed["ledger_count"]

        # 进程 2：只读检查，状态完整保留（含被清除主题、重复实时消息）
        dump = self._run_cli("dump", self.db_path)
        self.assertEqual(len(dump["events"]), ledger_count)
        self.assertEqual({r["topic"] for r in dump["retained"]},
                         {"sensors/temp", "sensors/hum"})
        temp = next(r for r in dump["retained"] if r["topic"] == "sensors/temp")
        self.assertEqual(temp["payload"], "v2")
        self.assertEqual(temp["version"], 2)
        # 空载荷清除后 obsolete 不存在
        self.assertNotIn("sensors/obsolete",
                         {r["topic"] for r in dump["retained"]})
        # 重复 message_id=4 的两条实时消息都在账本里
        dupes = [e for e in dump["events"] if e["message_id"] == 4]
        self.assertEqual(len(dupes), 2)

        # 进程 3：按订阅投递，初始保留视图跨进程仍可拿到且不重复
        poll1 = self._run_cli("poll", self.db_path, str(sub_id))
        self.assertFalse(poll1["retained_exhausted"])
        self.assertEqual(sorted(m["topic"] for m in poll1["messages"]),
                         ["sensors/hum", "sensors/temp"])
        poll2 = self._run_cli("poll", self.db_path, str(sub_id))
        self.assertTrue(poll2["retained_exhausted"])
        self.assertEqual(poll2["messages"], [])

    def test_quarantined_frames_persist_across_processes(self):
        with MqttIngress(self.db_path) as ing:
            ing.ingest({"type": "PUBLISH", "session_id": "", "topic": "x"})
            ing.ingest(publish("s", "a/#", "x"))
        dump = self._run_cli("dump", self.db_path)
        self.assertEqual(len(dump["quarantine"]), 2)
        reasons = {q["reason"] for q in dump["quarantine"]}
        self.assertIn("WILDCARD_IN_PUBLISH", reasons)


if __name__ == "__main__":
    unittest.main()
