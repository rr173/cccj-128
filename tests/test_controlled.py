"""受控订阅/读取入口：准入保护真实订阅创建、命名空间过滤、失败不落库。"""

import json
import unittest

from offline_mqtt.admission import AdmissionEngine, AdmissionInputError
from offline_mqtt.controlled import ControlledIngress
from offline_mqtt.generator import (
    connect,
    publish,
    retained_publish,
    subscribe,
)
from offline_mqtt.ingress import MqttIngress
from offline_mqtt.policy import parse_policy
from tests._helpers import TempDbMixin


def make_engine(rules, principals=None):
    principals = principals if principals is not None else [
        {"principal_id": "team-plant", "groups": ["plant-team"]},
        {"principal_id": "team-payroll", "groups": []},
    ]
    return AdmissionEngine(parse_policy(
        json.dumps({"version": 1, "principals": principals, "rules": rules})
    ))


def table_counts(ing: MqttIngress) -> dict:
    cur = ing.store.conn
    return {
        name: cur.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        for name in ("subscriptions", "subscription_filters",
                     "subscription_retained", "delivery_cursor")
    }


class ControlledSubscribeTest(TempDbMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.engine = make_engine([
            {"rule_id": "a1", "subject_type": "principal", "subject_id": "team-plant",
             "effect": "allow", "filter": "plant/#"},
            {"rule_id": "d1", "subject_type": "group", "subject_id": "plant-team",
             "effect": "deny", "filter": "plant/private/#"},
            {"rule_id": "pay", "subject_type": "principal", "subject_id": "team-payroll",
             "effect": "allow", "filter": "payroll/#"},
        ])

    def test_granted_creates_real_subscription_with_retained_view(self):
        with MqttIngress(self.db_path) as ing:
            ing.ingest(connect("sensor-1"))
            ing.ingest(retained_publish("sensor-1", "plant/temp", "20.1", message_id=1))
            controlled = ControlledIngress(ing, self.engine)

            before = table_counts(ing)
            result = controlled.controlled_subscribe(
                "team-plant", subscribe("any-session", ["plant/temp"], message_id=9))
            after = table_counts(ing)

            self.assertTrue(result.granted)
            self.assertIsNotNone(result.subscription_id)
            # 真实订阅：四张表都产生了行
            self.assertEqual(after["subscriptions"], before["subscriptions"] + 1)
            self.assertEqual(after["delivery_cursor"], before["delivery_cursor"] + 1)
            self.assertEqual(after["subscription_retained"],
                             before["subscription_retained"] + 1)
            # 准入依据随结果返回
            self.assertTrue(all(d.safe for d in result.filter_decisions))
            self.assertEqual(result.policy_summary["policy_digest_sha256"],
                             self.engine.policy_summary()["policy_digest_sha256"])

            # 初始保留视图可读，语义与旧入口一致
            poll = controlled.controlled_poll(result.subscription_id)
            self.assertEqual([m.topic for m in poll.messages], ["plant/temp"])
            self.assertTrue(all(m.retain for m in poll.messages))

    def test_denied_filter_creates_nothing(self):
        with MqttIngress(self.db_path) as ing:
            controlled = ControlledIngress(ing, self.engine)
            before = table_counts(ing)
            # plant/private/# 命中 deny 子树
            result = controlled.controlled_subscribe(
                "team-plant", subscribe("s", ["plant/private/#"]))
            self.assertFalse(result.granted)
            self.assertIsNone(result.subscription_id)
            self.assertEqual(table_counts(ing), before)  # 无任何订阅相关行
            # 逐过滤器可读原因 + 可复核 witness
            decision = result.filter_decisions[0]
            self.assertFalse(decision.safe)
            witness = decision.detail["witness"]
            self.assertTrue(witness["witness_topic"].startswith("plant/private"))
            self.assertIn("d1", witness["deny_rule_ids"])

    def test_mixed_granted_and_denied_request_is_all_or_nothing(self):
        with MqttIngress(self.db_path) as ing:
            controlled = ControlledIngress(ing, self.engine)
            before = table_counts(ing)
            result = controlled.controlled_subscribe(
                "team-plant",
                subscribe("s", ["plant/temp", "plant/private/x"]))
            self.assertFalse(result.granted)
            self.assertIsNone(result.subscription_id)
            self.assertEqual(table_counts(ing), before)
            self.assertEqual(len(result.filter_decisions), 2)
            safeties = {d.request_filter: d.safe for d in result.filter_decisions}
            self.assertEqual(safeties, {"plant/temp": True, "plant/private/x": False})

    def test_duplicate_filter_is_input_error_and_creates_nothing(self):
        with MqttIngress(self.db_path) as ing:
            controlled = ControlledIngress(ing, self.engine)
            before = table_counts(ing)
            result = controlled.controlled_subscribe(
                "team-plant", subscribe("s", ["plant/#", "plant/#"]))
            self.assertFalse(result.granted)
            self.assertTrue(result.errors)
            self.assertEqual(result.errors[0]["reason"], "DUPLICATE_FILTER")
            self.assertEqual(table_counts(ing), before)

    def test_unknown_principal_and_malformed_record_are_input_errors(self):
        with MqttIngress(self.db_path) as ing:
            controlled = ControlledIngress(ing, self.engine)
            before = table_counts(ing)
            result = controlled.controlled_subscribe(
                "ghost", subscribe("s", ["plant/#"]))
            self.assertFalse(result.granted)
            self.assertTrue(any(e["reason"] == "UNKNOWN_PRINCIPAL" for e in result.errors))
            self.assertEqual(table_counts(ing), before)

            result2 = controlled.controlled_subscribe(
                "team-plant", {"type": "CONNECT", "client_id": "x"})
            self.assertFalse(result2.granted)
            self.assertEqual(result2.errors[0]["reason"], "WRONG_FRAME_TYPE")
            self.assertEqual(table_counts(ing), before)

            # 结构非法记录 -> 帧解析错误，同样不抛异常、不落库
            result3 = controlled.controlled_subscribe(
                "team-plant", {"type": "SUBSCRIBE", "session_id": "s", "filters": []})
            self.assertFalse(result3.granted)
            self.assertEqual(result3.errors[0]["reason"], "MISSING_OR_INVALID_FIELD")
            self.assertEqual(table_counts(ing), before)

    def test_principal_not_inferred_from_session_id(self):
        with MqttIngress(self.db_path) as ing:
            controlled = ControlledIngress(ing, self.engine)
            # 记录里 session_id 写成 payroll，也不影响显式传入的 plant 身份
            # （plant/temp 不触及 deny 子树 plant/private/#）
            result = controlled.controlled_subscribe(
                "team-plant", subscribe("team-payroll-session", ["plant/temp"]))
            self.assertTrue(result.granted)
            self.assertEqual(result.principal_id, "team-plant")
            # 反过来：显式 payroll 身份即便记录 session_id 写成 plant，也只能访问 payroll
            result2 = controlled.controlled_subscribe(
                "team-payroll", subscribe("team-plant-session", ["payroll/#"]))
            self.assertTrue(result2.granted)
            # payroll 身份请求 plant 范围必须被拒，与记录里的 session_id 无关
            result3 = controlled.controlled_subscribe(
                "team-payroll", subscribe("team-plant-session", ["plant/temp"]))
            self.assertFalse(result3.granted)
            self.assertEqual(result3.filter_decisions[0].detail["witness"]["reason"],
                             "NO_ALLOW_RULE_MATCHED")


class ControlledReadNamespaceTest(TempDbMixin, unittest.TestCase):
    def test_non_namespace_topics_never_delivered(self):
        engine = make_engine([
            {"rule_id": "a", "subject_type": "principal", "subject_id": "team-plant",
             "effect": "allow", "filter": "#"},
        ])
        with MqttIngress(self.db_path) as ing:
            ing.ingest(connect("s"))
            # 命名空间内 + 命名空间外的保留值（底层 # 都会匹配）
            ing.ingest(retained_publish("s", "plant/temp", "ok", message_id=1))
            ing.ingest(retained_publish("s", "Plant/Temp", "uppercase", message_id=2))
            ing.ingest(retained_publish("s", "$SYS/ctrl", "sys", message_id=3))
            ing.ingest(retained_publish("s", "a//b", "empty-level", message_id=4))

            controlled = ControlledIngress(ing, engine)
            result = controlled.controlled_subscribe(
                "team-plant", subscribe("s", ["#"]))
            self.assertTrue(result.granted)
            messages = controlled.controlled_drain(result.subscription_id)
            topics = {m.topic for m in messages}
            self.assertIn("plant/temp", topics)
            # 命名空间外主题即便底层 # 匹配也不得混入
            self.assertNotIn("Plant/Temp", topics)
            self.assertNotIn("$SYS/ctrl", topics)
            self.assertNotIn("a//b", topics)

    def test_live_namespace_filtering_after_retained_phase(self):
        engine = make_engine([
            {"rule_id": "a", "subject_type": "principal", "subject_id": "team-plant",
             "effect": "allow", "filter": "#"},
        ])
        with MqttIngress(self.db_path) as ing:
            ing.ingest(connect("s"))
            controlled = ControlledIngress(ing, engine)
            sub = controlled.controlled_subscribe(
                "team-plant", subscribe("s", ["#"]))
            # 先耗尽（空的）保留视图阶段
            controlled.controlled_poll(sub.subscription_id)
            # 再发布命名空间内外混合的实时消息
            ing.ingest(publish("s", "plant/live", "1", qos=1, message_id=10))
            ing.ingest(publish("s", "Outside/Topic", "2", qos=1, message_id=11))
            ing.ingest(publish("s", "$share/x", "3", qos=1, message_id=12))
            messages = controlled.controlled_drain(sub.subscription_id)
            self.assertEqual([m.topic for m in messages], ["plant/live"])
            self.assertTrue(all(not m.retain for m in messages))

    def test_cursor_advances_without_losing_or_duplicating(self):
        engine = make_engine([
            {"rule_id": "a", "subject_type": "principal", "subject_id": "team-plant",
             "effect": "allow", "filter": "plant/#"},
        ])
        with MqttIngress(self.db_path) as ing:
            ing.ingest(connect("s"))
            controlled = ControlledIngress(ing, engine)
            sub = controlled.controlled_subscribe(
                "team-plant", subscribe("s", ["plant/#"]))
            controlled.controlled_poll(sub.subscription_id)  # 耗尽保留视图
            ing.ingest(publish("s", "plant/a", "x", qos=1, message_id=1))
            first = controlled.controlled_poll(sub.subscription_id)
            self.assertEqual([m.topic for m in first.messages], ["plant/a"])
            # 游标已推进；再次读取不重复
            second = controlled.controlled_poll(sub.subscription_id)
            self.assertEqual(second.messages, [])
            self.assertEqual(second.next_cursor, first.next_cursor)


class OldEntryPointUnchangedTest(TempDbMixin, unittest.TestCase):
    def test_legacy_ingest_still_accepts_full_mqtt_syntax(self):
        # 旧入口支持范围不缩小：大写主题、$ 前缀、空层仍可正常接入（不走准入）。
        with MqttIngress(self.db_path) as ing:
            ing.ingest(connect("s"))
            for topic in ("Plant/Temp", "$SYS/ctrl", "a//b"):
                res = ing.ingest(retained_publish("s", topic, "v", message_id=1))
                self.assertTrue(res.accepted, topic)
            sub = ing.ingest(subscribe("s", ["#"]))
            self.assertTrue(sub.accepted)
            got = {m.topic for m in ing.drain(sub.subscription_id)}
            self.assertEqual(got, {"Plant/Temp", "$SYS/ctrl", "a//b"})


if __name__ == "__main__":
    unittest.main()
