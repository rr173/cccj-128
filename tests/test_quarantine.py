"""非法帧隔离：结构错误、非法通配位置、超长主题、QoS/message_id 越界等。"""

import unittest

from offline_mqtt.generator import connect, publish, subscribe
from offline_mqtt.ingress import MqttIngress
from offline_mqtt.topic import MAX_TOPIC_LENGTH
from tests._helpers import TempDbMixin


class QuarantineTest(TempDbMixin, unittest.TestCase):
    def test_structurally_invalid_frames_quarantined(self):
        with MqttIngress(self.db_path) as ing:
            bad_frames = [
                "not-a-dict",
                {},
                {"type": "WHATEVER"},
                {"type": "CONNECT"},  # 缺 client_id
                {"type": "CONNECT", "client_id": ""},
                {"type": "PUBLISH", "session_id": "s", "topic": "a", "qos": 5},
                {"type": "PUBLISH", "session_id": "s", "topic": "a", "qos": 1},
                {"type": "PUBLISH", "session_id": "s", "topic": 123},
                {"type": "PUBLISH", "session_id": "s", "topic": "a", "payload": object()},
                {"type": "SUBSCRIBE", "session_id": "s", "filters": []},
                {"type": "SUBSCRIBE", "session_id": "s", "filters": "ok"},
                {"type": "SUBSCRIBE", "session_id": "s", "filters": [1, 2]},
                {"type": "DISCONNECT"},
                {"type": "PUBLISH", "session_id": "s", "topic": "a",
                 "message_id": 70000},
            ]
            for frame in bad_frames:
                result = ing.ingest(frame)
                self.assertFalse(result.accepted, frame)
                self.assertIsNotNone(result.quarantine_id)

            self.assertEqual(ing.list_events(), [])
            self.assertEqual(ing.list_retained(), [])
            self.assertEqual(ing.store.count_quarantine(), len(bad_frames))

    def test_wildcard_in_publish_topic_quarantined(self):
        with MqttIngress(self.db_path) as ing:
            for topic in ("a/+/c", "a/#", "+"):
                result = ing.ingest(publish("s", topic, "x"))
                self.assertFalse(result.accepted)
                self.assertEqual(result.reason, "WILDCARD_IN_PUBLISH")
            self.assertEqual(ing.list_events(), [])

    def test_illegal_filter_quarantined_and_no_subscription_created(self):
        with MqttIngress(self.db_path) as ing:
            ing.ingest(connect("s"))
            result = ing.ingest(subscribe("s", ["a#", "b/+"]))
            self.assertFalse(result.accepted)
            self.assertEqual(result.reason, "ILLEGAL_MULTI_WILDCARD")
            self.assertIsNone(result.subscription_id)
            # 隔离记录保留了原始帧
            q = ing.list_quarantine()[-1]
            self.assertEqual(q["frame_type"], "SUBSCRIBE")
            self.assertIn("a#", q["raw_json"])

    def test_overlong_topic_quarantined(self):
        with MqttIngress(self.db_path, max_topic_length=32) as ing:
            ing.ingest(publish("s", "x" * 33, "y"))
            ing.ingest(subscribe("s", ["y" * 33]))
            reasons = {q["reason"] for q in ing.list_quarantine()}
            self.assertEqual(reasons, {"TOPIC_TOO_LONG"})
            self.assertEqual(ing.list_events(), [])

            # 恰好达到上限的主题是合法的
            ok = ing.ingest(publish("s", "x" * 32, "y"))
            self.assertTrue(ok.accepted)

    def test_default_spec_limit_enforced(self):
        with MqttIngress(self.db_path) as ing:
            result = ing.ingest(publish("s", "z" * (MAX_TOPIC_LENGTH + 1), "y"))
            self.assertFalse(result.accepted)
            self.assertEqual(result.reason, "TOPIC_TOO_LONG")

    def test_quarantine_does_not_poison_valid_processing(self):
        with MqttIngress(self.db_path) as ing:
            ing.ingest(connect("s"))
            ing.ingest(publish("s", "bad/+", "x"))           # 隔离
            good = ing.ingest(publish("s", "good/a", "y",
                                      qos=1, message_id=1))  # 正常
            ing.ingest({"type": "NOPE"})                     # 隔离
            self.assertTrue(good.accepted)
            self.assertEqual(len(ing.list_events()), 1)
            self.assertEqual(ing.store.count_quarantine(), 2)

    def test_qos0_message_id_zero_rejected(self):
        with MqttIngress(self.db_path) as ing:
            result = ing.ingest(publish("s", "a", "x", qos=0, message_id=0))
            self.assertFalse(result.accepted)
            self.assertEqual(result.reason, "MESSAGE_ID_OUT_OF_RANGE")


if __name__ == "__main__":
    unittest.main()
