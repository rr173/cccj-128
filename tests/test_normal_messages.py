"""普通实时消息：到达次序、重复 DUP 消息、按订阅过滤、游标推进。"""

import unittest

from offline_mqtt.generator import connect, publish, subscribe
from offline_mqtt.ingress import MqttIngress
from tests._helpers import TempDbMixin


class NormalMessageTest(TempDbMixin, unittest.TestCase):
    def test_arrival_order_preserved_in_ledger(self):
        with MqttIngress(self.db_path) as ing:
            ing.ingest(connect("s1"))
            for i in range(20):
                ing.ingest(publish("s1", f"t/{i % 3}", f"p{i}",
                                   qos=1, message_id=i + 1))
            seqs_topics = [(e["seq"], e["topic"]) for e in ing.list_events()]
            self.assertEqual([t for _, t in seqs_topics],
                             [f"t/{i % 3}" for i in range(20)])
            self.assertEqual([s for s, _ in seqs_topics], list(range(1, 21)))

    def test_duplicate_normal_messages_both_land_in_ledger(self):
        with MqttIngress(self.db_path) as ing:
            ing.ingest(connect("s1"))
            ing.ingest(publish("s1", "t/a", "first", qos=1, message_id=42))
            ing.ingest(publish("s1", "t/a", "first-again", qos=1,
                               message_id=42, dup=True))
            # 同 message_id 的重传不去重：账本两条，身份字段一致
            rows = ing.find_events_by_message_id("s1", 42)
            self.assertEqual(len(rows), 2)
            self.assertEqual([r["seq"] for r in rows], [1, 2])
            self.assertEqual([r["dup"] for r in rows], [0, 1])

            sub = ing.ingest(subscribe("s1", ["t/#"]))
            msgs = ing.drain(sub.subscription_id)
            # 订阅建立之后没有新事件，因此实时流为空（旧消息不补投）
            self.assertEqual(msgs, [])

            ing.ingest(publish("s1", "t/a", "first", qos=1, message_id=42))
            ing.ingest(publish("s1", "t/a", "first-again", qos=1,
                               message_id=42, dup=True))
            msgs = ing.drain(sub.subscription_id)
            self.assertEqual(len(msgs), 2)
            self.assertEqual([m.message_id for m in msgs], [42, 42])
            self.assertEqual([m.dup for m in msgs], [False, True])
            self.assertTrue(all(not m.retain for m in msgs))

    def test_subscription_filter_applied_to_live_events(self):
        with MqttIngress(self.db_path) as ing:
            ing.ingest(connect("s1"))
            sub = ing.ingest(subscribe("s1", ["a/+/c", "x/#"]))
            ing.poll(sub.subscription_id)  # 排空（空）保留视图

            for topic in ("a/b/c", "a/b/d", "x/1/2", "y", "a/b/c/d", "x"):
                ing.ingest(publish("s1", topic, "z", qos=0))

            msgs = ing.drain(sub.subscription_id)
            self.assertEqual([m.topic for m in msgs],
                             ["a/b/c", "x/1/2", "x"])

    def test_cursor_advance_and_limit(self):
        with MqttIngress(self.db_path) as ing:
            ing.ingest(connect("s1"))
            sub = ing.ingest(subscribe("s1", ["t/#"]))
            ing.poll(sub.subscription_id)
            for i in range(5):
                ing.ingest(publish("s1", "t/x", f"p{i}", qos=0))

            first = ing.poll(sub.subscription_id, limit=2)
            self.assertEqual([m.payload for m in first.messages], [b"p0", b"p1"])
            self.assertEqual(first.next_cursor, 2)
            rest = ing.poll(sub.subscription_id)
            self.assertEqual([m.payload for m in rest.messages],
                             [b"p2", b"p3", b"p4"])
            # 再轮询无新消息，游标不变
            idle = ing.poll(sub.subscription_id)
            self.assertEqual(idle.messages, [])
            self.assertEqual(idle.next_cursor, 5)

    def test_retained_and_live_both_delivered_in_order_without_duplication(self):
        with MqttIngress(self.db_path) as ing:
            ing.ingest(connect("s1"))
            ing.ingest(publish("s1", "t/a", "before-sub", qos=1, message_id=1))
            sub = ing.ingest(subscribe("s1", ["t/#"]))
            ing.ingest(publish("s1", "t/a", "after-sub", qos=1, message_id=2))
            msgs = ing.drain(sub.subscription_id)
            # 订阅前的普通消息不补投，订阅后的实时消息恰好一条
            self.assertEqual([m.payload for m in msgs], [b"after-sub"])
            self.assertEqual([m.retain for m in msgs], [False])

    def test_identity_fields_distinguished(self):
        with MqttIngress(self.db_path) as ing:
            ing.ingest(connect("sess-A"))
            ing.ingest(connect("sess-B"))
            ing.ingest(publish("sess-A", "t", "a", qos=1, message_id=7))
            ing.ingest(publish("sess-B", "t", "b", qos=1, message_id=7))
            # 同 message_id 但会话身份不同，是两条相互独立的事件
            self.assertEqual(len(ing.find_events_by_message_id("sess-A", 7)), 1)
            self.assertEqual(len(ing.find_events_by_message_id("sess-B", 7)), 1)
            events = ing.list_events()
            self.assertEqual({e["session_id"] for e in events},
                             {"sess-A", "sess-B"})


if __name__ == "__main__":
    unittest.main()
