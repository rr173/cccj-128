"""保留值：替换版本唯一、空载荷清除、新订阅初始保留视图。"""

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


class RetainedTest(TempDbMixin, unittest.TestCase):
    def _ing(self, **kw):
        return MqttIngress(self.db_path, **kw)

    def test_retained_replace_keeps_single_current_version(self):
        with self._ing() as ing:
            ing.ingest(connect("s1"))
            ing.ingest(retained_publish("s1", "t/a", "v1", qos=1, message_id=1))
            ing.ingest(retained_publish("s1", "t/a", "v2", qos=1, message_id=2))
            ing.ingest(retained_publish("s1", "t/a", "v3", qos=1, message_id=3))

            rows = ing.list_retained()
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row["topic"], "t/a")
            self.assertEqual(row["payload"], b"v3")
            self.assertEqual(row["version"], 3)
            # 账本里三次发布全部保留，按到达次序
            events = ing.list_events()
            self.assertEqual([e["message_id"] for e in events], [1, 2, 3])

    def test_empty_payload_retained_clears_topic(self):
        with self._ing() as ing:
            ing.ingest(connect("s1"))
            ing.ingest(retained_publish("s1", "t/a", "v1", message_id=1))
            ing.ingest(retained_clear("s1", "t/a", message_id=2))

            self.assertIsNone(ing.get_retained("t/a"))
            # 清除事件本身仍是账本中的一条普通到达记录
            topics = [e["topic"] for e in ing.list_events()]
            self.assertEqual(topics, ["t/a", "t/a"])
            # 再发保留值可以重新建立
            ing.ingest(retained_publish("s1", "t/a", "again", message_id=3))
            self.assertEqual(ing.get_retained("t/a")["payload"], b"again")

    def test_clearing_unknown_topic_is_still_ledger_event(self):
        with self._ing() as ing:
            ing.ingest(connect("s1"))
            ing.ingest(retained_clear("s1", "never/seen", message_id=1))
            self.assertEqual(ing.list_retained(), [])
            self.assertEqual(len(ing.list_events()), 1)

    def test_new_subscription_gets_current_retained_view(self):
        with self._ing() as ing:
            ing.ingest(connect("s1"))
            ing.ingest(retained_publish("s1", "home/temp", "20", message_id=1))
            ing.ingest(retained_publish("s1", "home/hum", "55", message_id=2))
            ing.ingest(retained_publish("s1", "away/x", "1", message_id=3))
            sub = ing.ingest(subscribe("s1", ["home/#"], message_id=9))

            result = ing.poll(sub.subscription_id)
            self.assertFalse(result.retained_exhausted)
            topics = sorted(m.topic for m in result.messages)
            self.assertEqual(topics, ["home/hum", "home/temp"])
            self.assertTrue(all(m.retain for m in result.messages))
            # 保留视图只投递一次
            second = ing.poll(sub.subscription_id)
            self.assertEqual(second.messages, [])
            self.assertTrue(second.retained_exhausted)

    def test_subscription_after_clear_does_not_receive_cleared_topic(self):
        with self._ing() as ing:
            ing.ingest(connect("s1"))
            ing.ingest(retained_publish("s1", "t/a", "v1", message_id=1))
            ing.ingest(retained_clear("s1", "t/a", message_id=2))
            sub = ing.ingest(subscribe("s1", ["t/#"], message_id=3))
            result = ing.poll(sub.subscription_id)
            self.assertTrue(result.retained_exhausted)
            self.assertEqual(result.messages, [])

    def test_non_retained_publish_never_touches_retained_table(self):
        with self._ing() as ing:
            ing.ingest(connect("s1"))
            ing.ingest(publish("s1", "t/a", "live", qos=1, message_id=1))
            self.assertEqual(ing.list_retained(), [])
            sub = ing.ingest(subscribe("s1", ["t/#"]))
            # 订阅之前的普通实时消息不进保留视图，也不补投
            self.assertEqual(ing.poll(sub.subscription_id).messages, [])

    def test_filter_specificity_in_retained_view(self):
        with self._ing() as ing:
            ing.ingest(connect("s1"))
            for mid, (t, v) in enumerate((("a/b", "1"), ("a/c", "2"),
                                          ("a/b/d", "3"), ("x", "4")), start=1):
                ing.ingest(retained_publish("s1", t, v, message_id=mid))
            sub1 = ing.ingest(subscribe("s1", ["a/+"]))
            got = {m.topic for m in ing.poll(sub1.subscription_id).messages}
            self.assertEqual(got, {"a/b", "a/c"})

            sub2 = ing.ingest(subscribe("s1", ["a/#"]))
            got2 = {m.topic for m in ing.poll(sub2.subscription_id).messages}
            self.assertEqual(got2, {"a/b", "a/c", "a/b/d"})


if __name__ == "__main__":
    unittest.main()
