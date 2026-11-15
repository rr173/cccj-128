"""订阅建立与发布并发：初始保留视图不漏、不与实时事件重复。

覆盖两种并发形态：
1. 同一接入器对象的多线程（进程内 RLock + SQLite 写事务）；
2. 多个接入器实例共享同一数据库文件（SQLite 文件锁 + BEGIN IMMEDIATE）。
"""

import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

from offline_mqtt.generator import (
    connect,
    publish,
    retained_publish,
    subscribe,
)
from offline_mqtt.ingress import MqttIngress
from offline_mqtt.topic import topic_matches
from tests._helpers import TempDbMixin

N_PUBLISHERS = 6
N_PER_PUBLISHER = 60
N_SUBSCRIBERS = 4


def _publish_worker(ingress, session, index):
    # 交替保留/普通发布，主题在共享集合上不断更新保留版本
    for i in range(N_PER_PUBLISHER):
        topic = f"root/dev{index % 3}/data"
        mid = index * 1000 + i + 1
        if i % 3 == 0:
            ingress.ingest(retained_publish(session, topic,
                                            f"r-{index}-{i}", qos=1,
                                            message_id=mid))
        else:
            ingress.ingest(publish(session, topic, f"n-{index}-{i}",
                                   qos=1, message_id=mid))


def _subscribe_worker(ingress, session, index):
    res = ingress.ingest(subscribe(session, ["root/#"], message_id=5000 + index))
    return res.subscription_id


class ConcurrencyTest(TempDbMixin, unittest.TestCase):
    def _assert_subscription_invariant(self, ing, sub_id, cutoff):
        # drain 一次性取全：初始保留视图 + 全部实时事件，不丢弃任何投递
        all_msgs = ing.drain(sub_id)
        retained_msgs = [m for m in all_msgs if m.retain]
        live_msgs = [m for m in all_msgs if not m.retain]

        # 1) 保留视图中每个主题最多一条（同一主题只有一个当前版本）
        retained_topics = [m.topic for m in retained_msgs]
        self.assertEqual(len(retained_topics), len(set(retained_topics)))

        # 2) 保留视图固化的来源事件序号都 <= cutoff
        for m in retained_msgs:
            self.assertLessEqual(m.event_seq, cutoff)

        # 3) 实时事件序号全部 > cutoff，严格递增，且不和保留视图的来源事件重合
        live_seqs = [m.event_seq for m in live_msgs]
        self.assertEqual(live_seqs, sorted(live_seqs))
        self.assertTrue(all(s > cutoff for s in live_seqs))
        overlap = set(live_seqs) & {m.event_seq for m in retained_msgs}
        self.assertEqual(overlap, set())

        # 4) 账本中每个 seq 对该订阅最多投递一次（保留视图与实时流合计）
        all_seqs = [m.event_seq for m in retained_msgs] + live_seqs
        self.assertEqual(len(all_seqs), len(set(all_seqs)))

        # 5) 不漏：cutoff 之后每个匹配主题的事件都必须出现在实时流中
        filters = ing.get_subscription(sub_id)["filters"]
        expected_after = {
            e["seq"]
            for e in ing.list_events()
            if e["seq"] > cutoff
            and any(topic_matches(f, e["topic"]) for f in filters)
        }
        self.assertEqual(set(live_seqs), expected_after)

    def test_concurrent_publish_and_subscribe_single_ingress(self):
        with MqttIngress(self.db_path) as ing:
            ing.ingest(connect("hub"))
            barrier = threading.Barrier(N_PUBLISHERS + N_SUBSCRIBERS)

            def pub(idx):
                barrier.wait()
                _publish_worker(ing, "hub", idx)

            def sub(idx):
                barrier.wait()
                sub_id = _subscribe_worker(ing, "hub", idx)
                cutoff = ing.get_subscription(sub_id)["created_cutoff_seq"]
                return sub_id, cutoff

            with ThreadPoolExecutor(max_workers=N_PUBLISHERS + N_SUBSCRIBERS) as ex:
                futs_sub = [ex.submit(sub, i) for i in range(N_SUBSCRIBERS)]
                futs_pub = [ex.submit(pub, i) for i in range(N_PUBLISHERS)]
                sub_infos = [f.result() for f in futs_sub]
                for f in futs_pub:
                    f.result()

            # 账本总量确定且序号连续
            events = ing.list_events()
            self.assertEqual(len(events), N_PUBLISHERS * N_PER_PUBLISHER)
            self.assertEqual([e["seq"] for e in events],
                             list(range(1, len(events) + 1)))

            for sub_id, cutoff in sub_infos:
                self._assert_subscription_invariant(ing, sub_id, cutoff)

            # 每个主题的保留表恰好一行，即当前版本
            retained = ing.list_retained()
            self.assertEqual({r["topic"] for r in retained},
                             {f"root/dev{i}/data" for i in range(3)})

    def test_concurrent_publish_and_subscribe_multiple_ingresses(self):
        # 多个接入器实例 = 多个 SQLite 连接，互斥完全依赖文件锁/IMMEDIATE
        def open_ing():
            return MqttIngress(self.db_path, busy_timeout_ms=30000)

        bootstrap = open_ing()
        bootstrap.ingest(connect("hub"))
        bootstrap.close()

        barrier = threading.Barrier(N_PUBLISHERS + N_SUBSCRIBERS)
        errors = []

        def pub(idx):
            ing = open_ing()
            try:
                barrier.wait()
                _publish_worker(ing, "hub", idx)
            except Exception as exc:  # pragma: no cover - 失败路径
                errors.append(exc)
            finally:
                ing.close()

        def sub(idx, out):
            ing = open_ing()
            try:
                barrier.wait()
                sub_id = _subscribe_worker(ing, "hub", idx)
                cutoff = ing.get_subscription(sub_id)["created_cutoff_seq"]
                out.append((sub_id, cutoff))
            except Exception as exc:  # pragma: no cover
                errors.append(exc)
            finally:
                ing.close()

        sub_infos: list = []
        with ThreadPoolExecutor(max_workers=N_PUBLISHERS + N_SUBSCRIBERS) as ex:
            futs_pub = [ex.submit(pub, i) for i in range(N_PUBLISHERS)]
            futs_sub = [ex.submit(sub, i, sub_infos) for i in range(N_SUBSCRIBERS)]
            for f in futs_pub + futs_sub:
                f.result()

        self.assertEqual(errors, [])
        checker = open_ing()
        try:
            events = checker.list_events()
            self.assertEqual(len(events), N_PUBLISHERS * N_PER_PUBLISHER)
            for sub_id, cutoff in sub_infos:
                self._assert_subscription_invariant(checker, sub_id, cutoff)
        finally:
            checker.close()

    def test_no_loss_when_subscribe_commits_between_retained_updates(self):
        # 确定性交错：保留 v1 提交 -> 订阅 -> 保留 v2 提交。
        # v1 必须出现在初始保留视图，v2 必须出现在实时流，二者不重不漏。
        with MqttIngress(self.db_path) as ing:
            ing.ingest(connect("hub"))
            ing.ingest(retained_publish("hub", "t/a", "v1", qos=1, message_id=1))
            sub = ing.ingest(subscribe("hub", ["t/#"]))
            cutoff = ing.get_subscription(sub.subscription_id)["created_cutoff_seq"]
            ing.ingest(retained_publish("hub", "t/a", "v2", qos=1, message_id=2))

            first = ing.poll(sub.subscription_id)
            self.assertEqual([(m.topic, m.payload) for m in first.messages],
                             [("t/a", b"v1")])
            live = ing.drain(sub.subscription_id)
            self.assertEqual([(m.topic, m.payload) for m in live],
                             [("t/a", b"v2")])
            self.assertLessEqual(first.messages[0].event_seq, cutoff)
            self.assertGreater(live[0].event_seq, cutoff)


if __name__ == "__main__":
    unittest.main()
