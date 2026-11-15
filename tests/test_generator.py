"""本地帧生成器测试：脚本确定性、随机批量、非法帧比例。"""

import unittest

from offline_mqtt.frames import parse_frame, FrameFormatError
from offline_mqtt.generator import (
    LocalFrameGenerator,
    connect,
    publish,
    retained_clear,
    retained_publish,
    subscribe,
)
from offline_mqtt.ingress import MqttIngress


class GeneratorTest(unittest.TestCase):
    def test_scripted_scenario_is_deterministic(self):
        g = LocalFrameGenerator(seed=42)
        g.add(connect("s1"))
        g.add(retained_publish("s1", "t/a", "v", message_id=g.next_message_id()))
        g.add(retained_clear("s1", "t/a", message_id=g.next_message_id()))
        g.add(subscribe("s1", ["t/#"]))
        frames = g.generate()
        self.assertEqual([f["type"] for f in frames],
                         ["CONNECT", "PUBLISH", "PUBLISH", "SUBSCRIBE"])
        # 全部可被解析器接受（生成器产出的标准帧均合法）
        for f in frames:
            parse_frame(f)

    def test_random_burst_reproducible_with_seed(self):
        g1 = LocalFrameGenerator(seed=7)
        g2 = LocalFrameGenerator(seed=7)
        b1 = g1.random_burst(["s"], ["a/b", "a/c", "x"], 25)
        b2 = g2.random_burst(["s"], ["a/b", "a/c", "x"], 25)
        self.assertEqual(b1, b2)

    def test_random_burst_invalid_ratio(self):
        g = LocalFrameGenerator(seed=1)
        burst = g.random_burst(["s"], ["a"], 200, invalid_ratio=0.5)
        with MqttIngress(":memory:") as ing:
            results = ing.ingest_many(burst)
        accepted = sum(1 for r in results if r.accepted)
        rejected = len(results) - accepted
        self.assertEqual(accepted + rejected, 200)
        self.assertGreater(rejected, 0)

    def test_retained_clear_frame_shape(self):
        f = retained_clear("s", "t/a", message_id=3)
        self.assertEqual(f["retain"], True)
        self.assertEqual(f["payload"], "")
        parsed = parse_frame(f)
        self.assertEqual(parsed.payload, b"")

    def test_malformed_frame_rejected_by_parser(self):
        with self.assertRaises(FrameFormatError):
            parse_frame({"type": "PUBLISH"})


if __name__ == "__main__":
    unittest.main()
