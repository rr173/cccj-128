"""主题过滤器边界：单层/多层通配、非法通配位置、长度上限。"""

import unittest

from offline_mqtt.topic import (
    MAX_TOPIC_LENGTH,
    TopicError,
    topic_matches,
    validate_filter,
    validate_publish_topic,
)


class TopicMatchingTest(unittest.TestCase):
    def test_exact_match(self):
        self.assertTrue(topic_matches("a/b/c", "a/b/c"))
        self.assertFalse(topic_matches("a/b/c", "a/b/d"))
        self.assertFalse(topic_matches("a/b", "a/b/c"))

    def test_single_level_wildcard(self):
        self.assertTrue(topic_matches("a/+/c", "a/b/c"))
        self.assertTrue(topic_matches("a/+/c", "a/x/c"))
        self.assertFalse(topic_matches("a/+/c", "a/b/d"))
        # + 恰好匹配一层，不多不少
        self.assertFalse(topic_matches("a/+", "a/b/c"))
        self.assertFalse(topic_matches("a/+/c", "a/c"))
        self.assertTrue(topic_matches("+", "anything"))
        self.assertFalse(topic_matches("+", "a/b"))

    def test_multi_level_wildcard(self):
        self.assertTrue(topic_matches("a/#", "a/b/c"))
        self.assertTrue(topic_matches("a/#", "a/b"))
        # # 可匹配零层：a/# 匹配 a 本身
        self.assertTrue(topic_matches("a/#", "a"))
        self.assertFalse(topic_matches("a/#", "b"))
        self.assertTrue(topic_matches("#", "anything/at/all"))
        self.assertTrue(topic_matches("#", "single"))

    def test_leading_slash_levels(self):
        self.assertTrue(topic_matches("/a/+", "/a/b"))
        self.assertFalse(topic_matches("/a/+", "a/b"))
        self.assertTrue(topic_matches("/#", "/a/b"))
        self.assertFalse(topic_matches("/#", "a/b"))

    def test_mixed_wildcards(self):
        self.assertTrue(topic_matches("sport/+/player/#", "sport/tennis/player/a/b"))
        self.assertTrue(topic_matches("sport/+/player/#", "sport/tennis/player"))
        self.assertFalse(topic_matches("sport/+/player/#", "sport/tennis/umpire"))

    def test_empty_levels_are_significant(self):
        self.assertTrue(topic_matches("a//b", "a//b"))
        self.assertTrue(topic_matches("a/+/b", "a//b"))
        self.assertFalse(topic_matches("a/b", "a//b"))


class TopicValidationTest(unittest.TestCase):
    def test_publish_topic_rejects_wildcards(self):
        with self.assertRaises(TopicError) as ctx:
            validate_publish_topic("a/+/c")
        self.assertEqual(ctx.exception.reason, "WILDCARD_IN_PUBLISH")
        with self.assertRaises(TopicError) as ctx:
            validate_publish_topic("a/#")
        self.assertEqual(ctx.exception.reason, "WILDCARD_IN_PUBLISH")

    def test_publish_topic_empty_and_nul(self):
        with self.assertRaises(TopicError) as ctx:
            validate_publish_topic("")
        self.assertEqual(ctx.exception.reason, "EMPTY_TOPIC")
        with self.assertRaises(TopicError) as ctx:
            validate_publish_topic("a\x00b")
        self.assertEqual(ctx.exception.reason, "NUL_IN_TOPIC")
        with self.assertRaises(TopicError):
            validate_publish_topic(123)

    def test_illegal_wildcard_positions(self):
        for bad in ("a#", "a/#/b", "#/b", "a+b", "+a", "a/+b", "a/b#"):
            with self.subTest(bad=bad):
                with self.assertRaises(TopicError) as ctx:
                    validate_filter(bad)
                self.assertIn(ctx.exception.reason,
                              ("ILLEGAL_MULTI_WILDCARD", "ILLEGAL_SINGLE_WILDCARD"))

    def test_legal_filters_accepted(self):
        for good in ("#", "a/#", "a/+/b", "+", "a/+/+", "/#", "a//+"):
            with self.subTest(good=good):
                self.assertEqual(validate_filter(good), good)

    def test_length_limit(self):
        topic = "t/" + "x" * 100
        with self.assertRaises(TopicError) as ctx:
            validate_publish_topic(topic, max_length=10)
        self.assertEqual(ctx.exception.reason, "TOPIC_TOO_LONG")
        # 字节长度而非字符数
        multibyte = "中" * 10
        with self.assertRaises(TopicError) as ctx:
            validate_publish_topic(multibyte, max_length=20)
        self.assertEqual(ctx.exception.reason, "TOPIC_TOO_LONG")
        # 默认规范上限
        validate_publish_topic("a" * MAX_TOPIC_LENGTH)
        with self.assertRaises(TopicError):
            validate_publish_topic("a" * (MAX_TOPIC_LENGTH + 1))


if __name__ == "__main__":
    unittest.main()
