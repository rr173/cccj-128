"""采集命名空间严格校验：层字符集、空层、系统/共享前缀、字节上限。"""

import unittest

from offline_mqtt.namespace import (
    MAX_TOPIC_BYTES,
    is_namespace_topic,
    validate_namespace_filter,
    validate_namespace_topic,
)
from offline_mqtt.topic import TopicError


class NamespaceTopicTest(unittest.TestCase):
    def test_legal_topics(self):
        for topic in ("plant", "plant/temp-1", "a/b/c_2/x-9", "_", "1", "a-b_c"):
            with self.subTest(topic=topic):
                self.assertEqual(validate_namespace_topic(topic), topic)
                self.assertTrue(is_namespace_topic(topic))

    def test_rejects_uppercase_and_other_chars(self):
        for bad in ("Plant", "plant/Temp", "plant.temp", "plant temp", "plaünt",
                    "plant/+", "plant/#", "a@b"):
            with self.subTest(bad=bad):
                with self.assertRaises(TopicError) as ctx:
                    validate_namespace_topic(bad)
                self.assertIn(ctx.exception.reason,
                              ("INVALID_LEVEL_CHAR", "WILDCARD_IN_PUBLISH"))
                self.assertFalse(is_namespace_topic(bad))

    def test_rejects_empty_levels(self):
        for bad in ("a//b", "/a", "a/", "a//", "//"):
            with self.subTest(bad=bad):
                with self.assertRaises(TopicError) as ctx:
                    validate_namespace_topic(bad)
                self.assertEqual(ctx.exception.reason, "EMPTY_LEVEL")

    def test_rejects_system_and_shared_prefix(self):
        for bad in ("$SYS/ctrl", "$share/group/plant", "$local/x"):
            with self.subTest(bad=bad):
                with self.assertRaises(TopicError) as ctx:
                    validate_namespace_topic(bad)
                self.assertEqual(ctx.exception.reason, "SYS_OR_SHARED_PREFIX")

    def test_rejects_empty_and_non_string(self):
        with self.assertRaises(TopicError) as ctx:
            validate_namespace_topic("")
        self.assertEqual(ctx.exception.reason, "EMPTY_TOPIC")
        with self.assertRaises(TopicError):
            validate_namespace_topic(123)
        self.assertFalse(is_namespace_topic(None))

    def test_byte_limit(self):
        validate_namespace_topic("a" * MAX_TOPIC_BYTES)
        with self.assertRaises(TopicError) as ctx:
            validate_namespace_topic("a" * (MAX_TOPIC_BYTES + 1))
        self.assertEqual(ctx.exception.reason, "TOPIC_TOO_LONG")


class NamespaceFilterTest(unittest.TestCase):
    def test_legal_filters(self):
        for flt in ("plant/#", "plant/+/temp", "+", "#", "a/b/+", "a-b_c/1/#"):
            with self.subTest(flt=flt):
                self.assertEqual(validate_namespace_filter(flt), flt)

    def test_hash_only_at_last_level(self):
        for bad in ("a/#/b", "#/b", "a/b#", "a#"):
            with self.subTest(bad=bad):
                with self.assertRaises(TopicError) as ctx:
                    validate_namespace_filter(bad)
                self.assertEqual(ctx.exception.reason, "ILLEGAL_MULTI_WILDCARD")

    def test_plus_must_own_level(self):
        for bad in ("a+/b", "a/+b", "+a"):
            with self.subTest(bad=bad):
                with self.assertRaises(TopicError) as ctx:
                    validate_namespace_filter(bad)
                self.assertEqual(ctx.exception.reason, "ILLEGAL_SINGLE_WILDCARD")

    def test_filter_level_charset_and_empty_level(self):
        with self.assertRaises(TopicError) as ctx:
            validate_namespace_filter("A/#")
        self.assertEqual(ctx.exception.reason, "INVALID_LEVEL_CHAR")
        with self.assertRaises(TopicError) as ctx:
            validate_namespace_filter("a//+")
        self.assertEqual(ctx.exception.reason, "EMPTY_LEVEL")
        with self.assertRaises(TopicError) as ctx:
            validate_namespace_filter("$share/x/#")
        self.assertEqual(ctx.exception.reason, "SYS_OR_SHARED_PREFIX")

    def test_filter_byte_limit(self):
        with self.assertRaises(TopicError) as ctx:
            validate_namespace_filter("a" * (MAX_TOPIC_BYTES + 1))
        self.assertEqual(ctx.exception.reason, "TOPIC_TOO_LONG")


if __name__ == "__main__":
    unittest.main()
