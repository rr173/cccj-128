"""wire 层核验与字节级删除：手写 hex 用例 + 官方动态消息复核 + 拒绝证据。"""

import unittest

from offline_mqtt import _protobuf_vendor  # noqa: F401  先行布置 vendored protobuf

from google.protobuf import message_factory

from offline_mqtt.pb_descriptor import (
    build_plan,
    load_descriptor_set,
    normalize_paths,
)
from offline_mqtt.pb_errors import (
    INVALID_LENGTH,
    INVALID_PACKED,
    INVALID_TAG,
    INVALID_UTF8,
    INVALID_VARINT,
    RESOURCE_LIMIT,
    TRUNCATED,
    UNKNOWN_FIELD,
    WRONG_WIRE_TYPE,
    ExportError,
)
from offline_mqtt.pb_wire import Limits, build_schema, rewrite_payload
from tests._pb_fixtures import HAND_CASES, build_demo_descriptor_set


def _prepare(root_name, remove_paths):
    loaded = load_descriptor_set(build_demo_descriptor_set())
    desc = loaded.find_message(root_name)
    paths = normalize_paths(remove_paths, desc)
    plan = build_plan(paths)
    schema = build_schema(desc)
    hits = {p: 0 for p in paths}
    return loaded, desc, schema, plan, hits


class TestHandWrittenCases(unittest.TestCase):
    """逐字节核对：预期输出全部是手工写出的 hex，不由重写函数生成。"""

    def test_hand_cases_byte_exact(self):
        for (name, root, paths, in_hex, out_hex, intervals, hits) in HAND_CASES:
            with self.subTest(case=name):
                _, _, schema, plan, hit_map = _prepare(root, paths)
                payload = bytes.fromhex(in_hex)
                expected = bytes.fromhex(out_hex)
                result = rewrite_payload(schema, payload, plan, hits=hit_map,
                                         event_seq=7)
                self.assertEqual(result.output, expected,
                                 f"输出字节不符:\n got {result.output.hex()}\n exp {expected.hex()}")
                self.assertEqual(result.intervals, intervals)
                self.assertEqual(hit_map, hits)
                self.assertEqual(result.changed, payload != expected)

    def test_unchanged_returns_same_object(self):
        # 未变化的正文：输出与输入是同一对象（零拷贝），原长度前缀逐字节不变
        _, _, schema, plan, hits = _prepare("demo.Outer", [[1, 1]])
        payload = bytes.fromhex("0a8300" "1a0161")
        result = rewrite_payload(schema, payload, plan, hits=hits)
        self.assertIs(result.output, payload)
        self.assertFalse(result.changed)

    def test_deleted_bytes_actually_gone(self):
        # 两段出现的敏感原值都消失（不是只删解码后的逻辑值）
        _, _, schema, plan, hits = _prepare("demo.Outer", [[1, 1]])
        payload = bytes.fromhex("0a07" "0a027331" "1a0161" "0a07" "0a027332" "1a0162")
        result = rewrite_payload(schema, payload, plan, hits=hits)
        self.assertNotIn(b"s1", result.output)
        self.assertNotIn(b"s2", result.output)
        self.assertIn(b"a", result.output)  # label 保留

    def test_semantics_with_official_dynamic_messages(self):
        """用 protobuf 官方动态消息复核输出语义（补充检查）。"""
        loaded = load_descriptor_set(build_demo_descriptor_set())

        def parse(root, data):
            cls = message_factory.GetMessageClass(
                loaded.pool.FindMessageTypeByName(root))
            return cls.FromString(data)

        # A: secret 两次出现都被删；非规范 varint 的 n=1 保留
        _, _, schema, plan, hits = _prepare("demo.M1", [[1]])
        out = rewrite_payload(schema, bytes.fromhex("0a0261610a026262108100"),
                              plan, hits=hits).output
        msg = parse("demo.M1", out)
        self.assertEqual(msg.secret, "")
        self.assertEqual(msg.n, 1)

        # B: 跨边界长度重算后语义不变（除 secret 被删）
        _, _, schema, plan, hits = _prepare("demo.Outer", [[1, 1]])
        in_b = bytes.fromhex("0a8201" "0a027878" + "127c" + "00" * 124 + "12016b")
        out = rewrite_payload(schema, in_b, plan, hits=hits).output
        msg = parse("demo.Outer", out)
        self.assertEqual(msg.keep, "k")
        self.assertEqual(msg.sub.pad, b"\x00" * 124)
        self.assertEqual(msg.sub.secret, "")

        # C: 两段 message 的敏感值都消失；官方合并语义下 label 取后者
        out = rewrite_payload(
            schema, bytes.fromhex("0a07" "0a027331" "1a0161"
                                  "0a07" "0a027332" "1a0162"),
            plan, hits=hits).output
        msg = parse("demo.Outer", out)
        self.assertEqual(msg.sub.label, "b")
        self.assertEqual(msg.sub.secret, "")

        # D: oneof 旧成员被删，现成员保留
        _, _, schema, plan, hits = _prepare("demo.Choice", [[1]])
        out = rewrite_payload(schema, bytes.fromhex("0a027431" "1005"),
                              plan, hits=hits).output
        msg = parse("demo.Choice", out)
        self.assertEqual(msg.WhichOneof("sel"), "code")
        self.assertEqual(msg.code, 5)
        self.assertEqual(msg.token, "")

        # E: repeated message 每个出现的 secret 都删
        _, _, schema, plan, hits = _prepare("demo.RepList", [[1, 2]])
        out = rewrite_payload(
            schema,
            bytes.fromhex("0a06" "0a0161" "120178" "0a06" "0a0162" "120179"),
            plan, hits=hits).output
        msg = parse("demo.RepList", out)
        self.assertEqual([it.tag for it in msg.items], ["a", "b"])
        self.assertEqual([it.secret for it in msg.items], ["", ""])

        # F: map 的 message value 内部字段被删，key 与 value 其余字段保留
        _, _, schema, plan, hits = _prepare("demo.WithMap", [[1, 2, 2]])
        out = rewrite_payload(
            schema,
            bytes.fromhex("0a0c" "0a016b" "1207" "0a027631" "120173"),
            plan, hits=hits).output
        msg = parse("demo.WithMap", out)
        self.assertEqual(msg.attrs["k"].v, "v1")
        self.assertEqual(msg.attrs["k"].secret, "")

        # G: packed/unpacked 混用的数值完整保留
        _, _, schema, plan, hits = _prepare("demo.Nums", [[3]])
        out = rewrite_payload(
            schema, bytes.fromhex("0a03010203" "0804" "1009" "1a027373"),
            plan, hits=hits).output
        msg = parse("demo.Nums", out)
        self.assertEqual(list(msg.samples), [1, 2, 3, 4])
        self.assertEqual(msg.tail, 9)

        # H2: 删空后父字段仍存在（presence）
        _, _, schema, plan, hits = _prepare("demo.Outer", [[1, 1]])
        out = rewrite_payload(schema, bytes.fromhex("0a03" "0a0178"),
                              plan, hits=hits).output
        msg = parse("demo.Outer", out)
        self.assertTrue(msg.HasField("sub"))
        self.assertEqual(msg.sub.secret, "")

        # I: 递归描述符只删到路径指定深度
        _, _, schema, plan, hits = _prepare("demo.Node", [[1], [2, 1]])
        out = rewrite_payload(
            schema,
            bytes.fromhex("0a0161" "120b" "0a0162" "1206" "0a0163" "1a016e"
                          "1a0178"),
            plan, hits=hits).output
        msg = parse("demo.Node", out)
        self.assertEqual(msg.secret, "")
        self.assertEqual(msg.next.secret, "")
        self.assertEqual(msg.next.next.secret, "c")  # 更深一层不在路径内，保留
        self.assertEqual(msg.name, "x")

    def test_parent_path_covers_child(self):
        # 父路径与子路径同时列出：按父路径覆盖；清单次序/重复不改变结果
        loaded = load_descriptor_set(build_demo_descriptor_set())
        desc = loaded.find_message("demo.Outer")
        p1 = normalize_paths([[1], [1, 1]], desc)
        p2 = normalize_paths([[1, 1], [1]], desc)  # 次序颠倒
        p3 = normalize_paths([[1], [1], [1, 1]], desc)  # 重复
        self.assertEqual(p1, p2)
        for paths in (p1, p2, p3):
            plan = build_plan(paths)
            schema = build_schema(desc)
            hits = {p: 0 for p in paths}
            out = rewrite_payload(
                schema, bytes.fromhex("0a03" "0a0178" "12016b"),
                plan, hits=hits).output
            self.assertEqual(out, bytes.fromhex("12016b"))
            self.assertEqual(hits[(1,)], 1)
            self.assertEqual(hits[(1, 1)], 0)  # 被父路径覆盖，不再单独计数


class TestWireRejections(unittest.TestCase):
    """各类坏字节必须带类别拒绝，且只暴露序号/字段路径/字节位置/类别。"""

    def _reject(self, root, paths, in_hex, category):
        _, _, schema, plan, hits = _prepare(root, paths)
        with self.assertRaises(ExportError) as cm:
            rewrite_payload(schema, bytes.fromhex(in_hex), plan, hits=hits,
                            event_seq=42)
        err = cm.exception
        self.assertEqual(err.category, category, err.to_dict())
        self.assertEqual(err.event_seq, 42)
        # 错误详情不得回显正文内容
        self.assertNotIn(in_hex, str(err.to_dict()))
        return err

    def test_unknown_field(self):
        err = self._reject("demo.M1", [[1]], "4a0100", UNKNOWN_FIELD)
        self.assertEqual(err.field_path, (9,))
        self.assertEqual(err.byte_offset, 0)

    def test_unknown_field_nested(self):
        # 嵌套层出现未知字段同样拒绝（不能静默带出去）
        err = self._reject("demo.Outer", [[1, 1]], "0a03" "4a0100", UNKNOWN_FIELD)
        self.assertEqual(err.field_path, (1, 9))

    def test_unknown_field_inside_deleted_message(self):
        # 即使整个 message 字段将被删除，其内容里的未知字段也照样拒绝：
        # 整份选中正文必须核验成功才算交付
        self._reject("demo.Outer", [[1]], "0a03" "4a0100", UNKNOWN_FIELD)

    def test_wrong_wire_type(self):
        # string 字段用 varint 编码
        self._reject("demo.M1", [[1]], "0801", WRONG_WIRE_TYPE)

    def test_truncated_varint(self):
        self._reject("demo.M1", [[1]], "1081", TRUNCATED)

    def test_truncated_fixed32(self):
        self._reject("demo.Fixed", [[1]], "0d0102", TRUNCATED)

    def test_truncated_fixed64(self):
        self._reject("demo.Fixed", [[1]], "11010203", TRUNCATED)

    def test_length_out_of_bounds(self):
        self._reject("demo.M1", [[1]], "0a0561" "61", INVALID_LENGTH)

    def test_nested_length_out_of_bounds(self):
        # 嵌套 message 的长度越出父边界
        self._reject("demo.Outer", [[1, 1]], "0a05" "0a0178", INVALID_LENGTH)

    def test_bad_packed_varint(self):
        self._reject("demo.Nums", [[3]], "0a020182", INVALID_PACKED)

    def test_bad_packed_fixed32(self):
        self._reject("demo.Fixed", [[1]], "2a03010203", INVALID_PACKED)

    def test_bad_packed_fixed64(self):
        self._reject("demo.Fixed", [[1]], "32050102030405", INVALID_PACKED)

    def test_invalid_utf8_string(self):
        self._reject("demo.M1", [[1]], "0a01ff", INVALID_UTF8)

    def test_bytes_field_not_utf8_checked(self):
        # bytes 字段绝不按 UTF-8 解读：0xFF 合法
        _, _, schema, plan, hits = _prepare("demo.Outer", [[1, 1]])
        out = rewrite_payload(schema, bytes.fromhex("0a03" "1201ff"),
                              plan, hits=hits).output
        self.assertEqual(out, bytes.fromhex("0a03" "1201ff"))

    def test_field_number_zero(self):
        self._reject("demo.M1", [[1]], "0001", INVALID_TAG)

    def test_tag_field_number_out_of_range(self):
        # 字段号 2^29（越界）：tag = (2^29)<<3 = 2^32
        self._reject("demo.M1", [[1]], "808080801001", INVALID_TAG)

    def test_group_wire_type_rejected(self):
        self._reject("demo.M1", [[1]], "0b01", INVALID_TAG)  # wire type 3

    def test_varint_overflow(self):
        self._reject("demo.M1", [[1]], "10" + "ff" * 9 + "7f", INVALID_VARINT)

    def test_truncated_tag(self):
        self._reject("demo.M1", [[1]], "80", TRUNCATED)

    def test_empty_payload_is_valid(self):
        _, _, schema, plan, hits = _prepare("demo.M1", [[1]])
        result = rewrite_payload(schema, b"", plan, hits=hits)
        self.assertEqual(result.output, b"")
        self.assertFalse(result.changed)


class TestLimits(unittest.TestCase):
    def test_max_bytes(self):
        _, _, schema, plan, hits = _prepare("demo.M1", [[1]])
        with self.assertRaises(ExportError) as cm:
            rewrite_payload(schema, bytes.fromhex("0a026161"), plan,
                            limits=Limits(max_bytes=3, max_depth=8,
                                          max_occurrences=8),
                            hits=hits, event_seq=1)
        self.assertEqual(cm.exception.category, RESOURCE_LIMIT)

    def test_max_depth(self):
        _, _, schema, plan, hits = _prepare("demo.Node", [[1]])
        payload = bytes.fromhex("1205" "1203" "0a0163")  # 三层嵌套
        with self.assertRaises(ExportError) as cm:
            rewrite_payload(schema, payload, plan,
                            limits=Limits(max_bytes=100, max_depth=2,
                                          max_occurrences=8),
                            hits=hits)
        self.assertEqual(cm.exception.category, RESOURCE_LIMIT)

    def test_max_occurrences(self):
        _, _, schema, plan, hits = _prepare("demo.M1", [[1]])
        payload = bytes.fromhex("0a0161" "0a0162" "0a0163")
        with self.assertRaises(ExportError) as cm:
            rewrite_payload(schema, payload, plan,
                            limits=Limits(max_bytes=100, max_depth=8,
                                          max_occurrences=2),
                            hits=hits)
        self.assertEqual(cm.exception.category, RESOURCE_LIMIT)

    def test_limits_do_not_export_partial(self):
        # 超限返回未完成：不产出半条消息（异常即无输出）
        _, _, schema, plan, hits = _prepare("demo.Node", [[1]])
        payload = bytes.fromhex("0a0161" "1203" "0a0162")
        try:
            rewrite_payload(schema, payload, plan,
                            limits=Limits(max_bytes=100, max_depth=1,
                                          max_occurrences=8),
                            hits=hits)
            self.fail("应当超限")
        except ExportError as exc:
            self.assertEqual(exc.category, RESOURCE_LIMIT)


if __name__ == "__main__":
    unittest.main()
