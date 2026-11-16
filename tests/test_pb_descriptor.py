"""描述符集子集校验与删除路径校验。"""

import unittest

from offline_mqtt import _protobuf_vendor  # noqa: F401

from google.protobuf import descriptor_pb2

from offline_mqtt.pb_descriptor import (
    build_plan,
    load_descriptor_set,
    normalize_paths,
    validate_remove_path,
)
from offline_mqtt.pb_errors import CONFIG_ERROR, DESCRIPTOR_ERROR, ExportError
from tests._pb_fixtures import build_demo_descriptor_set

FDP = descriptor_pb2.FieldDescriptorProto


def _base_file(fds, name="a.proto", package="demo", syntax="proto3"):
    f = fds.file.add()
    f.name = name
    f.package = package
    f.syntax = syntax
    return f


class TestDescriptorSetLoading(unittest.TestCase):
    def test_ok(self):
        loaded = load_descriptor_set(build_demo_descriptor_set())
        self.assertEqual(loaded.find_message("demo.M1").full_name, "demo.M1")
        self.assertEqual(len(loaded.sha256), 64)

    def test_not_a_descriptor_set(self):
        with self.assertRaises(ExportError) as cm:
            load_descriptor_set(b"\xff\xff\xff\xff")
        self.assertEqual(cm.exception.category, DESCRIPTOR_ERROR)

    def test_empty_set(self):
        with self.assertRaises(ExportError) as cm:
            load_descriptor_set(b"")
        self.assertEqual(cm.exception.category, DESCRIPTOR_ERROR)

    def test_proto2_rejected(self):
        fds = descriptor_pb2.FileDescriptorSet()
        f = _base_file(fds, syntax="proto2")
        m = f.message_type.add(); m.name = "M"
        fld = m.field.add(); fld.name = "a"; fld.number = 1
        fld.label = FDP.LABEL_OPTIONAL; fld.type = FDP.TYPE_STRING
        with self.assertRaises(ExportError) as cm:
            load_descriptor_set(fds.SerializeToString())
        self.assertEqual(cm.exception.category, DESCRIPTOR_ERROR)
        self.assertIn("proto3", cm.exception.detail)

    def test_unspecified_syntax_rejected(self):
        fds = descriptor_pb2.FileDescriptorSet()
        f = _base_file(fds, syntax="")
        m = f.message_type.add(); m.name = "M"
        with self.assertRaises(ExportError) as cm:
            load_descriptor_set(fds.SerializeToString())
        self.assertEqual(cm.exception.category, DESCRIPTOR_ERROR)

    def test_extension_rejected(self):
        fds = descriptor_pb2.FileDescriptorSet()
        f = _base_file(fds)
        m = f.message_type.add(); m.name = "M"
        ext = f.extension.add()
        ext.name = "tag"; ext.number = 100
        ext.label = FDP.LABEL_OPTIONAL; ext.type = FDP.TYPE_STRING
        ext.extendee = ".demo.M"
        with self.assertRaises(ExportError) as cm:
            load_descriptor_set(fds.SerializeToString())
        self.assertEqual(cm.exception.category, DESCRIPTOR_ERROR)
        self.assertIn("extension", cm.exception.detail)

    def test_group_rejected(self):
        fds = descriptor_pb2.FileDescriptorSet()
        f = _base_file(fds)
        m = f.message_type.add(); m.name = "M"
        fld = m.field.add(); fld.name = "g"; fld.number = 1
        fld.label = FDP.LABEL_OPTIONAL; fld.type = FDP.TYPE_GROUP
        fld.type_name = ".demo.M.G"
        nested = m.nested_type.add(); nested.name = "G"
        with self.assertRaises(ExportError) as cm:
            load_descriptor_set(fds.SerializeToString())
        self.assertEqual(cm.exception.category, DESCRIPTOR_ERROR)
        self.assertIn("group", cm.exception.detail)

    def test_required_rejected(self):
        fds = descriptor_pb2.FileDescriptorSet()
        f = _base_file(fds)
        m = f.message_type.add(); m.name = "M"
        fld = m.field.add(); fld.name = "a"; fld.number = 1
        fld.label = FDP.LABEL_REQUIRED; fld.type = FDP.TYPE_STRING
        with self.assertRaises(ExportError) as cm:
            load_descriptor_set(fds.SerializeToString())
        self.assertEqual(cm.exception.category, DESCRIPTOR_ERROR)

    def test_missing_dependency(self):
        fds = descriptor_pb2.FileDescriptorSet()
        f = _base_file(fds)
        f.dependency.append("missing.proto")
        m = f.message_type.add(); m.name = "M"
        with self.assertRaises(ExportError) as cm:
            load_descriptor_set(fds.SerializeToString())
        self.assertEqual(cm.exception.category, DESCRIPTOR_ERROR)
        self.assertIn("missing.proto", cm.exception.detail)

    def test_duplicate_symbol_conflict(self):
        fds = descriptor_pb2.FileDescriptorSet()
        for name in ("a.proto", "b.proto"):
            f = _base_file(fds, name=name)
            m = f.message_type.add(); m.name = "M"
            fld = m.field.add(); fld.name = "a"; fld.number = 1
            fld.label = FDP.LABEL_OPTIONAL; fld.type = FDP.TYPE_STRING
        with self.assertRaises(ExportError) as cm:
            load_descriptor_set(fds.SerializeToString())
        self.assertEqual(cm.exception.category, DESCRIPTOR_ERROR)

    def test_duplicate_file_name(self):
        fds = descriptor_pb2.FileDescriptorSet()
        for _ in range(2):
            f = _base_file(fds)
            f.message_type.add().name = "M"
        with self.assertRaises(ExportError) as cm:
            load_descriptor_set(fds.SerializeToString())
        self.assertEqual(cm.exception.category, DESCRIPTOR_ERROR)

    def test_dependency_order_resolved(self):
        # 依赖项乱序给出也能按拓扑序载入
        fds = descriptor_pb2.FileDescriptorSet()
        f = _base_file(fds, name="dep.proto", package="dep")
        m = f.message_type.add(); m.name = "Base"
        fld = m.field.add(); fld.name = "a"; fld.number = 1
        fld.label = FDP.LABEL_OPTIONAL; fld.type = FDP.TYPE_STRING
        f2 = _base_file(fds, name="main.proto", package="main")
        f2.dependency.append("dep.proto")
        m2 = f2.message_type.add(); m2.name = "Top"
        fld = m2.field.add(); fld.name = "b"; fld.number = 1
        fld.label = FDP.LABEL_OPTIONAL; fld.type = FDP.TYPE_MESSAGE
        fld.type_name = ".dep.Base"
        loaded = load_descriptor_set(fds.SerializeToString())
        self.assertEqual(loaded.find_message("main.Top").full_name, "main.Top")

    def test_dangling_type_reference_rejected(self):
        # 目录中类型引用必须完整可解析
        for ftype, tname in ((FDP.TYPE_MESSAGE, ".demo.Nope"),
                             (FDP.TYPE_ENUM, ".demo.NoEnum")):
            fds = descriptor_pb2.FileDescriptorSet()
            f = _base_file(fds)
            m = f.message_type.add(); m.name = "M"
            fld = m.field.add(); fld.name = "x"; fld.number = 1
            fld.label = FDP.LABEL_OPTIONAL; fld.type = ftype
            fld.type_name = tname
            with self.assertRaises(ExportError) as cm:
                load_descriptor_set(fds.SerializeToString())
            self.assertEqual(cm.exception.category, DESCRIPTOR_ERROR)
            self.assertIn("不可解析", cm.exception.detail)


class TestRemovePathValidation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.loaded = load_descriptor_set(build_demo_descriptor_set())

    def _ok(self, root, path):
        validate_remove_path(self.loaded.find_message(root), path)

    def _bad(self, root, path, needle=""):
        with self.assertRaises(ExportError) as cm:
            validate_remove_path(self.loaded.find_message(root), path)
        self.assertEqual(cm.exception.category, CONFIG_ERROR)
        if needle:
            self.assertIn(needle, cm.exception.detail)

    def test_valid_paths(self):
        self._ok("demo.M1", (1,))
        self._ok("demo.M1", (3,))          # proto3 optional 字段
        self._ok("demo.Outer", (1, 1))
        self._ok("demo.Outer", (1, 2))     # 删整个 bytes 字段（终止项可以是标量）
        self._ok("demo.WithMap", (1,))     # 删整个 map
        self._ok("demo.WithMap", (1, 2))   # 删 map 的 message value 字段
        self._ok("demo.WithMap", (1, 2, 2))
        self._ok("demo.Node", (2, 2, 2, 1))  # 递归消息上的有限路径

    def test_path_must_exist(self):
        self._bad("demo.M1", (9,), "不存在")

    def test_path_through_scalar(self):
        self._bad("demo.M1", (2, 1), "标量")
        self._bad("demo.Outer", (1, 1, 1), "标量")  # Inner.secret 是 string

    def test_map_key_rejected(self):
        self._bad("demo.WithMap", (1, 1), "key")
        self._bad("demo.WithMap", (1, 1, 2), "key")

    def test_map_entry_unknown_field(self):
        self._bad("demo.WithMap", (1, 3), "entry")

    def test_scalar_map_value_rejected(self):
        self._bad("demo.StrMap", (1, 2), "标量 map value")
        self._bad("demo.StrMap", (1, 2, 1), "标量 map value")

    def test_normalize_rejects_bad_shapes(self):
        desc = self.loaded.find_message("demo.M1")
        for bad in ([], [0], [-1], [1 << 29], ["1"], [True], [1.5]):
            with self.assertRaises(ExportError) as cm:
                normalize_paths([bad], desc)
            self.assertEqual(cm.exception.category, CONFIG_ERROR, bad)

    def test_normalize_dedup_and_sort(self):
        desc = self.loaded.find_message("demo.M1")
        out = normalize_paths([[2], [1], [2], [1]], desc)
        self.assertEqual(out, [(1,), (2,)])


if __name__ == "__main__":
    unittest.main()
