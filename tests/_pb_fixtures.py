"""测试夹具：手工构建 FileDescriptorSet（不依赖 protoc）与手写 wire 用例。

所有“预期输出”都是手工逐字节写出的 hex，不由被测的重写函数生成；
语义再用 protobuf 官方动态消息复核。
"""

from __future__ import annotations

from offline_mqtt import _protobuf_vendor  # noqa: F401  先行布置 vendored protobuf

from google.protobuf import descriptor_pb2

FDP = descriptor_pb2.FieldDescriptorProto


def _field(msg, name, number, ftype, label=FDP.LABEL_OPTIONAL,
           type_name=None, oneof_index=None, proto3_optional=False):
    f = msg.field.add()
    f.name = name
    f.number = number
    f.label = label
    f.type = ftype
    if type_name:
        f.type_name = type_name
    if oneof_index is not None:
        f.oneof_index = oneof_index
    if proto3_optional:
        f.proto3_optional = True
    return f


def build_demo_descriptor_set() -> bytes:
    """demo 包：覆盖标量/嵌套/repeated/packed/oneof/map/proto3 optional/递归。"""
    fds = descriptor_pb2.FileDescriptorSet()
    f = fds.file.add()
    f.name = "demo.proto"
    f.package = "demo"
    f.syntax = "proto3"

    # message M1 { string secret = 1; uint32 n = 2; optional string note = 3; }
    m = f.message_type.add(); m.name = "M1"
    _field(m, "secret", 1, FDP.TYPE_STRING)
    _field(m, "n", 2, FDP.TYPE_UINT32)
    m.oneof_decl.add().name = "_note"
    _field(m, "note", 3, FDP.TYPE_STRING, oneof_index=0, proto3_optional=True)

    # message Inner { string secret = 1; bytes pad = 2; string label = 3; }
    m = f.message_type.add(); m.name = "Inner"
    _field(m, "secret", 1, FDP.TYPE_STRING)
    _field(m, "pad", 2, FDP.TYPE_BYTES)
    _field(m, "label", 3, FDP.TYPE_STRING)

    # message Outer { Inner sub = 1; string keep = 2; }
    m = f.message_type.add(); m.name = "Outer"
    _field(m, "sub", 1, FDP.TYPE_MESSAGE, type_name=".demo.Inner")
    _field(m, "keep", 2, FDP.TYPE_STRING)

    # message Choice { oneof sel { string token = 1; uint32 code = 2; } }
    m = f.message_type.add(); m.name = "Choice"
    m.oneof_decl.add().name = "sel"
    _field(m, "token", 1, FDP.TYPE_STRING, oneof_index=0)
    _field(m, "code", 2, FDP.TYPE_UINT32, oneof_index=0)

    # message Rep { string tag = 1; string secret = 2; }
    m = f.message_type.add(); m.name = "Rep"
    _field(m, "tag", 1, FDP.TYPE_STRING)
    _field(m, "secret", 2, FDP.TYPE_STRING)

    # message RepList { repeated Rep items = 1; }
    m = f.message_type.add(); m.name = "RepList"
    _field(m, "items", 1, FDP.TYPE_MESSAGE, label=FDP.LABEL_REPEATED,
           type_name=".demo.Rep")

    # message Attr { string v = 1; string secret = 2; }
    m = f.message_type.add(); m.name = "Attr"
    _field(m, "v", 1, FDP.TYPE_STRING)
    _field(m, "secret", 2, FDP.TYPE_STRING)

    # message WithMap { map<string, Attr> attrs = 1; }
    m = f.message_type.add(); m.name = "WithMap"
    entry = m.nested_type.add(); entry.name = "AttrsEntry"
    entry.options.map_entry = True
    _field(entry, "key", 1, FDP.TYPE_STRING)
    _field(entry, "value", 2, FDP.TYPE_MESSAGE, type_name=".demo.Attr")
    _field(m, "attrs", 1, FDP.TYPE_MESSAGE, label=FDP.LABEL_REPEATED,
           type_name=".demo.WithMap.AttrsEntry")

    # message StrMap { map<string, string> kv = 1; }
    m = f.message_type.add(); m.name = "StrMap"
    entry = m.nested_type.add(); entry.name = "KvEntry"
    entry.options.map_entry = True
    _field(entry, "key", 1, FDP.TYPE_STRING)
    _field(entry, "value", 2, FDP.TYPE_STRING)
    _field(m, "kv", 1, FDP.TYPE_MESSAGE, label=FDP.LABEL_REPEATED,
           type_name=".demo.StrMap.KvEntry")

    # message Nums { repeated uint32 samples = 1; uint32 tail = 2; string secret = 3; }
    m = f.message_type.add(); m.name = "Nums"
    _field(m, "samples", 1, FDP.TYPE_UINT32, label=FDP.LABEL_REPEATED)
    _field(m, "tail", 2, FDP.TYPE_UINT32)
    _field(m, "secret", 3, FDP.TYPE_STRING)

    # message Node { string secret = 1; Node next = 2; string name = 3; }
    m = f.message_type.add(); m.name = "Node"
    _field(m, "secret", 1, FDP.TYPE_STRING)
    _field(m, "next", 2, FDP.TYPE_MESSAGE, type_name=".demo.Node")
    _field(m, "name", 3, FDP.TYPE_STRING)

    # message Fixed { fixed32 a = 1; fixed64 b = 2; double c = 3; float d = 4;
    #                 repeated fixed32 rf = 5; repeated double rd = 6; }
    m = f.message_type.add(); m.name = "Fixed"
    _field(m, "a", 1, FDP.TYPE_FIXED32)
    _field(m, "b", 2, FDP.TYPE_FIXED64)
    _field(m, "c", 3, FDP.TYPE_DOUBLE)
    _field(m, "d", 4, FDP.TYPE_FLOAT)
    _field(m, "rf", 5, FDP.TYPE_FIXED32, label=FDP.LABEL_REPEATED)
    _field(m, "rd", 6, FDP.TYPE_DOUBLE, label=FDP.LABEL_REPEATED)

    # enum Status { STATUS_UNKNOWN = 0; STATUS_ON = 1; }
    e = f.enum_type.add(); e.name = "Status"
    v = e.value.add(); v.name = "STATUS_UNKNOWN"; v.number = 0
    v = e.value.add(); v.name = "STATUS_ON"; v.number = 1

    # message WithEnum { Status status = 1; string secret = 2; }
    m = f.message_type.add(); m.name = "WithEnum"
    _field(m, "status", 1, FDP.TYPE_ENUM, type_name=".demo.Status")
    _field(m, "secret", 2, FDP.TYPE_STRING)

    return fds.SerializeToString()


# ---------------------------------------------------------------------------
# 手写 wire 用例：input_hex / expected_hex 全部手工逐字节写出
# ---------------------------------------------------------------------------
# 每项: (用例名, 根消息, remove_paths, input_hex, expected_hex, 预期区间, 预期hits)
HAND_CASES = [
    # A: 敏感 singular string 连续两次，后跟未删的非规范但合法 varint
    ("A_singular_twice_noncanonical_varint", "demo.M1", [[1]],
     "0a0261610a0262621081" "00",
     "108100",
     [(0, 4), (4, 8)], {(1,): 2}),

    # B: 嵌套 message 长度跨 127/128 边界；删子字段后只有祖先长度前缀变化
    ("B_nested_length_boundary_127_128", "demo.Outer", [[1, 1]],
     "0a8201" "0a027878" + "127c" + "00" * 124 + "12016b",
     "0a7e" + "127c" + "00" * 124 + "12016b",
     [(3, 7)], {(1, 1): 1}),

    # B2: 未变化的容器连非规范长度前缀也逐字节不变
    ("B2_unchanged_container_keeps_noncanonical_length", "demo.Outer", [[1, 1]],
     "0a8300" "1a0161",
     "0a8300" "1a0161",
     [], {(1, 1): 0}),

    # B3: 兄弟字段被删时，未删容器的非规范长度前缀同样逐字节不变
    ("B3_sibling_delete_keeps_noncanonical_length", "demo.Outer", [[2]],
     "0a8300" "1a0161" "12016b",
     "0a8300" "1a0161",
     [(6, 9)], {(2,): 1}),

    # C: 同一 singular message 分两段出现，两段敏感原值都消失
    ("C_same_message_field_two_segments", "demo.Outer", [[1, 1]],
     "0a07" "0a027331" "1a0161" "0a07" "0a027332" "1a0162",
     "0a03" "1a0161" "0a03" "1a0162",
     [(2, 6), (11, 15)], {(1, 1): 2}),

    # D: oneof 中后来被覆盖的旧成员也按真实字段号删除
    ("D_oneof_stale_member", "demo.Choice", [[1]],
     "0a027431" "1005",
     "1005",
     [(0, 4)], {(1,): 1}),

    # E: repeated message 的每个出现都删
    ("E_repeated_message_each_occurrence", "demo.RepList", [[1, 2]],
     "0a06" "0a0161" "120178" "0a06" "0a0162" "120179",
     "0a03" "0a0161" "0a03" "0a0162",
     [(5, 8), (13, 16)], {(1, 2): 2}),

    # F: map 的 message value 内部继续删
    ("F_map_message_value_descend", "demo.WithMap", [[1, 2, 2]],
     "0a0c" "0a016b" "1207" "0a027631" "120173",
     "0a09" "0a016b" "1204" "0a027631",
     [(11, 14)], {(1, 2, 2): 1}),

    # F2: 删整个 map 字段
    ("F2_delete_whole_map", "demo.WithMap", [[1]],
     "0a0c" "0a016b" "1207" "0a027631" "120173",
     "",
     [(0, 14)], {(1,): 1}),

    # F4: 把 map 的 message value 字段整个删掉（保留 key）
    ("F4_delete_map_value_field", "demo.WithMap", [[1, 2]],
     "0a0c" "0a016b" "1207" "0a027631" "120173",
     "0a03" "0a016b",
     [(5, 14)], {(1, 2): 1}),

    # G: packed 与 unpacked 混用的未删数值逐字节保留
    ("G_packed_and_unpacked_preserved", "demo.Nums", [[3]],
     "0a03010203" "0804" "1009" "1a027373",
     "0a03010203" "0804" "1009",
     [(9, 13)], {(3,): 1}),

    # H1: 空容器原样保留
    ("H1_empty_container_preserved", "demo.Outer", [[1, 1]],
     "0a00" "12016b",
     "0a00" "12016b",
     [], {(1, 1): 0}),

    # H2: 删空后父字段仍存在（长度 0）
    ("H2_emptied_container_stays_present", "demo.Outer", [[1, 1]],
     "0a03" "0a0178",
     "0a00",
     [(2, 5)], {(1, 1): 1}),

    # I: 递归描述符；有限路径只删到指定深度
    ("I_recursive_descriptor_finite_path", "demo.Node", [[1], [2, 1]],
     "0a0161" "120b" "0a0162" "1206" "0a0163" "1a016e" "1a0178",
     "1208" "1206" "0a0163" "1a016e" "1a0178",
     [(0, 3), (5, 8)], {(1,): 1, (2, 1): 1}),

    # J: 未删字段的非规范 tag varint 逐字节保留
    ("J_noncanonical_tag_preserved", "demo.M1", [[1]],
     "0a0161" "9000" "05",
     "9000" "05",
     [(0, 3)], {(1,): 1}),

    # K: 定长数值（fixed32/float）逐字节保留；删 double（wire type 1）
    ("K_fixed_width_fields_preserved", "demo.Fixed", [[3]],
     "0d01020304" "2521222324" "190a0b0c0d0e0f1011",
     "0d01020304" "2521222324",
     [(10, 19)], {(3,): 1}),

    # L: enum 按 varint 处理并保留
    ("L_enum_varint_preserved", "demo.WithEnum", [[2]],
     "0801" "120178",
     "0801",
     [(2, 5)], {(2,): 1}),
]
