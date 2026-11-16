#!/usr/bin/env python3
"""离线外发（按描述符删除字段）端到端演示。

覆盖：
1. 手写 wire hex 与独立预期输出（逐字节核对 + 官方动态消息语义复核）；
2. 真实规模：>= 十万条消息的账本，经 CLI 导出，展示峰值内存与输出首尾记录；
3. 导出经读取接口重新接入空库，核对发布次数与载荷；
4. 各类真实拒绝证据（未知字段/错误 wire type/截短/非法 string/map key/
   坏描述符依赖/最后一条损坏/目标已存在/资源超限）；
5. 来源账本与旁边文件全程保持原样。

证据写入 demo/pb_export_evidence/。
"""

from __future__ import annotations

import hashlib
import json
import os
import resource
import shutil
import sqlite3
import subprocess
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

from offline_mqtt import _protobuf_vendor  # noqa: F401,E402
from google.protobuf import descriptor_pb2, message_factory  # noqa: E402

from offline_mqtt.ingress import MqttIngress  # noqa: E402
from offline_mqtt.pb_descriptor import load_descriptor_set  # noqa: E402
from offline_mqtt.pb_reader import iter_export  # noqa: E402
from offline_mqtt.pb_wire import build_schema, rewrite_payload  # noqa: E402
from offline_mqtt.pb_descriptor import build_plan, normalize_paths  # noqa: E402
from tests._pb_fixtures import HAND_CASES, build_demo_descriptor_set  # noqa: E402

EVIDENCE_DIR = os.path.join(REPO_ROOT, "demo", "pb_export_evidence")
FDP = descriptor_pb2.FieldDescriptorProto

N_TELEMETRY = 120_000
N_AUX = 5_000
N_OTHER = 3_000
N_TEMPLATES = 2_000


# ---------------------------------------------------------------------------
# 演示用遥测描述符（手工构建，不依赖 protoc）
# ---------------------------------------------------------------------------

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


def build_telemetry_descriptor_set() -> bytes:
    fds = descriptor_pb2.FileDescriptorSet()
    f = fds.file.add()
    f.name = "telemetry.proto"
    f.package = "acme.telemetry"
    f.syntax = "proto3"

    m = f.message_type.add(); m.name = "SubInfo"
    _field(m, "secret", 1, FDP.TYPE_STRING)
    _field(m, "label", 2, FDP.TYPE_STRING)

    m = f.message_type.add(); m.name = "Reading"
    _field(m, "tag", 1, FDP.TYPE_STRING)
    _field(m, "secret", 2, FDP.TYPE_STRING)

    m = f.message_type.add(); m.name = "Attr"
    _field(m, "v", 1, FDP.TYPE_STRING)
    _field(m, "secret", 2, FDP.TYPE_STRING)

    m = f.message_type.add(); m.name = "Node"
    _field(m, "secret", 1, FDP.TYPE_STRING)
    _field(m, "next", 2, FDP.TYPE_MESSAGE, type_name=".acme.telemetry.Node")

    # DeviceReport：型号/计数保留，身份与各处 secret 删除
    m = f.message_type.add(); m.name = "DeviceReport"
    _field(m, "device_model", 1, FDP.TYPE_STRING)
    _field(m, "device_identity", 2, FDP.TYPE_STRING)
    _field(m, "counter", 3, FDP.TYPE_UINT32)
    _field(m, "info", 4, FDP.TYPE_MESSAGE,
           type_name=".acme.telemetry.SubInfo")
    _field(m, "readings", 5, FDP.TYPE_MESSAGE, label=FDP.LABEL_REPEATED,
           type_name=".acme.telemetry.Reading")
    entry = m.nested_type.add(); entry.name = "AttrsEntry"
    entry.options.map_entry = True
    _field(entry, "key", 1, FDP.TYPE_STRING)
    _field(entry, "value", 2, FDP.TYPE_MESSAGE,
           type_name=".acme.telemetry.Attr")
    _field(m, "attrs", 6, FDP.TYPE_MESSAGE, label=FDP.LABEL_REPEATED,
           type_name=".acme.telemetry.DeviceReport.AttrsEntry")
    m.oneof_decl.add().name = "choice"
    _field(m, "token", 7, FDP.TYPE_STRING, oneof_index=0)
    _field(m, "code", 8, FDP.TYPE_UINT32, oneof_index=0)
    _field(m, "samples", 9, FDP.TYPE_UINT32, label=FDP.LABEL_REPEATED)
    _field(m, "chain", 10, FDP.TYPE_MESSAGE,
           type_name=".acme.telemetry.Node")
    m.oneof_decl.add().name = "_note"
    _field(m, "note", 11, FDP.TYPE_STRING, oneof_index=1,
           proto3_optional=True)

    m = f.message_type.add(); m.name = "HeartBeat"
    _field(m, "station_id", 1, FDP.TYPE_STRING)
    _field(m, "tick", 2, FDP.TYPE_UINT32)

    return fds.SerializeToString()


def make_templates(descriptor_bytes: bytes, n: int):
    """用官方动态消息生成 n 份互不相同的序列化载荷（独立于被测重写器）。"""
    loaded = load_descriptor_set(descriptor_bytes)
    pool = loaded.pool

    def cls(name):
        return message_factory.GetMessageClass(pool.FindMessageTypeByName(name))

    DrCls = cls("acme.telemetry.DeviceReport")
    SubCls = cls("acme.telemetry.SubInfo")
    RdCls = cls("acme.telemetry.Reading")
    AttrCls = cls("acme.telemetry.Attr")
    NodeCls = cls("acme.telemetry.Node")
    HbCls = cls("acme.telemetry.HeartBeat")

    telemetry = []
    for i in range(n):
        msg = DrCls(
            device_model="MX-2000",
            device_identity=f"IDENT-{i:06d}-SECRET",
            counter=i,
            info=SubCls(secret=f"infosec-{i}", label="line-A"),
            readings=[RdCls(tag="t1", secret=f"rs1-{i}"),
                      RdCls(tag="t2", secret=f"rs2-{i}")],
            attrs={"k1": AttrCls(v="v1", secret=f"as-{i}")},
            samples=[1, 2, 3, 300],
            chain=NodeCls(secret=f"cs-{i}",
                          next=NodeCls(secret=f"cs2-{i}")),
            note=f"note-{i}",
        )
        if i % 2:
            msg.token = f"tok-{i}"
        else:
            msg.code = i & 0xFFFF
        telemetry.append(msg.SerializeToString())
    heartbeats = [
        HbCls(station_id=f"STATION-{i:05d}-SECRET", tick=i).SerializeToString()
        for i in range(max(1, n // 4))
    ]
    return telemetry, heartbeats


# ---------------------------------------------------------------------------
# 小工具
# ---------------------------------------------------------------------------

def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def run_cli(args, cwd=REPO_ROOT):
    """在子进程跑 CLI；返回 (exit_code, stdout)。"""
    proc = subprocess.run(
        [sys.executable, "-m", "offline_mqtt.cli"] + args,
        cwd=cwd, capture_output=True, text=True)
    out = proc.stdout.strip()
    if proc.stderr.strip():
        out += "\n[stderr]\n" + proc.stderr.strip()
    return proc.returncode, out


def peak_rss_kb_of_children():
    return resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss


# ---------------------------------------------------------------------------
# 1. 手写 wire 用例
# ---------------------------------------------------------------------------

def run_hand_cases():
    loaded = load_descriptor_set(build_demo_descriptor_set())
    cases = []
    for (name, root, paths, in_hex, out_hex, intervals, hits) in HAND_CASES:
        desc = loaded.find_message(root)
        norm = normalize_paths(paths, desc)
        plan = build_plan(norm)
        schema = build_schema(desc)
        hit_map = {p: 0 for p in norm}
        payload = bytes.fromhex(in_hex)
        expected = bytes.fromhex(out_hex)
        result = rewrite_payload(schema, payload, plan, hits=hit_map,
                                 event_seq=1)
        # 逐字节核对：输出必须等于手工写出的预期
        byte_exact = result.output == expected
        # 未删区间次序保持：删除区间外的输入字节按序出现在输出中
        # （长度前缀重算只发生在被改容器上，已由 byte_exact 覆盖；
        #  这里再独立验证敏感原值不在输出中）
        sensitive_gone = all(
            payload[s:e] not in result.output
            for s, e in intervals if e - s >= 3
        )
        # 官方动态消息复核输出语义
        cls = message_factory.GetMessageClass(
            loaded.pool.FindMessageTypeByName(root))
        parsed = cls.FromString(result.output)  # 能解析即结构合法
        ok = (byte_exact and sensitive_gone
              and result.intervals == intervals and hit_map == hits)
        cases.append({
            "case": name,
            "root": root,
            "remove_paths": [list(p) for p in norm],
            "input_hex": in_hex,
            "expected_hex": out_hex,
            "output_hex": result.output.hex(),
            "byte_exact_vs_handwritten": byte_exact,
            "deleted_segments_absent": sensitive_gone,
            "intervals": result.intervals,
            "hits": {".".join(map(str, k)): v for k, v in hit_map.items()},
            "dynamic_message_parse_ok": parsed is not None,
            "ok": ok,
        })
        if not ok:
            raise SystemExit(f"手写用例失败: {name}\n"
                             f" got {result.output.hex()}\n exp {out_hex}")
    return cases


# ---------------------------------------------------------------------------
# 2. 规模演示
# ---------------------------------------------------------------------------

def seed_scale_ledger(db_path, telemetry_templates, heartbeat_templates):
    t0 = time.time()
    with MqttIngress(db_path) as ing:
        ing.ingest({"type": "CONNECT", "client_id": "collector-1"})
        ing.ingest({"type": "CONNECT", "client_id": "collector-2"})
        for i in range(N_TELEMETRY):
            ing.ingest({
                "type": "PUBLISH",
                "session_id": f"collector-{1 + i % 2}",
                "topic": "factory/telemetry",
                "payload": telemetry_templates[i % len(telemetry_templates)],
                "qos": 1 + (i % 2),
                "message_id": (i % 60000) + 1,
                "dup": bool(i % 7 == 0),
                "retain": bool(i % 1000 == 0),
            })
        for i in range(N_AUX):
            ing.ingest({
                "type": "PUBLISH",
                "session_id": "collector-1",
                "topic": "factory/aux",
                "payload": heartbeat_templates[i % len(heartbeat_templates)],
                "qos": 0,
            })
        for i in range(N_OTHER):
            ing.ingest({
                "type": "PUBLISH",
                "session_id": "collector-2",
                "topic": "factory/other",
                "payload": b"\x0a\x01x",
                "qos": 0,
            })
    return time.time() - t0


def reingest_and_verify(export_dir, new_db):
    """把导出经读取接口接入空库，核对发布次数与载荷（全程流式）。"""
    t0 = time.time()
    accepted = 0
    with MqttIngress(new_db) as ing:
        for rec in iter_export(export_dir):
            res = ing.ingest(rec.to_ingest_record())
            if not res.accepted:
                raise SystemExit(f"重新接入被拒: {res}")
            accepted += 1
    ingest_seconds = time.time() - t0
    # 流式核对：新库按 seq 次序与导出记录逐条对齐，不把载荷装入列表
    conn = sqlite3.connect(f"file:{new_db}?mode=ro", uri=True)
    try:
        cur = conn.execute(
            "SELECT topic, payload, qos, retain, dup, message_id, session_id"
            " FROM ledger ORDER BY seq")
        checked = 0
        mismatches = 0
        per_topic = {}
        for rec, row in zip(iter_export(export_dir), cur):
            (topic, payload, qos, retain, dup, message_id, session_id) = row
            checked += 1
            per_topic[rec.topic] = per_topic.get(rec.topic, 0) + 1
            if (topic != rec.topic or bytes(payload) != rec.payload
                    or qos != rec.qos or bool(retain) != rec.retain
                    or bool(dup) != rec.dup or message_id != rec.message_id
                    or session_id != rec.session_id):
                mismatches += 1
        leftover = cur.fetchone()  # 新库不应有额外记录
    finally:
        conn.close()
    ok = mismatches == 0 and leftover is None and checked == accepted
    return {
        "reingested_publishes": accepted,
        "ledger_events_checked": checked,
        "field_mismatches": mismatches,
        "ledger_has_extra_rows": leftover is not None,
        "per_topic_counts": per_topic,
        "seconds": round(ingest_seconds, 2),
        "ok": ok,
    }


def first_last_records(messages_path):
    first = last = None
    count = 0
    with open(messages_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if first is None:
                first = obj
            last = obj
            count += 1

    def shrink(obj):
        obj = dict(obj)
        b64 = obj["payload_base64"]
        obj["payload_base64"] = b64[:60] + ("..." if len(b64) > 60 else "")
        obj["payload_base64_length"] = len(b64)
        return obj

    return count, shrink(first), shrink(last)


# ---------------------------------------------------------------------------
# 3. 拒绝证据
# ---------------------------------------------------------------------------

def rejection_cases(workdir, descriptor_bytes):
    """每例真实跑 CLI，记录退出码与错误 JSON。"""
    loaded = load_descriptor_set(build_demo_descriptor_set())
    cls = message_factory.GetMessageClass(
        loaded.pool.FindMessageTypeByName("demo.M1"))
    good = cls(secret="x", n=1).SerializeToString()

    cases = []

    def run_case(name, payload_hex_list, config_topics, descriptor_bytes_use,
                 expect_code):
        case_dir = os.path.join(workdir, name)
        os.makedirs(case_dir, exist_ok=True)
        db = os.path.join(case_dir, "ledger.db")
        with MqttIngress(db) as ing:
            for j, hexpayload in enumerate(payload_hex_list):
                ing.ingest({"type": "PUBLISH", "session_id": "s",
                            "topic": config_topics[0]["topic"],
                            "payload": bytes.fromhex(hexpayload), "qos": 0})
        desc_path = os.path.join(case_dir, "desc.bin")
        with open(desc_path, "wb") as fh:
            fh.write(descriptor_bytes_use)
        cfg_path = os.path.join(case_dir, "cfg.json")
        with open(cfg_path, "w") as fh:
            json.dump({"topics": config_topics}, fh)
        out_dir = os.path.join(case_dir, "out")
        code, out = run_cli(["export", db, "--descriptor", desc_path,
                             "--config", cfg_path, "--out", out_dir])
        # inspect 必须拒绝不完整目录
        insp_code, _ = run_cli(["inspect", out_dir])
        cases.append({
            "case": name,
            "exit_code": code,
            "expected_exit_code": expect_code,
            "inspect_exit_code_after_failure": insp_code,
            "output_dir_left": os.path.exists(out_dir),
            "error_json": json.loads(out) if out.strip().startswith("{") else out,
            "ok": code == expect_code and not os.path.exists(out_dir)
                  and insp_code != 0,
        })
        return cases[-1]

    m1_cfg = [{"topic": "t/m1", "message_type": "demo.M1",
               "remove_paths": [[1]]}]
    demo_desc = build_demo_descriptor_set()

    run_case("unknown_field", ["0a0161" "4a0100"], m1_cfg, demo_desc, 2)
    run_case("wrong_wire_type", ["0801"], m1_cfg, demo_desc, 2)
    run_case("truncated_payload", ["1081"], m1_cfg, demo_desc, 2)
    run_case("invalid_utf8_string", ["0a01ff"], m1_cfg, demo_desc, 2)
    run_case("map_key_deletion", ["0a00"],
             [{"topic": "t/m1", "message_type": "demo.WithMap",
               "remove_paths": [[1, 1]]}], demo_desc, 2)

    # 坏描述符依赖
    fds = descriptor_pb2.FileDescriptorSet()
    f = fds.file.add()
    f.name = "x.proto"; f.package = "x"; f.syntax = "proto3"
    f.dependency.append("missing.proto")
    f.message_type.add().name = "M"
    run_case("bad_descriptor_dependency", ["0a0161"], m1_cfg,
             fds.SerializeToString(), 2)

    # 资源超限（max_bytes=2，正文 4 字节）
    case_dir = os.path.join(workdir, "resource_limit")
    os.makedirs(case_dir, exist_ok=True)
    db = os.path.join(case_dir, "ledger.db")
    with MqttIngress(db) as ing:
        ing.ingest({"type": "PUBLISH", "session_id": "s", "topic": "t/m1",
                    "payload": bytes.fromhex("0a0161"), "qos": 0})
    desc_path = os.path.join(case_dir, "desc.bin")
    open(desc_path, "wb").write(demo_desc)
    cfg_path = os.path.join(case_dir, "cfg.json")
    json.dump({"topics": m1_cfg, "limits": {"max_bytes": 2}}, open(cfg_path, "w"))
    code, out = run_cli(["export", db, "--descriptor", desc_path,
                         "--config", cfg_path, "--out",
                         os.path.join(case_dir, "out")])
    cases.append({
        "case": "resource_limit",
        "exit_code": code,
        "expected_exit_code": 3,
        "output_dir_left": os.path.exists(os.path.join(case_dir, "out")),
        "error_json": json.loads(out) if out.strip().startswith("{") else out,
        "ok": code == 3 and not os.path.exists(os.path.join(case_dir, "out")),
    })
    return cases


# ---------------------------------------------------------------------------

def main():
    if os.path.isdir(EVIDENCE_DIR):
        shutil.rmtree(EVIDENCE_DIR)
    os.makedirs(EVIDENCE_DIR)
    results = {"started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}

    # ---- 1. 手写 wire 用例 -------------------------------------------------
    print("== 1. 手写 wire hex 用例（独立预期输出 + 官方动态消息复核）")
    results["hand_cases"] = run_hand_cases()
    for c in results["hand_cases"]:
        print(f"   {c['case']}: byte_exact={c['byte_exact_vs_handwritten']} "
              f"hits={c['hits']}")
    assert all(c["ok"] for c in results["hand_cases"])

    # ---- 2. 规模导出 -------------------------------------------------------
    print(f"== 2. 规模导出：{N_TELEMETRY + N_AUX + N_OTHER} 条入账，"
          f"{N_TELEMETRY + N_AUX} 条选中")
    descriptor_bytes = build_telemetry_descriptor_set()
    desc_path = os.path.join(EVIDENCE_DIR, "descriptor.bin")
    with open(desc_path, "wb") as fh:
        fh.write(descriptor_bytes)
    config = {
        "version": 1,
        "topics": [
            {"topic": "factory/telemetry",
             "message_type": "acme.telemetry.DeviceReport",
             "remove_paths": [[2], [4, 1], [5, 2], [6, 2, 2], [7], [10, 1]]},
            {"topic": "factory/aux",
             "message_type": "acme.telemetry.HeartBeat",
             "remove_paths": [[1]]},
        ],
    }
    cfg_path = os.path.join(EVIDENCE_DIR, "config.json")
    with open(cfg_path, "w") as fh:
        json.dump(config, fh, indent=1)

    print("   生成载荷模板（官方动态消息序列化）…")
    telemetry_templates, heartbeat_templates = make_templates(
        descriptor_bytes, N_TEMPLATES)
    ledger_path = os.path.join(EVIDENCE_DIR, "ledger.db")
    print(f"   用现有接入器入账 {N_TELEMETRY + N_AUX + N_OTHER} 条…")
    seed_seconds = seed_scale_ledger(ledger_path, telemetry_templates,
                                     heartbeat_templates)
    print(f"   入账耗时 {seed_seconds:.1f}s")
    # 封存：checkpoint 后记录指纹；旁边放一个哨兵文件
    conn = sqlite3.connect(ledger_path)
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()
    sentinel = os.path.join(EVIDENCE_DIR, "sentinel.txt")
    with open(sentinel, "w") as fh:
        fh.write("do-not-touch\n")
    source_fingerprint_before = sha256_file(ledger_path)
    sentinel_before = sha256_file(sentinel)
    results["scale"] = {
        "ledger_records": N_TELEMETRY + N_AUX + N_OTHER,
        "selected_records_expected": N_TELEMETRY + N_AUX,
        "seed_seconds": round(seed_seconds, 2),
        "source_sha256_before": source_fingerprint_before,
    }

    export_dir = os.path.join(EVIDENCE_DIR, "export")
    print("   运行 export …")
    t0 = time.time()
    code, out = run_cli(["export", ledger_path, "--descriptor", desc_path,
                         "--config", cfg_path, "--out", export_dir])
    export_seconds = time.time() - t0
    peak_kb = peak_rss_kb_of_children()
    assert code == 0, out
    summary = json.loads(out)
    results["scale"]["export"] = {
        "exit_code": code,
        "seconds": round(export_seconds, 2),
        "peak_rss_mb_of_export_process": round(peak_kb / 1024, 1),
        "selected_records": summary["selected_records"],
        "modified_records": summary["modified_records"],
        "path_hits": summary["path_hits"],
    }
    print(f"   export 耗时 {export_seconds:.1f}s，"
          f"子进程峰值内存 {peak_kb / 1024:.1f} MB")
    print(f"   选中 {summary['selected_records']}，改动 {summary['modified_records']}")
    assert summary["selected_records"] == N_TELEMETRY + N_AUX
    assert summary["modified_records"] == N_TELEMETRY + N_AUX

    count, first, last = first_last_records(
        os.path.join(export_dir, "messages.jsonl"))
    results["scale"]["messages_file_records"] = count
    results["scale"]["first_record"] = first
    results["scale"]["last_record"] = last
    print(f"   输出首条记录: {json.dumps(first, ensure_ascii=False)}")
    print(f"   输出末条记录: {json.dumps(last, ensure_ascii=False)}")

    print("   运行 inspect …")
    code, out = run_cli(["inspect", export_dir])
    assert code == 0, out
    results["scale"]["inspect"] = json.loads(out)
    print(f"   inspect ok: {results['scale']['inspect']['checked_records']} 条核实")

    # 移开源库与配置后 inspect 仍能核实
    print("   移开源库与配置后再次 inspect …")
    os.rename(ledger_path, ledger_path + ".moved")
    os.rename(cfg_path, cfg_path + ".moved")
    code, out = run_cli(["inspect", export_dir])
    os.rename(ledger_path + ".moved", ledger_path)
    os.rename(cfg_path + ".moved", cfg_path)
    assert code == 0, out
    results["scale"]["inspect_without_source"] = json.loads(out)["ok"]
    print("   来源缺席时 inspect 仍通过")

    # ---- 3. 重新接入空库 ----------------------------------------------------
    print("== 3. 导出经读取接口重新接入空库")
    new_db = os.path.join(EVIDENCE_DIR, "reingest.db")
    results["reingest"] = reingest_and_verify(export_dir, new_db)
    print(f"   重新入账 {results['reingest']['reingested_publishes']} 条，"
          f"字段不一致 {results['reingest']['field_mismatches']} 处")
    assert results["reingest"]["ok"]

    # ---- 4. 拒绝证据 --------------------------------------------------------
    print("== 4. 拒绝证据（小规模逐例）")
    results["rejections"] = rejection_cases(
        os.path.join(EVIDENCE_DIR, "rejections"), descriptor_bytes)
    for c in results["rejections"]:
        err = c["error_json"]
        cat = err.get("error", {}).get("category") if isinstance(err, dict) else "?"
        print(f"   {c['case']}: exit={c['exit_code']} category={cat} "
              f"ok={c['ok']}")
    assert all(c["ok"] for c in results["rejections"])

    # 最后一条损坏（在规模账本副本上）：整批失败，不留完整结果
    print("== 4b. 最后一条损坏：整批拒绝")
    corrupt_db = os.path.join(EVIDENCE_DIR, "corrupt_last.db")
    shutil.copyfile(ledger_path, corrupt_db)
    conn = sqlite3.connect(corrupt_db)
    conn.execute(
        "INSERT INTO ledger(session_id, topic, payload, qos, retain, dup,"
        " message_id, ingested_at) VALUES('collector-9','factory/telemetry',"
        " X'1A01FF',0,0,0,NULL,0)")
    conn.commit()
    conn.close()
    corrupt_fp = sha256_file(corrupt_db)
    code, out = run_cli(["export", corrupt_db, "--descriptor", desc_path,
                         "--config", cfg_path, "--out",
                         os.path.join(EVIDENCE_DIR, "export_corrupt")])
    insp_code, _ = run_cli(["inspect", os.path.join(EVIDENCE_DIR,
                                                    "export_corrupt")])
    results["corrupt_last"] = {
        "exit_code": code,
        "error": json.loads(out) if out.strip().startswith("{") else out,
        "output_dir_left": os.path.exists(os.path.join(EVIDENCE_DIR,
                                                       "export_corrupt")),
        "inspect_exit_code": insp_code,
        "source_untouched": sha256_file(corrupt_db) == corrupt_fp,
        "ok": code == 2 and insp_code != 0
              and not os.path.exists(os.path.join(EVIDENCE_DIR, "export_corrupt")),
    }
    print(f"   exit={code}，输出目录残留={results['corrupt_last']['output_dir_left']}，"
          f"inspect exit={insp_code}")
    assert results["corrupt_last"]["ok"]

    # 目标已存在
    code, out = run_cli(["export", ledger_path, "--descriptor", desc_path,
                         "--config", cfg_path, "--out", export_dir])
    results["target_exists"] = {"exit_code": code, "ok": code == 4}
    print(f"== 4c. 目标已存在: exit={code}")
    assert code == 4

    # ---- 5. 来源与既有文件未动 ----------------------------------------------
    results["source_integrity"] = {
        "ledger_unchanged": sha256_file(ledger_path) == source_fingerprint_before,
        "sentinel_unchanged": sha256_file(sentinel) == sentinel_before,
        "no_wal_shm": not os.path.exists(ledger_path + "-wal")
                      and not os.path.exists(ledger_path + "-shm"),
    }
    print(f"== 5. 来源未动: {results['source_integrity']}")
    assert all(results["source_integrity"].values())

    results["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    results["ok"] = True
    with open(os.path.join(EVIDENCE_DIR, "results.json"), "w",
              encoding="utf-8") as fh:
        json.dump(results, fh, ensure_ascii=False, indent=1)
    print(f"\n证据写入 {os.path.join(EVIDENCE_DIR, 'results.json')}")
    print("演示全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
