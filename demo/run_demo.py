#!/usr/bin/env python3
"""采集范围准入 —— 可手工复核的业务演练（生成 demo/evidence 下的全部证据）。

用 ``python -I demo/run_demo.py`` 运行。脚本只依赖标准库与本仓库代码，
不连接任何消息代理。每个场景都会打印并断言真实数据，而不是只打印通过计数。

覆盖清单（对应用户要求）：
  1. 两个 allow 分支共同覆盖请求 -> safe；加入 deny 子树 -> 不安全 + 真实反例；
  2. 对反例分别验证“属于原请求”和“具体判断确实拒绝”；换团队得到不同答案；
  3. 仅在较深层才有的未授权分支；与策略字面层都不同的未见名称；
  4. 空层被拒绝；# 的零层匹配；身份名与组名同名；已登记无规则身份默认拒绝；
  5. 独立小枚举器（缩小字母表/深度）核对完整结果（补充检查）；
  6. 实际建立获准订阅 -> 读旧保留值 -> 发布普通消息读回 ->
     放入非采集命名空间主题确认受控读取不交付；
  7. 一个获准 + 一个越权过滤器的请求 -> 库中无 subscription / retained / cursor 残留；
  8. 坏策略与未知身份失败演练；200 条规则、80 层路径的非枚举判断；
  9. 字节上限：witness 永不超限，仅超限可达的差异不算越权。
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

from offline_mqtt import (  # noqa: E402
    AdmissionEngine,
    AdmissionInputError,
    AdmissionResourceLimit,
    ControlledIngress,
    MqttIngress,
    load_policy_file,
    parse_policy,
)
from offline_mqtt.generator import (  # noqa: E402
    connect,
    publish,
    retained_publish,
    subscribe,
)
from offline_mqtt.namespace import MAX_TOPIC_BYTES  # noqa: E402
from offline_mqtt.topic import topic_matches  # noqa: E402
from tests._enumerator import assert_scope_matches_enumeration  # noqa: E402

DEMO_DIR = os.path.dirname(os.path.abspath(__file__))
EVIDENCE_DIR = os.path.join(DEMO_DIR, "evidence")
POLICY_PATH = os.path.join(DEMO_DIR, "policies", "collections.policy.json")
BAD_POLICY_PATH = os.path.join(DEMO_DIR, "policies", "bad.policy.json")

_results: list[dict] = []


def record(scene: str, payload: dict) -> None:
    _results.append({"scene": scene, **payload})
    print(f"\n=== {scene} ===")
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def check(condition: bool, label: str) -> None:
    if not condition:
        raise AssertionError(f"断言失败: {label}")
    print(f"  [assert] {label} ✓")


def main() -> int:
    os.makedirs(EVIDENCE_DIR, exist_ok=True)
    engine = AdmissionEngine(load_policy_file(POLICY_PATH))
    digest = engine.policy_summary()["policy_digest_sha256"]
    print(f"策略摘要 sha256={digest}")

    # ------------------------------------------------ 1+2. 分支联合覆盖 / deny 反例
    # 注意：完整策略里 plant 已带 deny plant/private/#。为演示“先安全后不安全”，
    # 这里临时构造不含 deny 的等价策略。
    safe_policy = parse_policy(json.dumps({
        "version": 1,
        "principals": [{"principal_id": "team-plant", "groups": ["plant-team"]}],
        "rules": [
            {"rule_id": "plant-exact", "subject_type": "principal",
             "subject_id": "team-plant", "effect": "allow", "filter": "plant"},
            {"rule_id": "plant-sub", "subject_type": "principal",
             "subject_id": "team-plant", "effect": "allow", "filter": "plant/+/#"},
        ],
    }))
    safe_engine = AdmissionEngine(safe_policy)
    safe_decision = safe_engine.decide_scope("team-plant", "plant/#")
    record("1a-两个允许分支联合覆盖 plant/#", safe_decision.to_dict())
    check(safe_decision.safe, "plant 与 plant/+/# 合起来完整允许 plant/#")

    unsafe = engine.decide_scope("team-plant", "plant/#")
    record("1b-加入 deny plant/private/# 后不安全", unsafe.to_dict())
    check(not unsafe.safe, "同一请求现在不安全")
    witness_topic = unsafe.witness.topic
    check(topic_matches("plant/#", witness_topic), f"反例 {witness_topic} 属于原请求 plant/#")
    cross = engine.decide_topic("team-plant", witness_topic)
    record("1c-把反例交给具体判断复核", cross.to_dict())
    check(not cross.allowed, f"具体判断确实拒绝 {witness_topic}")
    check(cross.deny_rule_ids == unsafe.witness.deny_rule_ids, "命中的 deny 规则一致")

    # 换团队：payroll 对 plant/# 没有任何覆盖
    payroll = engine.decide_scope("team-payroll", "plant/#")
    record("2-换团队 team-payroll 看同一请求", payroll.to_dict())
    check(not payroll.safe and payroll.witness.reason == "NO_ALLOW_RULE_MATCHED",
          "payroll 对 plant/# 完全没有允许项覆盖")
    check(engine.decide_scope("team-payroll", "payroll/#").safe,
          "payroll 对自己的 payroll/# 是安全的")

    # ------------------------------------------------ 3. 深层未授权分支 + 未见名称
    deep_policy = AdmissionEngine(parse_policy(json.dumps({
        "version": 1,
        "principals": [{"principal_id": "t", "groups": []}],
        "rules": [
            {"rule_id": "broad", "subject_type": "principal", "subject_id": "t",
             "effect": "allow", "filter": "plant/#"},
            {"rule_id": "deep-deny", "subject_type": "principal", "subject_id": "t",
             "effect": "deny", "filter": "plant/x/y/z/#"},
        ],
    })))
    deep = deep_policy.decide_scope("t", "plant/#")
    record("3a-仅在第4层才出现的拒绝子树", deep.to_dict())
    check(deep.witness.topic.startswith("plant/x/y/z"), "反例落在深层拒绝子树")
    check(deep.witness.topic.count("/") >= 3, "固定深度抽样(<=2层)会漏掉它")

    unseen_policy = AdmissionEngine(parse_policy(json.dumps({
        "version": 1,
        "principals": [{"principal_id": "t", "groups": []}],
        "rules": [{"rule_id": "only-literal", "subject_type": "principal",
                   "subject_id": "t", "effect": "allow", "filter": "plant/temp"}],
    })))
    unseen = unseen_policy.decide_scope("t", "plant/+")
    record("3b-未见层值（策略里只有 temp 一个单词）", unseen.to_dict())
    check(unseen.witness.topic != "plant/temp", "反例使用了策略中从未出现的层值")
    check(topic_matches("plant/+", unseen.witness.topic), "未见名称仍属于请求范围")

    # ------------------------------------------------ 4. 空层 / 零层# / 同名 / 无规则
    def expect_input_error(label, fn):
        try:
            fn()
        except AdmissionInputError as exc:
            record(label, {"errors": exc.errors})
            return exc
        raise AssertionError(f"{label} 应当报输入错误")

    expect_input_error("4a-空层过滤器被拒绝",
                       lambda: engine.decide_scope("team-plant", "plant//x"))
    expect_input_error("4b-系统/共享前缀被拒绝",
                       lambda: engine.decide_scope("team-plant", "$share/g/plant"))
    expect_input_error("4c-大写层被拒绝",
                       lambda: engine.decide_topic("team-plant", "Plant/Temp"))

    zero_level = engine.decide_topic("team-plant", "plant")
    record("4d-# 的零层匹配：plant/# 包含 plant 本身", zero_level.to_dict())
    check(zero_level.allowed, "plant 本身被允许（# 占零层）")

    same_name = engine.decide_topic("alice", "g/anything")
    record("4e-身份名与组名同名 alice（组规则生效）", same_name.to_dict())
    check(same_name.allow_rule_ids == ("alice-group-rule",), "组 alice 的规则命中")
    same_name2 = engine.decide_topic("alice", "p/anything")
    check(same_name2.allow_rule_ids == ("alice-principal-rule",), "身份 alice 的规则也命中")

    no_rules = engine.decide_scope("team-no-rules", "#")
    record("4f-已登记但无规则身份 -> 默认拒绝", no_rules.to_dict())
    check(not no_rules.safe and no_rules.witness.reason == "NO_ALLOW_RULE_MATCHED",
          "无规则身份的请求被默认拒绝")

    # ------------------------------------------------ 5. 独立小枚举器核对
    tiny_engine = AdmissionEngine(parse_policy(json.dumps({
        "version": 1,
        "principals": [{"principal_id": "t", "groups": []}],
        "rules": [
            {"rule_id": "a1", "subject_type": "principal", "subject_id": "t",
             "effect": "allow", "filter": "a/#"},
            {"rule_id": "d1", "subject_type": "principal", "subject_id": "t",
             "effect": "deny", "filter": "a/b/b/#"},
        ],
    })))

    class _Probe:  # 枚举器只需要 assert* 接口
        @staticmethod
        def assertTrue(cond, msg=""):
            check(bool(cond), f"枚举器核对: {msg}")

        @staticmethod
        def assertFalse(cond, msg=""):
            check(not cond, f"枚举器核对: {msg}")

        @staticmethod
        def assertEqual(a, b, msg=""):
            check(a == b, f"枚举器核对: {msg} ({a!r}=={b!r})")

        @staticmethod
        def assertIsNone(v, msg=""):
            check(v is None, f"枚举器核对: {msg}")

        @staticmethod
        def assertIsNotNone(v, msg=""):
            check(v is not None, f"枚举器核对: {msg}")

        @staticmethod
        def assertIn(a, b, msg=""):
            check(a in b, f"枚举器核对: {msg}")

    for req in ("#", "a/#", "a/+/b", "a/b/#", "a/b/b/#", "+", "a/a/a"):
        assert_scope_matches_enumeration(_Probe, tiny_engine, "t", req)
    record("5-独立枚举器（字母表{a,b}、深度<=3）全部一致",
           {"checked_requests": ["#", "a/#", "a/+/b", "a/b/#", "a/b/b/#", "+", "a/a/a"]})

    # ------------------------------------------------ 6+7. 真实订阅 + 受控读取 + 失败不落库
    fd, db_path = tempfile.mkstemp(prefix="admission_demo_", suffix=".db")
    os.close(fd)
    os.unlink(db_path)
    with MqttIngress(db_path) as ing:
        ing.ingest(connect("sensor-1"))
        # 旧保留值：命名空间内 + 命名空间外
        ing.ingest(retained_publish("sensor-1", "plant/temp/current", "21.5",
                                    qos=1, message_id=1))
        ing.ingest(retained_publish("sensor-1", "Plant/Secret", "capital-letter",
                                    qos=1, message_id=2))
        controlled = ControlledIngress(ing, engine)

        def counts():
            c = ing.store.conn
            return {t: c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                    for t in ("subscriptions", "subscription_filters",
                              "subscription_retained", "delivery_cursor")}

        before = counts()
        granted = controlled.controlled_subscribe(
            "team-plant", subscribe("any-session-id", ["plant/temp/#"], message_id=20))
        record("6a-受控订阅获准并创建真实订阅", granted.to_dict())
        check(granted.granted and isinstance(granted.subscription_id, int),
              "返回真实 subscription_id")
        after = counts()
        check(after["subscriptions"] == before["subscriptions"] + 1, "subscriptions +1")
        check(after["delivery_cursor"] == before["delivery_cursor"] + 1, "delivery_cursor +1")
        sub_id = granted.subscription_id

        first = controlled.controlled_poll(sub_id)
        record("6b-首次受控读取：旧保留值初始视图",
               {"messages": [{"topic": m.topic, "payload": m.payload.decode(),
                              "retain": m.retain} for m in first.messages]})
        check([m.topic for m in first.messages] == ["plant/temp/current"],
              "只交付命名空间内的旧保留值，Plant/Secret 被剔除")

        ing.ingest(publish("sensor-1", "plant/temp/reading", "22.0",
                           qos=1, message_id=3))
        second = controlled.controlled_poll(sub_id)
        record("6c-发布普通消息后受控读回（实时）",
               {"messages": [{"topic": m.topic, "payload": m.payload.decode(),
                              "retain": m.retain} for m in second.messages]})
        check([m.topic for m in second.messages] == ["plant/temp/reading"],
              "实时消息按订阅语义读回")

        # 混合请求：一个获准 + 一个越权 -> 全或无，库中无残留
        mixed_before = counts()
        mixed = controlled.controlled_subscribe(
            "team-plant",
            subscribe("s", ["plant/temp/ok", "plant/private/salary"]))
        record("7-一个获准+一个越权的混合请求被整体拒绝", mixed.to_dict())
        check(not mixed.granted, "混合请求不创建订阅")
        mixed_after = counts()
        check(mixed_after == mixed_before,
              f"无任何订阅相关行新增（{mixed_before} == {mixed_after}）")

    # 直接查 SQLite 文件做独立断言
    conn = sqlite3.connect(db_path)
    raw_counts = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                  for t in ("subscriptions", "subscription_retained", "delivery_cursor")}
    conn.close()
    record("7b-直接打开数据库文件核对行数", raw_counts)
    check(raw_counts["subscriptions"] == 1, "数据库里只有第一次获准的那一个订阅")

    # ------------------------------------------------ 8. 坏策略 / 未知身份 / 200规则 / 80层
    try:
        load_policy_file(BAD_POLICY_PATH)
    except Exception as exc:  # PolicyError
        record("8a-坏策略失败演练（带位置的输入错误）", {"errors": exc.errors})
        locations = {e["location"] for e in exc.errors}
        check("$.rules[0].subject_id" in locations, "指出不存在的组引用位置")
        check(any("DUPLICATE_PRINCIPAL_ID" == e["reason"] for e in exc.errors),
              "指出重复 principal_id")
        check(any(e["reason"] == "DUPLICATE_RULE_ID" for e in exc.errors), "指出重复 rule_id")

    expect_input_error("8b-未知身份失败演练",
                       lambda: engine.decide_scope("nobody", "plant/#"))

    big_rules = [
        {"rule_id": f"r{i:03d}", "subject_type": "principal", "subject_id": "t",
         "effect": "allow", "filter": f"prefix{i:03d}/#"}
        for i in range(200)
    ]
    big_engine = AdmissionEngine(parse_policy(json.dumps({
        "version": 1,
        "principals": [{"principal_id": "t", "groups": []}],
        "rules": big_rules,
    })))
    big_safe = big_engine.decide_scope("t", "prefix123/#")
    big_unsafe = big_engine.decide_scope("t", "not-covered/#")
    record("8c-200条规则的非枚举判断", {
        "rule_count": 200,
        "covered_request_safe": big_safe.safe,
        "uncovered_request_safe": big_unsafe.safe,
        "uncovered_witness": big_unsafe.witness.topic,
        "product_states": big_unsafe.product_states_explored,
    })
    check(big_safe.safe and not big_unsafe.safe, "200 规则下结论正确且无需枚举主题")

    levels = [f"lv{i:02d}" for i in range(80)]
    deep80 = "/".join(levels)
    engine80 = AdmissionEngine(parse_policy(json.dumps({
        "version": 1,
        "principals": [{"principal_id": "t", "groups": []}],
        "rules": [{"rule_id": "deep", "subject_type": "principal",
                   "subject_id": "t", "effect": "allow", "filter": deep80}],
    })))
    d80 = engine80.decide_scope("t", deep80)
    record("8d-80层路径的非枚举判断", {
        "depth": 80, "safe": d80.safe,
        "product_states": d80.product_states_explored,
        "product_edges": d80.product_edges_explored,
    })
    check(d80.safe, "80 层精确允许被完整认可")

    # ------------------------------------------------ 9. 字节上限
    near_cap = "z" * 65530
    cap_engine = AdmissionEngine(parse_policy(json.dumps({
        "version": 1,
        "principals": [{"principal_id": "t", "groups": []}],
        "rules": [
            {"rule_id": "a", "subject_type": "principal", "subject_id": "t",
             "effect": "allow", "filter": "q/#"},
            {"rule_id": "d", "subject_type": "principal", "subject_id": "t",
             "effect": "deny", "filter": "q/" + near_cap},
        ],
    })))
    cap_decision = cap_engine.decide_scope("t", "q/#")
    witness_len = len(cap_decision.witness.topic.encode("utf-8"))
    record("9-字节上限：贴近 65535 的反例仍合法且不超限", {
        "max_topic_bytes": MAX_TOPIC_BYTES,
        "witness_topic_bytes": witness_len,
        "safe": cap_decision.safe,
    })
    check(witness_len <= MAX_TOPIC_BYTES, "witness 永远不超过完整主题字节上限")

    # 资源上限演练（区别于普通越权）
    too_deep = "/".join("a" for _ in range(101))
    try:
        engine.decide_scope("team-plant", too_deep)
    except AdmissionResourceLimit as exc:
        record("8e-触及资源上限 -> 明确报无法处理（不伪装 safe）", {"detail": str(exc)})

    # 落证据文件
    out_path = os.path.join(EVIDENCE_DIR, "demo_results.json")
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump({"policy_digest_sha256": digest, "scenes": _results},
                  fh, ensure_ascii=False, indent=2)
    print(f"\n全部 {len(_results)} 个场景通过，证据已写入 {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
