"""范围准入引擎：多允许分支、拒绝子树、零层 #、未见名称、深层、字节上限、非枚举核对。"""

import json
import unittest

from offline_mqtt.admission import (
    AdmissionEngine,
    AdmissionInputError,
    AdmissionResourceLimit,
)
from offline_mqtt.namespace import MAX_TOPIC_BYTES
from offline_mqtt.policy import parse_policy
from offline_mqtt.topic import topic_matches
from tests._enumerator import assert_scope_matches_enumeration


def build(rules, principals=None):
    principals = principals if principals is not None else [
        {"principal_id": "team-plant", "groups": ["plant-team"]},
        {"principal_id": "team-payroll", "groups": []},
    ]
    return AdmissionEngine(parse_policy(
        json.dumps({"version": 1, "principals": principals, "rules": rules})
    ))


def allow(rid, pid, flt, subject_type="principal"):
    return {"rule_id": rid, "subject_type": subject_type, "subject_id": pid,
            "effect": "allow", "filter": flt}


def deny(rid, pid, flt, subject_type="principal"):
    return {"rule_id": rid, "subject_type": subject_type, "subject_id": pid,
            "effect": "deny", "filter": flt}


class DecideTopicTest(unittest.TestCase):
    def setUp(self):
        self.engine = build([
            allow("a1", "team-plant", "plant/#"),
            deny("d1", "plant-team", "plant/private/#", subject_type="group"),
            allow("a2", "team-payroll", "payroll/#"),
        ])

    def test_allow_deny_default_rule_ids(self):
        d = self.engine.decide_topic("team-plant", "plant/temp")
        self.assertTrue(d.allowed)
        self.assertEqual(d.allow_rule_ids, ("a1",))
        self.assertEqual(d.deny_rule_ids, ())
        self.assertIsNone(d.reason)

        d = self.engine.decide_topic("team-plant", "plant/private/salary")
        self.assertFalse(d.allowed)
        self.assertEqual(d.deny_rule_ids, ("d1",))
        self.assertEqual(d.allow_rule_ids, ("a1",))  # allow 也命中，但 deny 优先
        self.assertEqual(d.reason, "DENY_RULE_MATCHED")

        d = self.engine.decide_topic("team-plant", "payroll/tax")
        self.assertFalse(d.allowed)
        self.assertEqual(d.allow_rule_ids, ())
        self.assertEqual(d.reason, "NO_ALLOW_RULE_MATCHED")

    def test_policy_summary_attached(self):
        d = self.engine.decide_topic("team-payroll", "payroll/x")
        self.assertEqual(d.policy_summary["principal_count"], 2)
        self.assertEqual(len(d.policy_summary["policy_digest_sha256"]), 64)

    def test_unknown_principal_and_bad_topic_are_input_errors(self):
        with self.assertRaises(AdmissionInputError) as ctx:
            self.engine.decide_topic("ghost", "plant/x")
        self.assertEqual(ctx.exception.errors[0]["reason"], "UNKNOWN_PRINCIPAL")
        with self.assertRaises(AdmissionInputError) as ctx:
            self.engine.decide_topic("team-plant", "Bad/Topic")
        self.assertEqual(ctx.exception.errors[0]["reason"], "INVALID_LEVEL_CHAR")
        with self.assertRaises(AdmissionInputError):
            self.engine.decide_topic("", "plant/x")


class DecideScopeTest(unittest.TestCase):
    def test_two_allow_branches_jointly_cover_request(self):
        # 单独任一条都不足以覆盖 plant/#，两条合起来完整覆盖。
        engine = build([
            allow("a-exact", "team-plant", "plant"),
            allow("a-sub", "team-plant", "plant/+/#"),
        ])
        decision = engine.decide_scope("team-plant", "plant/#")
        self.assertTrue(decision.safe)
        self.assertIsNone(decision.witness)

    def test_single_branch_is_not_enough(self):
        engine = build([allow("a-exact", "team-plant", "plant")])
        decision = engine.decide_scope("team-plant", "plant/#")
        self.assertFalse(decision.safe)
        # 零层 plant 本身是允许的，反例必须是更深的真实主题
        self.assertTrue(decision.witness.topic.startswith("plant/"))
        self.assertEqual(decision.witness.reason, "NO_ALLOW_RULE_MATCHED")
        self.assertTrue(topic_matches("plant/#", decision.witness.topic))

    def test_deny_subtree_makes_union_unsafe_with_real_witness(self):
        engine = build([
            allow("a-exact", "team-plant", "plant"),
            allow("a-sub", "team-plant", "plant/+/#"),
            deny("d-priv", "plant-team", "plant/private/#", subject_type="group"),
        ])
        decision = engine.decide_scope("team-plant", "plant/#")
        self.assertFalse(decision.safe)
        witness = decision.witness.topic
        # 业务可复核：属于原请求 + 具体判断确实拒绝
        self.assertTrue(topic_matches("plant/#", witness))
        cross = engine.decide_topic("team-plant", witness)
        self.assertFalse(cross.allowed)
        self.assertEqual(cross.deny_rule_ids, ("d-priv",))
        self.assertEqual(decision.witness.deny_rule_ids, ("d-priv",))
        self.assertEqual(decision.witness.reason, "DENY_RULE_MATCHED")

    def test_other_team_sees_different_answer(self):
        engine = build([
            allow("a-exact", "team-plant", "plant"),
            allow("a-sub", "team-plant", "plant/+/#"),
            allow("pay", "team-payroll", "payroll/#"),
        ])
        # plant 团队对 plant/# 安全；payroll 团队对同一请求无任何覆盖
        self.assertTrue(engine.decide_scope("team-plant", "plant/#").safe)
        payroll = engine.decide_scope("team-payroll", "plant/#")
        self.assertFalse(payroll.safe)
        self.assertTrue(payroll.witness.topic.startswith("plant"))
        self.assertEqual(payroll.witness.reason, "NO_ALLOW_RULE_MATCHED")
        # 反向：payroll 自己的命名空间安全
        self.assertTrue(engine.decide_scope("team-payroll", "payroll/#").safe)

    def test_hash_zero_level_match_includes_bare_prefix(self):
        # plant/# 包含 plant 本身（# 占零层）。
        engine = build([allow("a", "team-plant", "plant/#")])
        self.assertTrue(engine.decide_topic("team-plant", "plant").allowed)
        self.assertTrue(engine.decide_scope("team-plant", "plant/#").safe)
        # 只有 plant（无 #）被允许时，plant/# 不安全，且 plant 本身不是反例
        engine2 = build([allow("a", "team-plant", "plant")])
        d = engine2.decide_scope("team-plant", "plant/#")
        self.assertFalse(d.safe)
        self.assertNotEqual(d.witness.topic, "plant")
        self.assertTrue(topic_matches("plant/#", d.witness.topic))

    def test_bare_hash_request_against_empty_policy(self):
        engine = build([])
        d = engine.decide_scope("team-plant", "#")
        self.assertFalse(d.safe)
        self.assertEqual(d.witness.reason, "NO_ALLOW_RULE_MATCHED")
        cross = engine.decide_topic("team-plant", d.witness.topic)
        self.assertFalse(cross.allowed)

    def test_unseen_name_not_among_policy_literals(self):
        # 只允许一个具体字面层；请求 plant/+ 的另一个层值是策略里从未出现的名称。
        engine = build([allow("a", "team-plant", "plant/temp")])
        d = engine.decide_scope("team-plant", "plant/+")
        self.assertFalse(d.safe)
        witness = d.witness.topic
        self.assertTrue(topic_matches("plant/+", witness))
        self.assertNotEqual(witness, "plant/temp")  # 戳穿“只试已有单词”
        self.assertFalse(engine.decide_topic("team-plant", witness).allowed)

    def test_deep_only_unauthorized_branch(self):
        # 浅层全部允许，差异只在第 3 层才出现的拒绝子树。
        engine = build([
            allow("a", "team-plant", "plant/#"),
            deny("d", "team-plant", "plant/x/y/#"),
        ])
        d = engine.decide_scope("team-plant", "plant/#")
        self.assertFalse(d.safe)
        self.assertGreaterEqual(d.witness.topic.count("/"), 2)  # 至少 plant/x/y
        self.assertTrue(d.witness.topic.startswith("plant/x/y"))
        self.assertEqual(d.witness.deny_rule_ids, ("d",))

    def test_plus_wildcard_semantics_in_scope(self):
        engine = build([allow("a", "team-plant", "plant/+/temp")])
        self.assertTrue(engine.decide_scope("team-plant", "plant/+/temp").safe)
        # plant/+/temp 不覆盖两层中间值
        d = engine.decide_scope("team-plant", "plant/+/+/temp")
        self.assertFalse(d.safe)
        self.assertEqual(d.witness.topic.count("/"), 3)

    def test_witness_always_legal_and_within_byte_cap(self):
        engine = build([allow("a", "team-plant", "plant/#"),
                        deny("d", "team-plant", "plant/secret/#")])
        d = engine.decide_scope("team-plant", "#")
        self.assertFalse(d.safe)
        witness = d.witness.topic
        self.assertLessEqual(len(witness.encode("utf-8")), MAX_TOPIC_BYTES)
        # 复核：合法、属于请求、确实被拒
        from offline_mqtt.namespace import validate_namespace_topic
        validate_namespace_topic(witness)
        self.assertTrue(topic_matches("#", witness))
        self.assertFalse(engine.decide_topic("team-plant", witness).allowed)

    def test_byte_budget_prune_no_overlength_witness(self):
        # 极长字面层：任何到达它的具体主题都贴近/超过字节上限。
        # 可达时 witness 仍必须 <= 65535；构造一条最短匹配恰在上限内的 deny。
        big = "z" * 65530  # q/ + big = 65532 字节，合法且可达
        engine = build([
            allow("a", "team-plant", "q/#"),
            deny("d", "team-plant", "q/" + big),
        ])
        d = engine.decide_scope("team-plant", "q/#")
        self.assertFalse(d.safe)
        self.assertLessEqual(len(d.witness.topic.encode()), MAX_TOPIC_BYTES)

    def test_illegal_request_filter_is_input_error(self):
        engine = build([allow("a", "team-plant", "plant/#")])
        for bad in ("plant//", "$SYS/x", "Plant/#", "a/#/b"):
            with self.subTest(bad=bad):
                with self.assertRaises(AdmissionInputError):
                    engine.decide_scope("team-plant", bad)

    def test_deep_path_80_levels(self):
        levels = [f"level{i:02d}" for i in range(80)]
        exact = "/".join(levels)
        engine = build([allow("deep", "team-plant", exact)])
        self.assertTrue(engine.decide_scope("team-plant", exact).safe)
        # 把最后一层换成 +：字面层允许无法覆盖通配位置
        plus_request = "/".join(levels[:-1] + ["+"])
        d = engine.decide_scope("team-plant", plus_request)
        self.assertFalse(d.safe)
        self.assertEqual(d.witness.topic.count("/") + 1, 80)

    def test_200_rules_scale_terminates_without_enumeration(self):
        rules = [
            allow(f"r{i:03d}", "team-plant", f"prefix{i:03d}/#")
            for i in range(200)
        ]
        engine = build(rules)
        # 命中其中一个前缀：安全
        self.assertTrue(engine.decide_scope("team-plant", "prefix077/#").safe)
        # 未被任何前缀覆盖：给出真实反例
        d = engine.decide_scope("team-plant", "prefix077/extra/#")
        self.assertTrue(d.safe)  # prefix077/# 已覆盖其子树
        d2 = engine.decide_scope("team-plant", "unauthorized/root/#")
        self.assertFalse(d2.safe)
        self.assertTrue(d2.witness.topic.startswith("unauthorized"))
        # 探索量是有限自动机状态数，与“可能主题的无限集合”无关
        self.assertGreater(d2.product_states_explored, 0)
        self.assertLess(d2.product_states_explored, 100_000)

    def test_resource_limit_reported_not_silently_safe(self):
        # 超过过滤器深度上限：明确报资源不足，而不是 safe 或普通越权。
        deep_filter = "/".join("a" for _ in range(101))
        # 规则过滤器超深 -> 构造引擎即报告资源不足
        with self.assertRaises(AdmissionResourceLimit):
            build([allow("deep", "team-plant", deep_filter)])
        # 请求过滤器超深（规则本身合法）-> 范围裁定时报告资源不足
        engine = build([allow("ok", "team-plant", "a/#")])
        with self.assertRaises(AdmissionResourceLimit):
            engine.decide_scope("team-plant", deep_filter)


class EnumeratorCrossCheckTest(unittest.TestCase):
    """用小字母表/小深度的独立全枚举核对正式引擎的若干完整结果。"""

    def setUp(self):
        # 所有字面层都落在小字母表 {a,b} 内，枚举才有完整约束力。
        self.engine = build([
            allow("a1", "team-plant", "a/#"),
            allow("a2", "team-plant", "a/b"),       # 与 a/# 重叠也无妨
            deny("d1", "team-plant", "a/b/b/#"),
            allow("pay", "team-payroll", "b/#"),
        ])

    def test_scope_decisions_match_brute_force(self):
        requests = [
            "#", "a/#", "b/#", "a/+/b", "a/b/#", "a/b/b/#",
            "a/+", "+", "a/b", "b/a/#", "a/a/a",
        ]
        for principal in ("team-plant", "team-payroll"):
            for request in requests:
                with self.subTest(principal=principal, request=request):
                    assert_scope_matches_enumeration(
                        self, self.engine, principal, request
                    )

    def test_enumerator_independence_is_supplementary(self):
        # 枚举器只在 3 层、2 字母的小空间工作；正式引擎能处理它无法覆盖的深层。
        from tests._enumerator import enumerate_topics
        topics = enumerate_topics()
        self.assertTrue(all(t.count("/") + 1 <= 3 for t in topics))
        self.assertTrue(all(set(t.replace("/", "")) <= {"a", "b"} for t in topics))
        self.assertGreater(len(topics), 0)


if __name__ == "__main__":
    unittest.main()
