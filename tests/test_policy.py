"""策略解析：主体类型显式区分、组解析、带位置的输入错误、摘要稳定性。"""

import json
import unittest

from offline_mqtt.policy import PolicyError, parse_policy


def policy_text(rules, principals=None, *, version=1) -> str:
    principals = principals if principals is not None else [
        {"principal_id": "team-plant", "groups": ["plant-team"]},
        {"principal_id": "team-payroll", "groups": []},
    ]
    return json.dumps({"version": version, "principals": principals, "rules": rules})


class PolicyParseTest(unittest.TestCase):
    def test_valid_policy_and_digest_stable(self):
        text = policy_text([
            {"rule_id": "r1", "subject_type": "principal",
             "subject_id": "team-plant", "effect": "allow", "filter": "plant/#"},
        ])
        p1 = parse_policy(text)
        p2 = parse_policy(json.loads(text))
        self.assertEqual(p1.digest_sha256, p2.digest_sha256)
        self.assertEqual(len(p1.digest_sha256), 64)  # sha256 hex
        self.assertEqual(p1.summary()["rule_count"], 1)

    def test_effective_rules_merge_principal_and_groups(self):
        policy = parse_policy(policy_text([
            {"rule_id": "direct", "subject_type": "principal",
             "subject_id": "team-plant", "effect": "allow", "filter": "plant/#"},
            {"rule_id": "via-group", "subject_type": "group",
             "subject_id": "plant-team", "effect": "deny", "filter": "plant/private/#"},
            {"rule_id": "other-team", "subject_type": "principal",
             "subject_id": "team-payroll", "effect": "allow", "filter": "payroll/#"},
        ]))
        ids = {r.rule_id for r in policy.effective_rules("team-plant")}
        self.assertEqual(ids, {"direct", "via-group"})
        # payroll 不属于 plant-team，看不到组规则
        self.assertEqual({r.rule_id for r in policy.effective_rules("team-payroll")},
                         {"other-team"})

    def test_same_name_principal_and_group_are_distinct(self):
        policy = parse_policy(json.dumps({
            "version": 1,
            "principals": [{"principal_id": "alice", "groups": ["alice"]}],
            "rules": [
                {"rule_id": "g", "subject_type": "group", "subject_id": "alice",
                 "effect": "allow", "filter": "g/#"},
                {"rule_id": "p", "subject_type": "principal", "subject_id": "alice",
                 "effect": "allow", "filter": "p/#"},
            ],
        }))
        ids = {r.rule_id for r in policy.effective_rules("alice")}
        self.assertEqual(ids, {"g", "p"})

    def test_duplicate_rule_id_and_principal_id(self):
        with self.assertRaises(PolicyError) as ctx:
            parse_policy(policy_text([
                {"rule_id": "x", "subject_type": "principal",
                 "subject_id": "team-plant", "effect": "allow", "filter": "a/#"},
                {"rule_id": "x", "subject_type": "principal",
                 "subject_id": "team-plant", "effect": "allow", "filter": "b/#"},
            ], principals=[
                {"principal_id": "dup", "groups": []},
                {"principal_id": "dup", "groups": []},
            ]))
        reasons = {e["reason"] for e in ctx.exception.errors}
        self.assertIn("DUPLICATE_RULE_ID", reasons)
        self.assertIn("DUPLICATE_PRINCIPAL_ID", reasons)

    def test_unknown_group_and_principal_references_point_to_location(self):
        with self.assertRaises(PolicyError) as ctx:
            parse_policy(policy_text([
                {"rule_id": "g", "subject_type": "group", "subject_id": "ghost-group",
                 "effect": "allow", "filter": "#"},
                {"rule_id": "p", "subject_type": "principal", "subject_id": "ghost-p",
                 "effect": "allow", "filter": "#"},
            ]))
        errors = ctx.exception.errors
        locations = {e["location"] for e in errors}
        self.assertIn("$.rules[0].subject_id", locations)
        self.assertIn("$.rules[1].subject_id", locations)
        reasons = {e["reason"] for e in errors}
        self.assertIn("UNKNOWN_GROUP_REF", reasons)
        self.assertIn("UNKNOWN_PRINCIPAL_REF", reasons)

    def test_bad_effect_bad_type_bad_filter(self):
        with self.assertRaises(PolicyError) as ctx:
            parse_policy(policy_text([
                {"rule_id": "r", "subject_type": "weird", "subject_id": "team-plant",
                 "effect": "maybe", "filter": "Bad/#"},
            ]))
        reasons = {e["reason"] for e in ctx.exception.errors}
        self.assertIn("MISSING_OR_INVALID_FIELD", reasons)
        self.assertIn("INVALID_LEVEL_CHAR", reasons)

    def test_illegal_filters_rejected_not_ignored(self):
        for bad_filter in ("a//b", "$SYS/x", "a/#/b", "UPPER/x"):
            with self.subTest(bad_filter=bad_filter):
                with self.assertRaises(PolicyError) as ctx:
                    parse_policy(policy_text([
                        {"rule_id": "r", "subject_type": "principal",
                         "subject_id": "team-plant", "effect": "allow",
                         "filter": bad_filter},
                    ]))
                self.assertTrue(ctx.exception.errors)

    def test_unknown_field_and_bad_json(self):
        with self.assertRaises(PolicyError) as ctx:
            parse_policy(policy_text([]).replace('"rules"', '"rules", "extra": 1')
                         if False else
                         json.dumps({"version": 1,
                                     "principals": [{"principal_id": "a", "groups": []}],
                                     "rules": [], "surprise": True}))
        self.assertEqual(ctx.exception.errors[0]["reason"], "UNKNOWN_FIELD")
        with self.assertRaises(PolicyError) as ctx:
            parse_policy("{not json")
        self.assertEqual(ctx.exception.errors[0]["reason"], "INVALID_JSON")

    def test_duplicate_group_membership_and_bad_group_name(self):
        with self.assertRaises(PolicyError) as ctx:
            parse_policy(json.dumps({
                "version": 1,
                "principals": [{"principal_id": "a", "groups": ["g", "g", ""]}],
                "rules": [],
            }))
        reasons = {e["reason"] for e in ctx.exception.errors}
        self.assertIn("DUPLICATE_GROUP_MEMBERSHIP", reasons)
        self.assertIn("MISSING_OR_INVALID_FIELD", reasons)

    def test_registered_principal_without_rules_is_normal(self):
        policy = parse_policy(policy_text([]))
        self.assertEqual(policy.effective_rules("team-payroll"), [])


if __name__ == "__main__":
    unittest.main()
