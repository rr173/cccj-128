"""独立枚举器（补充检查，**不是正式判定算法**）。

在刻意缩小的字母表（``{a, b}``）和深度（0..3 层）上，把请求过滤器所表示的
**全部**具体主题逐一列出来，用朴素的逐条规则匹配复算授权结论，再与正式引擎
:meth:`AdmissionEngine.decide_scope` 的布尔结论 / witness 交叉核对。

它的存在只是为了给“乘积自动机非枚举判定”提供一个可完全手工理解的独立参照；
正式判定绝不依赖枚举，也不设置采样深度。
"""

from __future__ import annotations

import itertools

from offline_mqtt.admission import AdmissionEngine
from offline_mqtt.namespace import is_namespace_topic
from offline_mqtt.topic import topic_matches

TINY_ALPHABET = ("a", "b")
MAX_DEPTH = 3


def enumerate_topics(prefix_levels: tuple = ()) -> list[str]:
    """枚举小字母表上 1..MAX_DEPTH 层的全部具体主题（非空）。"""
    topics: list[str] = []
    for depth in range(1, MAX_DEPTH + 1):
        for combo in itertools.product(TINY_ALPHABET, repeat=depth):
            topics.append("/".join(prefix_levels + combo))
    return topics


def brute_force_allowed_set(engine: AdmissionEngine, principal_id: str,
                            request_filter: str) -> tuple[set, set]:
    """返回 (请求范围内且被允许的主题, 请求范围内且被拒绝的主题)。"""
    allowed: set[str] = set()
    denied: set[str] = set()
    for topic in enumerate_topics():
        if not is_namespace_topic(topic) or not topic_matches(request_filter, topic):
            continue
        decision = engine.decide_topic(principal_id, topic)
        (allowed if decision.allowed else denied).add(topic)
    return allowed, denied


def _request_fits_enumeration_space(request_filter: str) -> bool:
    """请求过滤器能否被深度<=3、字母表{a,b} 的枚举空间完整覆盖。

    带 # 的请求可延伸到任意深度，必然超出 MAX_DEPTH；因此只要请求含 # 就不能
    用有限枚举证明“完整安全”。不含 # 时，请求是固定深度，深度<=3 且每个字面层
    都落在小字母表内才可被完整枚举。
    """
    levels = request_filter.split("/")
    if "#" in levels:
        return False
    if len(levels) > MAX_DEPTH:
        return False
    for level in levels:
        if level not in ("+",) and level not in TINY_ALPHABET:
            return False
    return True


def assert_scope_matches_enumeration(test_case, engine: AdmissionEngine,
                                     principal_id: str, request_filter: str) -> None:
    """正式引擎的 scope 结论必须与小空间全枚举一致。"""
    allowed, denied = brute_force_allowed_set(engine, principal_id, request_filter)
    scope = engine.decide_scope(principal_id, request_filter)
    in_scope = allowed | denied

    if not in_scope:
        # 小空间里没有任何主题落在请求范围内（例如请求字面层不在小字母表），
        # 枚举无法约束结论，跳过。
        return

    if denied:
        test_case.assertFalse(scope.safe, f"{request_filter} 枚举发现越权但引擎判 safe")
        test_case.assertIsNotNone(scope.witness)
        witness = scope.witness.topic
        # witness 必须真实属于请求范围，且确实被具体判断拒绝。
        test_case.assertTrue(
            topic_matches(request_filter, witness),
            f"witness {witness!r} 不属于请求 {request_filter!r}",
        )
        # witness 的层值可能是引擎 OTHER 符号还原出的、不在小字母表内的合法层值
        # （这正是“未见名称”能力）；枚举集只覆盖小字母表，因此这里用具体判断复核，
        # 并对落在枚举空间内的 witness 额外要求它确实出现在 denied 集合里。
        cross = engine.decide_topic(principal_id, witness)
        test_case.assertFalse(cross.allowed)
        test_case.assertEqual(tuple(cross.deny_rule_ids), scope.witness.deny_rule_ids)
        test_case.assertEqual(tuple(cross.allow_rule_ids), scope.witness.allow_rule_ids)
        if witness in in_scope:
            test_case.assertIn(witness, denied,
                               f"枚举空间内的 witness {witness!r} 未被枚举判为拒绝")
    elif _request_fits_enumeration_space(request_filter):
        # 只有当请求被枚举空间完整覆盖时，“枚举内全部允许”才能用来约束引擎。
        if scope.safe:
            test_case.assertIsNone(scope.witness)
        else:
            # 引擎判 unsafe 但枚举空间内全允许：其 witness 必须落在枚举空间之外
            # （OTHER 还原出的未见层值），否则就是真正的不一致。
            witness = scope.witness.topic
            test_case.assertNotIn(
                witness, in_scope,
                f"{request_filter} 枚举空间内全部允许，但引擎给出空间内反例 {witness!r}",
            )
            # 且该 witness 仍必须真实属于请求并被具体判断拒绝。
            test_case.assertTrue(topic_matches(request_filter, witness))
            test_case.assertFalse(engine.decide_topic(principal_id, witness).allowed)
