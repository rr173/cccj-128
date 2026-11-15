"""采集范围准入判定引擎。

提供两种可导入的判断：

1. :meth:`AdmissionEngine.decide_topic` —— 对一个**具体主题**做授权裁定，
   返回 allowed、命中的全部 allow/deny rule_id 与拒绝原因；
2. :meth:`AdmissionEngine.decide_scope` —— 对一个合法**请求过滤器**所表示的
   整个主题集合做判定：只有其中每个合法具体主题都被允许才 ``safe=true``，
   否则交付一个真实、可复核的越权 witness_topic。

核心难题是不能枚举主题（层值来自 39 个字符的无限长字符串集合，且层级数没有
上界），也不能只看当前 retained/ledger 里出现过的主题。这里的做法是把“过滤器
匹配主题”建模成有限自动机，在**过滤器乘积自动机**上求可达性，而不是枚举主题。

为什么无需逐个穷举且必然终止
----------------------------
- 每个过滤器（请求过滤器与每条有效规则过滤器）编译成一个 DFA：状态是“已匹配
  到第几层”，数量 = 过滤器层数（去掉末尾 ``#``）+ 1，外加一个死状态；``#``
  编译为接受状态上的自环（零层或多层），``+`` 是对任意层值的转移，字面量是
  对该层值的转移。DFA 状态数与主题长度无关，是有限的。
- 层值字符串是无限集合，但转移只区分“等于某字面量”与“其他任何合法层值”
  （OTHER）。因此自动机字母表 = 所有过滤器里出现过的字面层 + 一个 OTHER 符号，
  仍然是有限集合。
- 判定在“请求 DFA × 全部规则 DFA”的乘积 DFA 上做最短路径搜索（Dijkstra，
  边权 = 该层字节数 + 必要的 ``/``）。乘积状态数与边数都有有限上界，搜索必然
  终止；``#`` 的任意多层只是接受态上的自环，不会产生新状态，所以 80 层、
  200 条规则这类输入不会引发无限展开。
- 若所有可达的请求接受状态都满足“至少一条 allow DFA 同时接受且无 deny DFA
  接受”，则请求集合被完整允许（safe=true）；否则第一个不满足的接受状态沿最短
  路径还原出的层值序列就是反例。OTHER 符号还原成一个确定的、不属于任何字面量
  的合法层值，因此反例是**真实存在的具体主题**，并在返回前再跑一次
  ``decide_topic`` 复核。

字节上限
--------
完整具体主题不得超过 65535 字节。路径边权按真实字节计费，超过预算的路径直接剪
枝；因此“只有在超限长度下才出现的差异”不会被判成越权，也不可能用超限串充当
反例——所有 witness 的构造预算本身就在上限以内。

资源上限
--------
可以设置明确的策略资源上限（规则数、过滤器深度、乘积状态/边数）。触及上限时
抛 :class:`AdmissionResourceLimit`，明确报告“无法处理”，绝不伪装成 safe=true
或普通越权。
"""

from __future__ import annotations

import heapq
import itertools
from dataclasses import dataclass
from typing import Optional

from .namespace import (
    MAX_TOPIC_BYTES,
    validate_namespace_filter,
    validate_namespace_topic,
)
from .policy import Policy
from .topic import TopicError, topic_matches

_INF = float("inf")

# 明确的策略资源上限。正常目标负载（200 条规则、80 层路径）远低于这些值。
MAX_RULES = 500
MAX_FILTER_DEPTH = 100
MAX_ALPHABET_SYMBOLS = 20000
MAX_PRODUCT_STATES = 200_000
MAX_PRODUCT_EDGES = 1_000_000

# 采集命名空间单层允许的全部字符（与 namespace._LEVEL_RE 保持一致）。
_LEVEL_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789_-"
_DEAD = -1


class AdmissionError(ValueError):
    """准入判断相关错误的基类。"""


class AdmissionInputError(AdmissionError):
    """调用输入错误（未知身份、非法主题/过滤器等），携带带位置的错误列表。"""

    def __init__(self, errors: list[dict]):
        self.errors = errors
        summary = "; ".join(f"[{e['location']}] {e['reason']}: {e['detail']}" for e in errors)
        super().__init__(f"准入输入错误 {len(errors)} 条: {summary}")


class AdmissionResourceLimit(AdmissionError):
    """判定触及明确资源上限，无法给出结论（不得当作 safe 或普通越权）。"""


# ----------------------------------------------------------------- 数据结构
@dataclass(frozen=True)
class TopicDecision:
    principal_id: str
    topic: str
    allowed: bool
    allow_rule_ids: tuple
    deny_rule_ids: tuple
    reason: Optional[str]
    policy_summary: dict

    def to_dict(self) -> dict:
        return {
            "principal_id": self.principal_id,
            "topic": self.topic,
            "allowed": self.allowed,
            "allow_rule_ids": list(self.allow_rule_ids),
            "deny_rule_ids": list(self.deny_rule_ids),
            "reason": self.reason,
            "policy": self.policy_summary,
        }


@dataclass(frozen=True)
class Witness:
    topic: str
    allow_rule_ids: tuple
    deny_rule_ids: tuple
    reason: str

    def to_dict(self) -> dict:
        return {
            "witness_topic": self.topic,
            "allow_rule_ids": list(self.allow_rule_ids),
            "deny_rule_ids": list(self.deny_rule_ids),
            "reason": self.reason,
        }


@dataclass(frozen=True)
class ScopeDecision:
    principal_id: str
    request_filter: str
    safe: bool
    witness: Optional[Witness]
    policy_summary: dict
    product_states_explored: int
    product_edges_explored: int

    def to_dict(self) -> dict:
        out = {
            "principal_id": self.principal_id,
            "request_filter": self.request_filter,
            "safe": self.safe,
            "witness": None if self.witness is None else self.witness.to_dict(),
            "policy": self.policy_summary,
            "resource_usage": {
                "product_states_explored": self.product_states_explored,
                "product_edges_explored": self.product_edges_explored,
            },
        }
        return out


# ----------------------------------------------------------------- 过滤器 DFA
class _FilterDfa:
    """单个过滤器编译出的 DFA。

    状态 0..m：已匹配 m 个非 ``#`` 层；``_DEAD``：已确定不匹配。
    接受位置恒为 m（有 ``#`` 时 m 带自环，无 ``#`` 时 m 是终止态）。
    """

    __slots__ = ("rule_id", "effect", "pattern", "has_hash", "m")

    def __init__(self, rule_id: Optional[str], effect: Optional[str], topic_filter: str):
        levels = topic_filter.split("/")
        self.has_hash = levels[-1] == "#"
        self.pattern = levels[:-1] if self.has_hash else levels
        self.rule_id = rule_id
        self.effect = effect
        self.m = len(self.pattern)

    def start(self) -> int:
        return 0

    def is_accept(self, state: int) -> bool:
        return state == self.m

    def step(self, state: int, symbol: str) -> int:
        if state == _DEAD:
            return _DEAD
        if state == self.m:
            # 已到末位置：有 # 则在接受态自环（继续吃任意层），否则终止。
            return self.m if self.has_hash else _DEAD
        expected = self.pattern[state]
        if expected == "+" or expected == symbol:
            return state + 1
        return _DEAD


class AdmissionEngine:
    """基于一份已校验策略的准入判定器（无状态裁定，可重复/并发调用）。"""

    def __init__(self, policy: Policy):
        self.policy = policy
        if len(policy.rules) > MAX_RULES:
            raise AdmissionResourceLimit(
                f"策略规则数 {len(policy.rules)} 超过上限 {MAX_RULES}，无法处理"
            )
        for rule in policy.rules.values():
            depth = rule.topic_filter.count("/") + 1
            if depth > MAX_FILTER_DEPTH:
                raise AdmissionResourceLimit(
                    f"规则 {rule.rule_id!r} 过滤器深度 {depth} 超过上限 {MAX_FILTER_DEPTH}"
                )

    # ----------------------------------------------------- 1) 具体主题裁定
    def decide_topic(self, principal_id: object, topic: object) -> TopicDecision:
        principal = self._require_principal(principal_id)
        try:
            valid_topic = validate_namespace_topic(topic)
        except TopicError as exc:
            raise AdmissionInputError([{
                "location": "topic", "reason": exc.reason, "detail": exc.detail,
            }])

        allow_ids: list[str] = []
        deny_ids: list[str] = []
        for rule in self.policy.effective_rules(principal.principal_id):
            if topic_matches(rule.topic_filter, valid_topic):
                if rule.effect == "allow":
                    allow_ids.append(rule.rule_id)
                else:
                    deny_ids.append(rule.rule_id)
        allow_ids.sort()
        deny_ids.sort()

        if deny_ids:
            allowed = False
            reason = "DENY_RULE_MATCHED"
        elif not allow_ids:
            allowed = False
            reason = "NO_ALLOW_RULE_MATCHED"
        else:
            allowed = True
            reason = None
        return TopicDecision(
            principal_id=principal.principal_id,
            topic=valid_topic,
            allowed=allowed,
            allow_rule_ids=tuple(allow_ids),
            deny_rule_ids=tuple(deny_ids),
            reason=reason,
            policy_summary=self.policy.summary(),
        )

    # ----------------------------------------------------- 2) 过滤器范围裁定
    def decide_scope(self, principal_id: object, request_filter: object) -> ScopeDecision:
        principal = self._require_principal(principal_id)
        try:
            valid_filter = validate_namespace_filter(request_filter)
        except TopicError as exc:
            raise AdmissionInputError([{
                "location": "request_filter", "reason": exc.reason, "detail": exc.detail,
            }])
        depth = valid_filter.count("/") + 1
        if depth > MAX_FILTER_DEPTH:
            raise AdmissionResourceLimit(
                f"请求过滤器深度 {depth} 超过上限 {MAX_FILTER_DEPTH}，无法处理"
            )

        effective = self.policy.effective_rules(principal.principal_id)
        request_dfa = _FilterDfa(None, None, valid_filter)
        rule_dfas = [_FilterDfa(r.rule_id, r.effect, r.topic_filter) for r in effective]

        # 安全的结构性剪枝：无 # 的规则 DFA 只在恰好等于其固定层数 m 的深度接受。
        # 请求过滤器所能表示的具体主题深度范围是：
        #   带 # ：[request_dfa.m, +∞)；不带 #：{request_dfa.m}（固定层数）。
        # 若规则的接受深度根本不在请求的深度范围内，它在请求范围内永不命中，
        # 直接剔除（不影响裁定，只缩小乘积自动机）。
        req_fixed = request_dfa.m
        compatible: list[_FilterDfa] = []
        for dfa in rule_dfas:
            if dfa.has_hash:
                compatible.append(dfa)            # # 规则在 >=m 的所有深度都可能接受
            elif request_dfa.has_hash:
                if dfa.m >= req_fixed:
                    compatible.append(dfa)
            elif dfa.m == req_fixed:
                compatible.append(dfa)
        rule_dfas = compatible

        # 有限字母表：全部字面层 + OTHER（代表“其他任何合法层值”）。
        literals = self._collect_literals(request_dfa, rule_dfas)
        if len(literals) > MAX_ALPHABET_SYMBOLS:
            raise AdmissionResourceLimit(
                f"过滤器字面层符号数 {len(literals)} 超过上限 {MAX_ALPHABET_SYMBOLS}"
            )
        other_token = self._shortest_other_token(literals)
        symbols = sorted(literals) + [other_token]

        witness, states_used, edges_used = self._search(
            request_dfa, rule_dfas, symbols, other_token,
            principal_id=principal.principal_id, request_filter=valid_filter,
        )
        return ScopeDecision(
            principal_id=principal.principal_id,
            request_filter=valid_filter,
            safe=witness is None,
            witness=witness,
            policy_summary=self.policy.summary(),
            product_states_explored=states_used,
            product_edges_explored=edges_used,
        )

    # ------------------------------------------------------------- 内部实现
    def _require_principal(self, principal_id: object):
        if not isinstance(principal_id, str) or principal_id == "":
            raise AdmissionInputError([{
                "location": "principal_id",
                "reason": "MISSING_OR_INVALID_FIELD",
                "detail": "principal_id 必须是非空字符串",
            }])
        principal = self.policy.principals.get(principal_id)
        if principal is None:
            raise AdmissionInputError([{
                "location": "principal_id",
                "reason": "UNKNOWN_PRINCIPAL",
                "detail": f"身份 {principal_id!r} 未在策略中登记",
            }])
        return principal

    @staticmethod
    def _collect_literals(request_dfa: _FilterDfa, rule_dfas: list[_FilterDfa]) -> set[str]:
        literals: set[str] = set()
        for dfa in itertools.chain([request_dfa], rule_dfas):
            for level in dfa.pattern:
                if level != "+":
                    literals.add(level)
        return literals

    @staticmethod
    def _shortest_other_token(literals: frozenset | set) -> str:
        """返回不属于字面量集合的最短合法层值（OTHER 的具体还原，全局固定）。"""
        for length in range(1, 5):
            for combo in itertools.product(_LEVEL_CHARS, repeat=length):
                token = "".join(combo)
                if token not in literals:
                    return token
        # 理论不可达：4 字符组合数远大于任何受上限约束的字面量集合。
        raise AdmissionResourceLimit("无法为 OTHER 符号选择具体层值")

    def _search(
        self,
        request_dfa: _FilterDfa,
        rule_dfas: list[_FilterDfa],
        symbols: list[str],
        other_token: str,
        *,
        principal_id: str,
        request_filter: str,
    ) -> tuple[Optional[Witness], int, int]:
        # 乘积状态把**层数 depth 编进键**：(depth, 请求 DFA 状态, 规则 DFA 状态元组)。
        # 原因是末层 # 在 DFA 上表现为接受态自环，若状态不含 depth，自环会回到
        # 同一个键而被最短路去重丢弃，裸 '#' 请求就永远展开不到第一层。depth 受
        # 65535 字节预算约束（每层至少 1 字节），因此状态空间仍然有限、必然终止。
        # 起始态 depth=0 对应空主题；空主题不参与裁定，起始态本身不做接受检查。
        # 每个 DFA 位置只关心自己 pattern 里该位置的字面层（'+' 不引入符号）。
        # 预计算“位置 -> 相关字面层集合”，展开时只枚举这些符号 + OTHER，避免在
        # 200 条互不相关规则下让每个状态都遍历全部 200 个字面量导致边数平方爆炸。
        def literals_at(dfa: _FilterDfa, pos: int) -> set:
            if pos < len(dfa.pattern) and dfa.pattern[pos] != "+":
                return {dfa.pattern[pos]}
            return set()

        start = (0, request_dfa.start(), tuple(d.start() for d in rule_dfas))
        dist: dict[tuple, int] = {start: 0}
        prev: dict[tuple, tuple] = {}   # state -> (prev_state, symbol)
        heap: list[tuple] = [(0, start)]
        states_used = 1
        edges_used = 0

        while heap:
            cost, state = heapq.heappop(heap)
            if cost != dist.get(state):
                continue  # 过期条目
            depth, req_state, rule_states = state

            # 非起始的请求接受态 => 对应一条真实的非空具体主题路径。
            if depth > 0 and request_dfa.is_accept(req_state):
                witness = self._violation(
                    state, rule_dfas, prev, other_token,
                    principal_id=principal_id, request_filter=request_filter,
                )
                if witness is not None:
                    return witness, states_used, edges_used
                # 该状态合法；# 自环的更深路径由下方统一展开，不在此提前终止。

            # 本位置相关字母表：请求位置的字面量 + 仍存活规则该位置的字面量 + OTHER。
            relevant: set = set(literals_at(request_dfa, req_state))
            for dfa, rstate in zip(rule_dfas, rule_states):
                if rstate != _DEAD:
                    relevant.update(literals_at(dfa, rstate))
            active_symbols = sorted(relevant) + [other_token]

            for symbol in active_symbols:
                if states_used >= MAX_PRODUCT_STATES or edges_used >= MAX_PRODUCT_EDGES:
                    raise AdmissionResourceLimit(
                        f"乘积自动机展开触及上限（states={states_used}, edges={edges_used}），"
                        f"无法对过滤器 {request_filter!r} 给出完整结论"
                    )
                next_req = request_dfa.step(req_state, symbol)
                if next_req == _DEAD:
                    continue
                # 已进入死态的规则 DFA 永远停在死态，折叠成常量 _DEAD：
                # 不让“已失效规则”的状态位置继续参与乘积，避免边数随无关规则爆炸。
                next_rules = tuple(
                    _DEAD if s == _DEAD else d.step(s, symbol)
                    for d, s in zip(rule_dfas, rule_states)
                )
                next_state = (depth + 1, next_req, next_rules)
                token_len = len(other_token) if symbol == other_token else len(symbol)
                edge_cost = token_len + (1 if depth > 0 else 0)  # 第二层起每层前一个 '/'
                new_cost = cost + edge_cost
                # 超过 65535 字节的路径只代表超限具体主题，不参与权限判断：
                # 既不展开也不能作为反例（直接剪枝，不能用超限串充当 witness）。
                if new_cost > MAX_TOPIC_BYTES:
                    continue
                edges_used += 1
                old_cost = dist.get(next_state)
                if old_cost is None:
                    states_used += 1
                if new_cost < (old_cost if old_cost is not None else _INF):
                    dist[next_state] = new_cost
                    prev[next_state] = (state, symbol)
                    heapq.heappush(heap, (new_cost, next_state))
        return None, states_used, edges_used

    def _violation(
        self,
        state: tuple,
        rule_dfas: list[_FilterDfa],
        prev: dict,
        other_token: str,
        *,
        principal_id: str,
        request_filter: str,
    ) -> Optional[Witness]:
        _depth, _req_state, rule_states = state
        allow_ids: list[str] = []
        deny_ids: list[str] = []
        for dfa, rule_state in zip(rule_dfas, rule_states):
            if dfa.is_accept(rule_state):
                if dfa.effect == "allow":
                    allow_ids.append(dfa.rule_id)
                else:
                    deny_ids.append(dfa.rule_id)
        allow_ids.sort()
        deny_ids.sort()
        if not deny_ids and allow_ids:
            return None  # 该具体主题被允许

        # 沿前驱链还原层值序列（OTHER 还原为确定的具体合法层值）。
        symbols_rev: list[str] = []
        cursor = state
        while cursor in prev:
            cursor, symbol = prev[cursor]
            symbols_rev.append(symbol)
        levels = list(reversed(symbols_rev))
        witness_topic = "/".join(other_token if s == other_token else s for s in levels)

        reason = "DENY_RULE_MATCHED" if deny_ids else "NO_ALLOW_RULE_MATCHED"
        witness = Witness(
            topic=witness_topic,
            allow_rule_ids=tuple(allow_ids),
            deny_rule_ids=tuple(deny_ids),
            reason=reason,
        )
        self._verify_witness(witness, principal_id, request_filter)
        return witness

    def _verify_witness(self, witness: Witness, principal_id: str, request_filter: str) -> None:
        """返回前的硬复核：witness 必须是合法、属于请求范围且确实被拒绝的真实主题。"""
        validate_namespace_topic(witness.topic)
        if len(witness.topic.encode("utf-8")) > MAX_TOPIC_BYTES:
            raise AdmissionError("内部错误：witness 超过主题字节上限")
        if not topic_matches(request_filter, witness.topic):
            raise AdmissionError(f"内部错误：witness {witness.topic!r} 不属于请求 {request_filter!r}")
        cross_check = self.decide_topic(principal_id, witness.topic)
        if cross_check.allowed:
            raise AdmissionError(f"内部错误：witness {witness.topic!r} 实际被允许")
        if tuple(cross_check.deny_rule_ids) != witness.deny_rule_ids \
                or tuple(cross_check.allow_rule_ids) != witness.allow_rule_ids:
            raise AdmissionError(
                f"内部错误：witness 规则命中与具体主题裁定不一致 "
                f"({cross_check.allow_rule_ids}/{cross_check.deny_rule_ids})"
            )

    # 便于外部直接拿到策略摘要
    def policy_summary(self) -> dict:
        return self.policy.summary()
