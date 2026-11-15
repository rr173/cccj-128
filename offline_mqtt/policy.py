"""采集范围授权策略：可读 JSON 文件的解析、校验与规范化。

策略文件结构（字段名固定、未知字段一律报输入错误，避免静默忽略）::

    {
      "version": 1,
      "principals": [
        {"principal_id": "team-plant", "groups": ["plant-team"]},
        {"principal_id": "team-payroll", "groups": []}
      ],
      "rules": [
        {"rule_id": "r1", "subject_type": "principal",
         "subject_id": "team-plant", "effect": "allow", "filter": "plant/#"},
        {"rule_id": "r2", "subject_type": "group",
         "subject_id": "plant-team", "effect": "deny",  "filter": "plant/private/#"}
      ]
    }

裁定语义（见 :mod:`offline_mqtt.admission`）：
一个已登记身份的有效规则 = 其直接规则 + 全部所属组规则的合集；主体类型必须
显式声明，**不能**靠名字是否相同来猜测（身份名与组名可以同名）。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from .namespace import validate_namespace_filter
from .topic import TopicError

POLICY_VERSION = 1
EFFECTS = ("allow", "deny")
SUBJECT_TYPES = ("principal", "group")


class PolicyError(ValueError):
    """策略输入错误。

    ``errors`` 携带全部问题（每条含 location/reason/detail），因此一次校验即可
    指出所有位置，而不是遇到第一个错误就停。策略非法时不得继续裁定。
    """

    def __init__(self, errors: list[dict]):
        self.errors = errors
        summary = "; ".join(f"[{e['location']}] {e['reason']}: {e['detail']}" for e in errors)
        super().__init__(f"策略输入错误 {len(errors)} 条: {summary}")


@dataclass(frozen=True)
class Rule:
    rule_id: str
    subject_type: str          # "principal" | "group"
    subject_id: str
    effect: str                # "allow" | "deny"
    topic_filter: str
    source_index: int          # 在 rules 数组中的下标，便于定位


@dataclass(frozen=True)
class Principal:
    principal_id: str
    groups: tuple              # tuple[str, ...]
    source_index: int


@dataclass
class Policy:
    principals: dict           # principal_id -> Principal
    groups: dict               # group_name -> tuple[principal_id, ...]
    rules: dict                # rule_id -> Rule
    raw: dict = field(repr=False, default_factory=dict)
    canonical_json: str = ""
    digest_sha256: str = ""

    # ------------------------------------------------------------- 规则索引
    def effective_rules(self, principal_id: str) -> list[Rule]:
        """返回某身份的全部有效规则（直接规则 + 所属组规则），按 rule_id 稳定排序。"""
        principal = self.principals[principal_id]
        out: list[Rule] = []
        for rule in self.rules.values():
            if rule.subject_type == "principal" and rule.subject_id == principal_id:
                out.append(rule)
            elif rule.subject_type == "group" and rule.subject_id in principal.groups:
                out.append(rule)
        out.sort(key=lambda r: r.rule_id)
        return out

    def summary(self) -> dict:
        """策略内容摘要，随判断结果返回，便于同事用同一份策略重现。"""
        return {
            "policy_digest_sha256": self.digest_sha256,
            "canonical_json_sha256": self.digest_sha256,
            "principal_count": len(self.principals),
            "group_count": len(self.groups),
            "rule_count": len(self.rules),
        }


# ----------------------------------------------------------------- 工具函数
def _is_nonempty_str(value: Any) -> bool:
    return isinstance(value, str) and value != ""


def load_policy_file(path: str) -> Policy:
    """从文件读取并解析策略（UTF-8 JSON）。"""
    with open(path, "r", encoding="utf-8") as fh:
        text = fh.read()
    return parse_policy(text)


def parse_policy(text_or_obj: Any) -> Policy:
    """解析策略文本（JSON 字符串）或等价 dict，做完整校验后返回 :class:`Policy`。

    任何输入错误都汇总为单个 :class:`PolicyError`（可能含多条带位置的错误）。
    """
    errors: list[dict] = []

    def err(location: str, reason: str, detail: str) -> None:
        errors.append({"location": location, "reason": reason, "detail": detail})

    raw: Any = text_or_obj
    if isinstance(text_or_obj, str):
        try:
            raw = json.loads(text_or_obj)
        except json.JSONDecodeError as exc:
            raise PolicyError([{
                "location": "$",
                "reason": "INVALID_JSON",
                "detail": f"JSON 解析失败: {exc.msg} (line {exc.lineno} col {exc.colno})",
            }])
    if not isinstance(raw, dict):
        raise PolicyError([{
            "location": "$",
            "reason": "INVALID_POLICY_ROOT",
            "detail": f"策略根必须是对象，实际类型 {type(raw).__name__}",
        }])

    _check_unknown_keys(raw, ("version", "principals", "rules"), "$", err)

    version = raw.get("version")
    if isinstance(version, bool) or not isinstance(version, int):
        err("$.version", "MISSING_OR_INVALID_FIELD", "version 必须是整数")
    elif version != POLICY_VERSION:
        err("$.version", "UNSUPPORTED_POLICY_VERSION", f"只支持 version={POLICY_VERSION}，实际 {version}")

    principals = _parse_principals(raw, err)
    group_membership: dict[str, list[str]] = {}
    for principal in principals.values():
        for group in principal.groups:
            group_membership.setdefault(group, []).append(principal.principal_id)

    rules = _parse_rules(raw, frozenset(principals), frozenset(group_membership), err)

    if errors:
        raise PolicyError(errors)

    canonical = json.dumps(raw, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return Policy(
        principals=principals,
        groups={name: tuple(sorted(members)) for name, members in group_membership.items()},
        rules=rules,
        raw=raw,
        canonical_json=canonical,
        digest_sha256=digest,
    )


def _check_unknown_keys(obj: dict, allowed: tuple, location: str, err) -> None:
    for key in obj:
        if key not in allowed:
            err(f"{location}.{key}", "UNKNOWN_FIELD", f"未知字段 {key!r}，合法字段: {', '.join(allowed)}")


def _parse_principals(raw: dict, err) -> dict:
    location = "$.principals"
    raw_principals = raw.get("principals")
    if not isinstance(raw_principals, list):
        err(location, "MISSING_OR_INVALID_FIELD", "principals 必须是数组")
        return {}

    principals: dict[str, Principal] = {}
    for index, item in enumerate(raw_principals):
        loc = f"{location}[{index}]"
        if not isinstance(item, dict):
            err(loc, "MISSING_OR_INVALID_FIELD", f"身份条目必须是对象，实际类型 {type(item).__name__}")
            continue
        _check_unknown_keys(item, ("principal_id", "groups"), loc, err)

        pid = item.get("principal_id")
        if not _is_nonempty_str(pid):
            err(f"{loc}.principal_id", "MISSING_OR_INVALID_FIELD", "principal_id 必须是非空字符串")
            pid = None
        elif pid in principals:
            err(f"{loc}.principal_id", "DUPLICATE_PRINCIPAL_ID", f"principal_id {pid!r} 重复登记")
            pid = None

        raw_groups = item.get("groups", [])
        groups: list[str] = []
        if not isinstance(raw_groups, list):
            err(f"{loc}.groups", "MISSING_OR_INVALID_FIELD", "groups 必须是字符串数组（可为空数组）")
        else:
            seen: set[str] = set()
            for gi, group in enumerate(raw_groups):
                gloc = f"{loc}.groups[{gi}]"
                if not _is_nonempty_str(group):
                    err(gloc, "MISSING_OR_INVALID_FIELD", "组名必须是非空字符串")
                    continue
                if group in seen:
                    err(gloc, "DUPLICATE_GROUP_MEMBERSHIP", f"身份 {pid!r} 重复归入组 {group!r}")
                    continue
                seen.add(group)
                groups.append(group)

        if pid is not None:
            principals[pid] = Principal(pid, tuple(groups), index)
    return principals


def _parse_rules(raw: dict, known_principals: frozenset, known_groups: frozenset, err) -> dict:
    location = "$.rules"
    raw_rules = raw.get("rules")
    if not isinstance(raw_rules, list):
        err(location, "MISSING_OR_INVALID_FIELD", "rules 必须是数组")
        return {}

    rules: dict[str, Rule] = {}
    for index, item in enumerate(raw_rules):
        loc = f"{location}[{index}]"
        if not isinstance(item, dict):
            err(loc, "MISSING_OR_INVALID_FIELD", f"规则条目必须是对象，实际类型 {type(item).__name__}")
            continue
        _check_unknown_keys(item, ("rule_id", "subject_type", "subject_id", "effect", "filter"), loc, err)

        rule_id = item.get("rule_id")
        if not _is_nonempty_str(rule_id):
            err(f"{loc}.rule_id", "MISSING_OR_INVALID_FIELD", "rule_id 必须是非空字符串")
        elif rule_id in rules:
            err(f"{loc}.rule_id", "DUPLICATE_RULE_ID", f"rule_id {rule_id!r} 重复")

        subject_type = item.get("subject_type")
        if subject_type not in SUBJECT_TYPES:
            err(f"{loc}.subject_type", "MISSING_OR_INVALID_FIELD",
                f"subject_type 必须是 {SUBJECT_TYPES} 之一，实际 {subject_type!r}")

        subject_id = item.get("subject_id")
        if not _is_nonempty_str(subject_id):
            err(f"{loc}.subject_id", "MISSING_OR_INVALID_FIELD", "subject_id 必须是非空字符串")
        elif subject_type == "principal" and subject_id not in known_principals:
            err(f"{loc}.subject_id", "UNKNOWN_PRINCIPAL_REF",
                f"规则引用了未登记身份 {subject_id!r}")
        elif subject_type == "group" and subject_id not in known_groups:
            err(f"{loc}.subject_id", "UNKNOWN_GROUP_REF",
                f"规则引用了不存在的组 {subject_id!r}（组由 principals[].groups 隐式声明）")

        effect = item.get("effect")
        if effect not in EFFECTS:
            err(f"{loc}.effect", "MISSING_OR_INVALID_FIELD",
                f"effect 必须是 {EFFECTS} 之一，实际 {effect!r}")

        topic_filter = item.get("filter")
        if not isinstance(topic_filter, str):
            err(f"{loc}.filter", "MISSING_OR_INVALID_FIELD",
                f"filter 必须是字符串，实际类型 {type(topic_filter).__name__}")
        else:
            try:
                validate_namespace_filter(topic_filter)
            except TopicError as exc:
                err(f"{loc}.filter", exc.reason, exc.detail)

        if _is_nonempty_str(rule_id) and rule_id not in rules and subject_type in SUBJECT_TYPES \
                and effect in EFFECTS and isinstance(topic_filter, str):
            # 过滤器即便非法也登记会在下面被 errors 拦截；这里只在合法时建对象。
            try:
                validate_namespace_filter(topic_filter)
            except TopicError:
                continue
            rules[rule_id] = Rule(
                rule_id=rule_id,
                subject_type=subject_type,
                subject_id=subject_id,
                effect=effect,
                topic_filter=topic_filter,
                source_index=index,
            )
    return rules
