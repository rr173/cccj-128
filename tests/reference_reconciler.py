"""独立参考器：枚举全部顺序配对与小范围 delta 值（不调用正式求解函数）。

它刻意用最朴素的方式表达合同，用于核对正式实现的 K、最优 delta 集合、
每根候选、可不配标志与 forced 对。只适用于小输入（delta 宽度有限），
不作为生产算法，也不导入 :mod:`offline_mqtt.capture_match` 的任何函数。
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RefRoot:
    index: int
    observed_ns: int
    topic: str
    payload: bytes
    retain: bool

    @property
    def key(self) -> tuple:
        return (self.topic, self.payload, self.retain)


def _enumerate_matchings(left, right, delta, u, v):
    """返回该 delta 下所有“非交叉、一对一”配对集合。"""
    n = len(left)
    m = len(right)
    options = []
    for i in range(n):
        row = []
        for j in range(m):
            if left[i].key != right[j].key:
                continue
            transit = right[j].observed_ns + delta - left[i].observed_ns
            if u <= transit <= v:
                row.append(j)
        options.append(row)

    results = []

    def walk(i, last_right, chosen):
        if i == n:
            results.append(frozenset(chosen))
            return
        walk(i + 1, last_right, chosen)
        for j in options[i]:
            if j > last_right:
                chosen.append((i, j))
                walk(i + 1, j, chosen)
                chosen.pop()

    walk(0, -1, [])
    return results


def reference_summary(left, right, offset, transit):
    a, b = offset
    u, v = transit
    n, m = len(left), len(right)
    per_delta = {}
    for delta in range(a, b + 1):
        matchings = _enumerate_matchings(left, right, delta, u, v)
        best = max((len(x) for x in matchings), default=0)
        per_delta[delta] = (best, [x for x in matchings if len(x) == best])

    K = max((best for best, _ in per_delta.values()), default=0)
    optimal_deltas = [d for d in range(a, b + 1) if per_delta[d][0] == K]

    left_cand = [set() for _ in range(n)]
    right_cand = [set() for _ in range(m)]
    left_unmatched = [False] * n
    right_unmatched = [False] * m
    forced = None

    for delta in optimal_deltas:
        _, optimal = per_delta[delta]
        cell_forced = set()
        for matching in optimal:
            used_left = {i for i, _ in matching}
            used_right = {j for _, j in matching}
            for i, j in matching:
                left_cand[i].add(j)
                right_cand[j].add(i)
            for i in range(n):
                if i not in used_left:
                    left_unmatched[i] = True
            for j in range(m):
                if j not in used_right:
                    right_unmatched[j] = True
        for i in range(n):
            common = None
            for matching in optimal:
                cols = {j for x, j in matching if x == i}
                common = cols if common is None else common & cols
            for j in common or ():
                cell_forced.add((i, j))
        forced = cell_forced if forced is None else forced & cell_forced

    return {
        "K": K,
        "optimal_deltas": optimal_deltas,
        "left_candidates": [sorted(x) for x in left_cand],
        "right_candidates": [sorted(x) for x in right_cand],
        "left_can_be_unmatched": left_unmatched,
        "right_can_be_unmatched": right_unmatched,
        "forced_pairs": sorted(forced or ()),
        "per_delta": per_delta,
    }


def intervals_of(values):
    """把升序整数列表压缩成有序闭区间。"""
    out = []
    for value in values:
        if out and out[-1][1] + 1 == value:
            out[-1][1] = value
        else:
            out.append([value, value])
    return out
