"""捕获链对账的纯计算核心（无 I/O、无 SQLite、无副作用）。

输入是两端**已折叠**的根发布列表（见 :mod:`offline_mqtt.capture_reconcile`），
以及整数闭区间 ``offset_ns=[a,b]``、``transit_ns=[u,v]``。

合同
====
- 一条有序转发链：每次独立发布至多在另一端对应一次，发布先后不颠倒；
  允许丢失与单侧多出。**不宣称任意 MQTT 部署都具备这项顺序保证。**
- 配对只发生在根发布之间；内容相等仅指 ``topic`` 原字符串、``payload`` 原字节、
  ``retain`` 相等。
- 同一次比较只有一个共同整数偏差 delta ∈ [a,b]，加在右端时钟上；
  每一对都须满足 ``u ≤ R + delta − L ≤ v``。
- 配对是非交叉的一对一关系：左端次序递增时右端次序严格递增。

算法
====
固定一对内容相等的根，使该边合法（时间条件成立 + 内容相等）的 delta 集合是
整数闭区间 ``[L−R+u, L−R+v] ∩ [a,b]``。所有这样的区间端点把 delta 轴切成
若干段；同一段内每个整数 delta 的**边集完全相同**，因此最大不交叉配对数
（LCS）在该段内恒定。于是：

1. 用扫描线枚举这些段（段数 ≤ 2·内容相等对数 + 1），**不逐纳秒枚举**，也不
   抽样后宣称完整；
2. 每段用位并行 LCS（Hyyrö 半字节位向量）精确求得最大配对数；
3. 全局 K = 所有段 K 的最大值；可行最优 delta = 所有 K 段在整数轴上的并集，
   相邻整数合并为闭区间；
4. 只对最优段逐段计算“某条边能否出现在最优方案里”“某根能否不被配对”
   “某条边是否在每一个最优方案里”，再分别取并/或/交。每个局部结论都来自
   单个 delta 下的完整最优方案，因此不会把不同 delta 的局部方案拼成全局方案；
5. 任选一个最优段，回溯出一个完整最优配对实例；回溯规则固定，重复执行稳定。

资源上限见 :data:`MAX_DELTA_CELLS` / :data:`MAX_OPTIMAL_CELLS`；超限抛
:class:`MatchResourceLimit`，绝不输出貌似完整的结论。
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass
from typing import Optional

# ------------------------------------------------------------------ 资源上限
MAX_DELTA_CELLS = 200_000       # delta 切点段总数上限
MAX_OPTIMAL_CELLS = 2048        # 参与细算的最优段数上限
MAX_ANALYSIS_CACHE = 1024       # 相同边集段的分析缓存条数


class MatchResourceLimit(RuntimeError):
    """计算触及明确资源上限；调用方必须报告“未能完成”，不得给出 K。"""


# ------------------------------------------------------------------ 位运算
_REV = bytes(int(f"{i:08b}"[::-1], 2) for i in range(256))


def reverse_bits(value: int, width: int) -> int:
    """把 ``width`` 位的低位序位串整体反转。"""
    if width <= 0:
        return 0
    nbytes = (width + 7) // 8
    raw = value.to_bytes(nbytes, "little")
    rev = int.from_bytes(bytes(_REV[b] for b in reversed(raw)), "little")
    return rev >> (8 * nbytes - width)


def lcs_size(masks: list[int]) -> int:
    """位并行 LCS：返回行掩码序列与列集合的最大非交叉配对数。"""
    state = 0
    for mask in masks:
        if mask:
            x = state | mask
            state = x & ~(x - ((state << 1) | 1))
    return state.bit_count()


def forward_states(masks: list[int]) -> list[int]:
    """返回每处理完一行后的 LCS 位状态（前缀长度性质见模块文档）。"""
    out = [0]
    state = 0
    for mask in masks:
        if mask:
            x = state | mask
            state = x & ~(x - ((state << 1) | 1))
        out.append(state)
    return out


_LOW_MASKS: list[int] = [0]


def _low_mask(width: int) -> int:
    """低 ``width`` 位全 1 的掩码；带缓存，避免内层循环反复构造大整数。"""
    while len(_LOW_MASKS) <= width:
        _LOW_MASKS.append((_LOW_MASKS[-1] << 1) | 1)
    return _LOW_MASKS[width]


def pref_count(mask: int, width: int) -> int:
    """低 ``width`` 位中 1 的个数 = LCS(已处理行, 前 width 列)。"""
    return (mask & _low_mask(width)).bit_count()


# --------------------------------------------------------------- 边集与段
@dataclass(frozen=True)
class DeltaCell:
    lo: int
    hi: int
    pairs: int


class DeltaSweep:
    """在 delta 轴上按边集不变段扫描，并精确求每段最大配对数。"""

    def __init__(self, left: list, right: list, offset: tuple, transit: tuple):
        self.left = left
        self.right = right
        self.a, self.b = offset
        self.u, self.v = transit
        self.n = len(left)
        self.m = len(right)
        self._groups: dict[tuple, list[int]] = {}
        for j, root in enumerate(right):
            self._groups.setdefault(root.content_key, []).append(j)
        self._group_times = {
            key: [right[j].observed_ns for j in idxs]
            for key, idxs in self._groups.items()
        }
        self._events: dict[int, list[tuple[int, int, bool]]] = {}
        self.content_pair_count = 0
        self._build_events()

    def _build_events(self) -> None:
        a, b, u, v = self.a, self.b, self.u, self.v
        for i, lroot in enumerate(self.left):
            idxs = self._groups.get(lroot.content_key)
            if not idxs:
                continue
            ltime = lroot.observed_ns
            for j in idxs:
                rtime = self.right[j].observed_ns
                lo = ltime - rtime + u
                hi = ltime - rtime + v
                if lo < a:
                    lo = a
                if hi > b:
                    hi = b
                if lo > hi:
                    continue
                self.content_pair_count += 1
                self._events.setdefault(lo, []).append((i, j, True))
                if hi + 1 <= b:
                    self._events.setdefault(hi + 1, []).append((i, j, False))

    def _positions(self) -> list[int]:
        return sorted({self.a, self.b + 1} | set(self._events))

    def cells(self) -> list[DeltaCell]:
        positions = self._positions()
        if len(positions) - 1 > MAX_DELTA_CELLS:
            raise MatchResourceLimit(
                f"delta 切点段数 {len(positions) - 1} 超过上限 {MAX_DELTA_CELLS}"
            )
        masks = [0] * self.n
        out: list[DeltaCell] = []
        for idx in range(len(positions) - 1):
            pos = positions[idx]
            for i, j, add in self._events.get(pos, ()):
                bit = 1 << j
                if add:
                    masks[i] |= bit
                else:
                    masks[i] &= ~bit
            out.append(DeltaCell(pos, positions[idx + 1] - 1, lcs_size(masks)))
        return out

    def replay(self, wanted_starts: set[int]):
        """按段重放，产出 (start, hi, masks)；只产出指定起始位置的段。"""
        positions = self._positions()
        masks = [0] * self.n
        for idx in range(len(positions) - 1):
            pos = positions[idx]
            for i, j, add in self._events.get(pos, ()):
                bit = 1 << j
                if add:
                    masks[i] |= bit
                else:
                    masks[i] &= ~bit
            if pos in wanted_starts:
                yield pos, positions[idx + 1] - 1, list(masks)


# --------------------------------------------------------- 单段完整分析
@dataclass
class CellAnalysis:
    candidate_edges: frozenset
    left_unmatched: tuple
    right_unmatched: tuple
    forced_edges: frozenset


def analyze_cell(masks: list[int], n: int, m: int, k: int) -> CellAnalysis:
    """在边集固定的一个 delta 段内，求恰好达到 K 的全部局部结论。

    设 ``D(i,j)=LCS(前 i 行, 前 j 列)``、``S(i,j)=LCS(第 i 行起, 第 j 列起)``：

    - 边 (i,j) 能出现在某个最优方案 ⇔ ``D(i,j)+1+S(i+1,j+1)=K``；
    - 左根 i 可不配 ⇔ 存在最优方案不使用行 i ⇔
      ``max_q D(i,q)+S(i+1,q)=K``；
    - 右根 j 可不配 ⇔ 存在最优方案不使用列 j ⇔
      ``max_p D(p,j)+S(p,j+1)=K``；
    - 边 (i,j) 是 forced ⇔ 它可达最优，且去掉“使用该边”的所有方案后最大
      配对数 < K（含不使用行 i 的方案，或使用另一列的最优方案）。
    """
    states = forward_states(masks)
    rev_masks = [reverse_bits(masks[n - 1 - i], m) for i in range(n)]
    rev_states = forward_states(rev_masks)

    left_unmatched = [False] * n
    right_best = [0] * m
    candidate_edges: set[tuple[int, int]] = set()
    forced_edges: set[tuple[int, int]] = set()

    for i in range(n + 1):
        prefix = states[i]
        # pv[j] = D(i, j)；位状态的前缀 1 计数随 j 单调，逐位递推即可。
        pv = [0] * (m + 1)
        count = 0
        for j in range(m):
            pv[j] = count
            if (prefix >> j) & 1:
                count += 1
        pv[m] = count
        if i < n:
            suffix_state = rev_states[n - i - 1]
            # 反向状态低 q 位 = S(i+1, m-q)：一次扫描同时得到 sv 与 tail 计数。
            sv = [0] * (m + 1)
            tail_counts = [0] * (m + 1)
            acc = 0
            for q in range(1, m + 1):
                acc += (suffix_state >> (q - 1)) & 1
                sv[m - q] = acc
                tail_counts[q] = acc
            tail_counts[0] = 0
            row_unmatched_best = max(pv[j] + sv[j] for j in range(m + 1))
            if row_unmatched_best == k:
                left_unmatched[i] = True
            mask = masks[i]
            if mask:
                # Q[j] = D(i,j) + S(i+1,j+1)：使用边 (i,j) 时的最大配对数 − 1
                qvals = [pv[j] + sv[j + 1] for j in range(m)]
                allowed = []
                bits = mask
                while bits:
                    low = bits & -bits
                    j = low.bit_length() - 1
                    bits ^= low
                    allowed.append(j)
                for j in allowed:
                    if qvals[j] + 1 == k:
                        candidate_edges.add((i, j))
                if allowed:
                    order = sorted(allowed, key=lambda j: (-qvals[j], j))
                    top = order[0]
                    for j in allowed:
                        if qvals[j] + 1 != k:
                            continue
                        if top != j:
                            other = qvals[top]
                        else:
                            other = qvals[order[1]] if len(order) > 1 else -1
                        without_edge = max(row_unmatched_best, other + 1)
                        if without_edge < k:
                            forced_edges.add((i, j))
            # 右根可不配：在行边界 i 处切开，max_j D(i,j) + S(i,j+1)。
            # rev_states[n-i] 是“行 i..n-1 反向扫描”的状态：其低 r 位 =
            # LCS(行 i..n-1, 列 m-r..m-1) = S(i, m-r)。
            tail_state = rev_states[n - i]
            acc = 0
            for q in range(0, m):
                # q 位掩码对应原列 m-q..m-1，即 S(i, m-q)；取 j = m-q-1。
                j = m - q - 1
                best = pv[j] + acc
                if best > right_best[j]:
                    right_best[j] = best
                acc += (tail_state >> q) & 1
        else:
            # i == n：没有后续行，S(n,j)=0，右根可不配需 D(n,j)=K。
            for j in range(m):
                if pv[j] > right_best[j]:
                    right_best[j] = pv[j]

    right_unmatched = [value == k for value in right_best]
    return CellAnalysis(
        candidate_edges=frozenset(candidate_edges),
        left_unmatched=tuple(left_unmatched),
        right_unmatched=tuple(right_unmatched),
        forced_edges=frozenset(forced_edges),
    )


# ------------------------------------------------------------- 配对回溯
def _dp_table(masks: list[int], row_start: int, row_end: int,
              col_start: int, col_end: int) -> list[list[int]]:
    rows = row_end - row_start
    cols = col_end - col_start
    table = [[0] * (cols + 1) for _ in range(rows + 1)]
    for ri in range(1, rows + 1):
        mask = masks[row_start + ri - 1]
        cur = table[ri]
        prev = table[ri - 1]
        for cj in range(1, cols + 1):
            value = prev[cj]
            if cur[cj - 1] > value:
                value = cur[cj - 1]
            if (mask >> (col_start + cj - 1)) & 1:
                diag = prev[cj - 1] + 1
                if diag > value:
                    value = diag
            cur[cj] = value
    return table


def _backtrack(table: list[list[int]], masks: list[int],
               row_start: int, row_end: int,
               col_start: int, col_end: int) -> list[tuple[int, int]]:
    """从 DP 表逐格回溯一个最大非交叉配对；优先使用当前格的对角边。"""
    i = row_end - row_start
    j = col_end - col_start
    pairs: list[tuple[int, int]] = []
    while i > 0 and j > 0:
        row = row_start + i - 1
        col = col_start + j - 1
        if (masks[row] >> col) & 1 and table[i][j] == table[i - 1][j - 1] + 1:
            pairs.append((row, col))
            i -= 1
            j -= 1
        elif table[i][j] == table[i - 1][j]:
            i -= 1
        else:
            j -= 1
    pairs.reverse()
    return pairs


def full_matching(masks: list[int], n: int, m: int) -> list[tuple[int, int]]:
    table = _dp_table(masks, 0, n, 0, m)
    return _backtrack(table, masks, 0, n, 0, m)


def matching_with_forced_edge(masks: list[int], n: int, m: int,
                              edge: tuple[int, int]) -> Optional[list[tuple[int, int]]]:
    """求一个使用指定边的最优配对；该边不能出现在最优方案时返回 None。"""
    i, j = edge
    if not ((masks[i] >> j) & 1):
        return None
    before = _dp_table(masks, 0, i, 0, j)
    after = _dp_table(masks, i + 1, n, j + 1, m)
    total = before[i][j] + 1 + after[n - i - 1][m - j - 1]
    if total != lcs_size(masks):
        return None
    pairs = _backtrack(before, masks, 0, i, 0, j)
    pairs.append((i, j))
    pairs.extend(_backtrack(after, masks, i + 1, n, j + 1, m))
    return pairs


def matching_without_left(masks: list[int], n: int, m: int,
                          row: int) -> Optional[list[tuple[int, int]]]:
    """求一个不使用左根 row 的最优配对；不存在时返回 None。"""
    before = _dp_table(masks, 0, row, 0, m)
    after = _dp_table(masks, row + 1, n, 0, m)
    target = lcs_size(masks)
    best_q = None
    for q in range(m + 1):
        total = before[row][q] + after[n - row - 1][m - q]
        if total == target:
            best_q = q
            break
    if best_q is None:
        return None
    pairs = _backtrack(before, masks, 0, row, 0, best_q)
    pairs.extend(_backtrack(after, masks, row + 1, n, best_q, m))
    return pairs


def matching_without_right(masks: list[int], n: int, m: int,
                           col: int) -> Optional[list[tuple[int, int]]]:
    """求一个不使用右根 col 的最优配对；不存在时返回 None。"""
    before = _dp_table(masks, 0, n, 0, col)
    after = _dp_table(masks, 0, n, col + 1, m)
    target = lcs_size(masks)
    best_p = None
    for p in range(n + 1):
        total = before[p][col] + after[n - p][m - col - 1]
        if total == target:
            best_p = p
            break
    if best_p is None:
        return None
    pairs = _backtrack(before, masks, 0, best_p, 0, col)
    pairs.extend(_backtrack(after, masks, best_p, n, col + 1, m))
    return pairs


# ------------------------------------------------------------ 总体求解
@dataclass
class MatchResult:
    K: int
    optimal_intervals: list[tuple[int, int]]
    left_candidates: list[tuple[int, ...]]
    right_candidates: list[tuple[int, ...]]
    left_can_be_unmatched: list[bool]
    right_can_be_unmatched: list[bool]
    forced_pairs: tuple
    left_candidate_delta: dict
    right_candidate_delta: dict
    left_unmatched_delta: dict
    right_unmatched_delta: dict
    witness_delta: int
    witness_pairs: list[tuple[int, int]]
    stats: dict

    def delay(self, right_roots, left_roots, left_index: int,
              right_index: int, delta: int) -> int:
        return (right_roots[right_index].observed_ns + delta
                - left_roots[left_index].observed_ns)


def solve(left: list, right: list, offset: tuple, transit: tuple) -> MatchResult:
    """在声明的 offset/transit 与顺序合同下求最大可解释配对数。"""
    a, b = offset
    u, v = transit
    if a > b:
        raise ValueError("offset_ns 下界大于上界")
    if u < 0 or u > v:
        raise ValueError("transit_ns 必须满足 0<=u<=v")

    sweep = DeltaSweep(left, right, offset, transit)
    cells = sweep.cells()
    n, m = len(left), len(right)
    if not cells:
        # [a,b] 为空时不会发生（清单层已拒绝 a>b），这里只作防御。
        raise ValueError("delta 轴没有可分析段")

    best = max(cell.pairs for cell in cells)
    optimal_cells = [cell for cell in cells if cell.pairs == best]
    if len(optimal_cells) > MAX_OPTIMAL_CELLS:
        raise MatchResourceLimit(
            f"最优 delta 段数 {len(optimal_cells)} 超过上限 {MAX_OPTIMAL_CELLS}"
        )

    intervals: list[list[int]] = []
    for cell in optimal_cells:
        if intervals and intervals[-1][1] + 1 == cell.lo:
            intervals[-1][1] = cell.hi
        else:
            intervals.append([cell.lo, cell.hi])

    left_cand: list[set[int]] = [set() for _ in range(n)]
    right_cand: list[set[int]] = [set() for _ in range(m)]
    left_unmatched = [False] * n
    right_unmatched = [False] * m
    left_candidate_delta: dict[tuple[int, int], int] = {}
    right_candidate_delta: dict[tuple[int, int], int] = {}
    left_unmatched_delta: dict[int, int] = {}
    right_unmatched_delta: dict[int, int] = {}
    forced: Optional[set[tuple[int, int]]] = None

    wanted = {cell.lo for cell in optimal_cells}
    cache: dict[tuple[int, ...], CellAnalysis] = {}
    for lo, _hi, masks in sweep.replay(wanted):
        key = tuple(masks)
        analysis = cache.get(key)
        if analysis is None:
            analysis = analyze_cell(masks, n, m, best)
            if len(cache) < MAX_ANALYSIS_CACHE:
                cache[key] = analysis
        for i, j in analysis.candidate_edges:
            left_cand[i].add(j)
            right_cand[j].add(i)
            left_candidate_delta.setdefault((i, j), lo)
            right_candidate_delta.setdefault((i, j), lo)
        for i, flag in enumerate(analysis.left_unmatched):
            if flag:
                left_unmatched[i] = True
                left_unmatched_delta.setdefault(i, lo)
        for j, flag in enumerate(analysis.right_unmatched):
            if flag:
                right_unmatched[j] = True
                right_unmatched_delta.setdefault(j, lo)
        if forced is None:
            forced = set(analysis.forced_edges)
        else:
            forced &= analysis.forced_edges

    if forced is None:
        forced = set()

    # 完整最优实例：取数值最小的最优段起点，确定性回溯。
    witness_delta = optimal_cells[0].lo
    witness_masks = None
    for lo, _hi, masks in sweep.replay({witness_delta}):
        witness_masks = masks
        break
    assert witness_masks is not None
    witness_pairs = full_matching(witness_masks, n, m)

    stats = {
        "algorithm": "delta-cutpoint-sweep + bit-parallel-LCS",
        "delta_cells": len(cells),
        "optimal_delta_cells": len(optimal_cells),
        "content_equal_pairs": sweep.content_pair_count,
        "delta_values_represented": len(cells),
        "optimal_delta_values_represented": len(optimal_cells),
        "roots_left": n,
        "roots_right": m,
        "limits": {
            "max_delta_cells": MAX_DELTA_CELLS,
            "max_optimal_cells": MAX_OPTIMAL_CELLS,
        },
    }
    return MatchResult(
        K=best,
        optimal_intervals=[tuple(x) for x in intervals],
        left_candidates=[tuple(sorted(s)) for s in left_cand],
        right_candidates=[tuple(sorted(s)) for s in right_cand],
        left_can_be_unmatched=left_unmatched,
        right_can_be_unmatched=right_unmatched,
        forced_pairs=tuple(sorted(forced)),
        left_candidate_delta=left_candidate_delta,
        right_candidate_delta=right_candidate_delta,
        left_unmatched_delta=left_unmatched_delta,
        right_unmatched_delta=right_unmatched_delta,
        witness_delta=witness_delta,
        witness_pairs=witness_pairs,
        stats=stats,
    )


def masks_at(left: list, right: list, offset: tuple, transit: tuple,
             delta: int) -> list[int]:
    """给定 delta，构造左根 × 右根的内容+时间边集掩码。"""
    u, v = transit
    n, m = len(left), len(right)
    masks = [0] * n
    groups: dict[tuple, list[int]] = {}
    for j, root in enumerate(right):
        groups.setdefault(root.content_key, []).append(j)
    for i, lroot in enumerate(left):
        idxs = groups.get(lroot.content_key)
        if not idxs:
            continue
        lo_ns = lroot.observed_ns + u - delta
        hi_ns = lroot.observed_ns + v - delta
        mask = 0
        for j in idxs:
            if lo_ns <= right[j].observed_ns <= hi_ns:
                mask |= 1 << j
        masks[i] = mask
    return masks
