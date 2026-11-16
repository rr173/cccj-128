"""可复现节点交错的同步关卡（测试夹具，产品代码之外不产生任何副作用）。

关卡只做两件事：让产品流程在**真实同步点**上等待，以及让测试线程放行。
它绝不替产品保存状态、补结果或修改任何作业数据；未配置关卡时所有调用都是
空操作，生产运行零开销。

支持的关卡点（均位于运行时真实状态迁移处）::

    source.post_cut.<source>     源在屏障截面之后又投递了一条新记录
    node.first_barrier.<node>    节点收到本检查点首个屏障、封存状态之后
    node.recorded.<node>         节点把一条“截面之后”的记录记入在途之后
    sink.staged.<sink>           sink 暂存了一行（检查点尚未完成）
    commit.partial_state         检查点事务提交之后、运行者继续收尾之前
    commit.after_persist         完成标记已持久化、控制端收到成功之前

两种动作：
- ``wait``：产品流程在该点阻塞，直到测试调用 :meth:`release`；
- ``raise``：产品流程在该点抛出 :class:`GateAbort`，模拟写入失败/进程故障。

跨进程使用时关卡以文件为媒介（CLI 子进程通过 --gates 配置 JSON、
--gates-dir 指定状态目录），同进程使用时直接共享对象、用两个独立事件
区分“已命中”和“已放行”。
"""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass, field
from typing import Optional


class GateAbort(RuntimeError):
    """关卡要求产品流程在该同步点中止（用于真实写入失败演练）。"""


@dataclass
class GateConfig:
    waits: set[str] = field(default_factory=set)
    raises: set[str] = field(default_factory=set)
    # point -> 前 skip 次命中直接放行，之后才真正等待（用于两段式交错）。
    wait_after: dict[str, int] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: Optional[dict | str]) -> "GateConfig":
        if raw is None:
            return cls()
        if isinstance(raw, str):
            raw = json.loads(raw)
        return cls(
            waits=set(raw.get("wait", [])),
            raises=set(raw.get("raise", [])),
            wait_after=dict(raw.get("wait_after", {})),
        )


class Gates:
    """同步关卡注册表。"""

    def __init__(self, config: Optional[GateConfig] = None, state_dir: Optional[str] = None):
        self.config = config or GateConfig()
        self.state_dir = state_dir
        # point -> 当前阻塞在 hit() 上、等待放行的 future（产品侧）。
        self._blocked: dict[str, set[asyncio.Future]] = {}
        # point -> 等待“命中发生”的通知 future 集合（测试侧 wait_hit）。
        self._watchers: dict[str, set[asyncio.Future]] = {}
        # 自动放行点：命中即放行但仍通知 watcher，供命中次数不确定的交错使用，
        # 由产品事件循环同步处理，无轮询竞态。
        self._auto_release: set[str] = set()
        # 本事件循环内至少命中过一次的点（供 wait_hit 处理“先命中后等待”）。
        self._ever_hit: set[str] = set()
        self._hit_counts: dict[str, int] = {}
        # 所属事件循环；跨 asyncio.run 复用 Gates 时旧循环已关闭，需重置。
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def enable_auto_release(self, point: str) -> None:
        """启用某点的自动放行（仍可通过 wait_hit/wait_first_hit 观察命中）。"""
        self.config.waits.add(point)
        self._auto_release.add(point)

    def _on_loop(self) -> None:
        loop = asyncio.get_running_loop()
        if self._loop is not loop:
            self._loop = loop
            self._blocked = {}
            self._watchers = {}
            self._hit_counts = {}
            self._ever_hit = set()

    def _path(self, point: str, suffix: str) -> str:
        assert self.state_dir is not None
        return os.path.join(self.state_dir, f"gate_{point}.{suffix}".replace("/", "_"))

    async def hit(self, point: str) -> None:
        """产品流程在真实同步点调用：按配置抛错或等待放行。"""
        if point in self.config.raises:
            raise GateAbort(f"关卡 {point!r} 注入故障")
        if point not in self.config.waits:
            return
        self._on_loop()
        count = self._hit_counts.get(point, 0) + 1
        self._hit_counts[point] = count
        # wait_after：前 N 次命中直接通过，第 N+1 次起才真正阻塞。
        if count <= self.config.wait_after.get(point, 0):
            return
        if self.state_dir is not None:
            with open(self._path(point, "hit"), "w", encoding="utf-8") as fh:
                fh.write(str(count))
            release_path = self._path(point, "release")
            while not os.path.exists(release_path):
                await asyncio.sleep(0.005)
            return
        # 通知所有测试侧 watcher：本次命中已发生。
        for watcher in list(self._watchers.get(point, ())):
            if not watcher.done():
                watcher.set_result(None)
        self._watchers[point] = set()
        # 自动放行点：同步立即放行，不阻塞产品流程（无轮询竞态）。
        if point in self._auto_release:
            self._ever_hit.add(point)
            return
        # 产品侧阻塞，直到 release 唤醒本次命中。
        release_future = asyncio.get_running_loop().create_future()
        self._blocked.setdefault(point, set()).add(release_future)
        self._ever_hit.add(point)
        await release_future

    def release(self, point: str) -> None:
        """放行一次（同进程唤醒当前命中；跨进程写 release 文件）。"""
        if self.state_dir is not None:
            with open(self._path(point, "release"), "w", encoding="utf-8") as fh:
                fh.write("1")
            return
        for future in list(self._blocked.get(point, ())):
            if not future.done():
                future.set_result(None)
        self._blocked[point] = set()

    def arm_wait(self, point: str) -> None:
        """运行中动态启用一个等待关卡（用于先让某检查点完成、再阻塞后续处理）。"""
        self.config.waits.add(point)

    def arm_raise(self, point: str) -> None:
        """运行中动态启用一个故障关卡。"""
        self.config.raises.add(point)

    async def wait_hit(self, point: str, timeout: float = 5.0) -> None:
        """测试端等待产品流程真正到达该同步点（限时断言，不依赖长 sleep）。"""
        if self.state_dir is not None:
            hit_path = self._path(point, "hit")
            deadline = asyncio.get_running_loop().time() + timeout
            while not os.path.exists(hit_path):
                if asyncio.get_running_loop().time() > deadline:
                    raise TimeoutError(f"关卡 {point!r} 在 {timeout}s 内未被命中")
                await asyncio.sleep(0.005)
            return
        self._on_loop()
        # 基于命中计数做确定性等待：注册后先复查一次（处理“先命中后等待”），
        # 再挂到 watcher 上；计数只增不减，不会漏掉任何一次命中。
        if self._hit_counts.get(point, 0) > 0 or point in self._ever_hit \
                or self._blocked.get(point):
            return
        watcher = asyncio.get_running_loop().create_future()
        self._watchers.setdefault(point, set()).add(watcher)
        try:
            # 注册后再复查，消除注册与 hit 通知之间的窗口。
            if self._hit_counts.get(point, 0) > 0:
                return
            await asyncio.wait_for(watcher, timeout=timeout)
        finally:
            self._watchers.get(point, set()).discard(watcher)


def noop_gates() -> Gates:
    return Gates(GateConfig())
