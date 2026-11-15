# 离线 MQTT 消息接入器

完全离线的 MQTT 协议帧接入器：不连接任何代理，输入是“项目定义记录”
（普通 dict）形式的 `CONNECT` / `PUBLISH` / `SUBSCRIBE` / `DISCONNECT` 帧，
持久化基于 Python 标准库 SQLite，零第三方依赖。

## 能力概览

- **主题过滤**：单层通配 `+`（恰好一层）与多层通配 `#`（零到多层，只能在末层且独占一层）。
- **保留值（retain）**：
  - 同一主题在 `retained` 表中永远只有一行（当前版本，版本号单调递增）；
  - 新订阅建立时，在**同一个立即写事务**内固化匹配主题的当前保留值为初始视图，
    每个主题恰好投递一次；
  - 空载荷的保留发布清除该主题保留值（清除事件本身仍留在事件账本）。
- **事件账本**：仅追加（append-only），`seq` 单调递增即全局到达次序；
  普通实时消息不去重，DUP 重传同样按到达次序入账。
- **身份区分**：会话身份（`session_id`）、主题（`topic`）、消息身份（`message_id`）
  三类字段分别存储与查询；同 `message_id` 不同会话是相互独立的事件。
- **非法帧隔离**：结构错误、发布主题含通配符、非法通配位置、超长主题、
  QoS/message_id 越界等均写入 `quarantine` 表，不污染账本与保留表。
- **进程重启**：所有状态（账本、保留值、订阅、初始保留视图投递标记、实时游标）
  均在 SQLite 中，重新打开数据库后继续工作；WAL + `synchronous=FULL`。

## 并发正确性（核心不变式）

订阅建立与 PUBLISH 并发时，保证每个事件**不漏、不重**：

```
BEGIN IMMEDIATE
  cutoff = MAX(ledger.seq)          -- 截断序号
  snapshot = SELECT * FROM retained -- 同一事务快照
  写入 subscriptions / subscription_retained（初始保留视图）/ delivery_cursor(cutoff)
COMMIT
```

任何并发 PUBLISH 只能落在事务的一侧：

| PUBLISH 提交时机 | 去向 | 判定 |
|---|---|---|
| 先于订阅事务提交 | 初始保留视图（seq ≤ cutoff） | 不再进实时流，不重复 |
| 后于订阅事务提交 | 实时事件流（seq > cutoff） | 不进初始视图，不漏 |

PUBLISH 自身也在单个立即写事务内同时写账本与保留表，因此外部观察到的
“到达次序”与“当前保留值”始终来自同一个已提交状态。互斥由两层保证：
接入器进程内 `RLock` 串行化；多进程/多实例共享库文件时由 SQLite
`BEGIN IMMEDIATE` 文件锁 + `busy_timeout` 串行化。

## 目录结构

```
offline_mqtt/
  topic.py       主题/过滤器校验与通配匹配
  frames.py      帧 dataclass 与记录解析（结构校验）
  store.py       SQLite schema 与数据访问（全部事务化）
  ingress.py     接入器外观：ingest / poll / drain / 检查接口
  generator.py   本地帧生成器（脚本式 + 可控随机，离线）
  cli.py         跨进程 seed/dump/poll 命令
tests/           6 个 unittest 模块，43 个用例
run_tests.py     隔离模式（python -I）测试运行器
```

## 快速上手

```python
from offline_mqtt import MqttIngress
from offline_mqtt.generator import (
    connect, publish, retained_publish, retained_clear, subscribe,
)

with MqttIngress("mqtt.db") as ing:
    ing.ingest(connect("sensor-01"))
    ing.ingest(retained_publish("sensor-01", "sensors/temp", "20.1",
                                qos=1, message_id=1))
    sub = ing.ingest(subscribe("sensor-01", ["sensors/+"]))

    ing.poll(sub.subscription_id)      # 首次：初始保留视图（retain=True）
    ing.poll(sub.subscription_id)      # 保留视图已耗尽，返回实时事件

    ing.ingest(retained_clear("sensor-01", "sensors/temp", message_id=2))
    print(ing.list_retained())         # 该主题保留值已被清除
```

### 本地帧生成器

```python
from offline_mqtt.generator import LocalFrameGenerator

g = LocalFrameGenerator(seed=42)
burst = g.random_burst(["s1"], ["a/b", "a/c"], 100,
                       retain_ratio=0.3, invalid_ratio=0.1)
results = ing.ingest_many(burst)      # 非法帧自动进入隔离区
```

### CLI（跨进程验证重启持久性）

```bash
python -m offline_mqtt.cli seed DB_PATH --sub 'sensors/#'
python -m offline_mqtt.cli dump DB_PATH
python -m offline_mqtt.cli poll DB_PATH 1
```

## 运行测试

无第三方依赖，使用标准库 unittest：

```bash
python3 run_tests.py                 # 常规
python3 -I run_tests.py              # 隔离模式（推荐，防止路径劫持）
```

测试覆盖：

1. `test_topic.py` — 通配匹配边界、非法通配位置、字节级长度上限；
2. `test_retained.py` — 保留值替换版本唯一、空载荷清除、新订阅初始视图、过滤器选择性；
3. `test_normal_messages.py` — 到达次序、DUP 重复消息入账、过滤、游标推进、身份区分；
4. `test_concurrency.py` — 同进程多线程与多实例文件锁两种并发形态，
   校验不漏（cutoff 后匹配事件全在实时流）、不重（保留视图与实时流序号不重叠）；
5. `test_quarantine.py` — 各类结构/语义非法帧隔离且不影响合法处理；
6. `test_restart.py` — 同进程重开 + `subprocess` 独立进程（`python -I`）双重重启验证。

## SQLite 表

| 表 | 用途 |
|---|---|
| `sessions` | 会话身份与连接状态 |
| `ledger` | 仅追加事件账本（全局到达次序） |
| `retained` | 每主题唯一当前保留值（主键 topic，version 递增） |
| `subscriptions` / `subscription_filters` | 订阅与过滤器 |
| `subscription_retained` | 订阅建立时固化的初始保留视图（含投递标记） |
| `delivery_cursor` | 每订阅实时事件投递游标（重启续投） |
| `quarantine` | 非法帧隔离区（原因码 + 原始帧 JSON） |
