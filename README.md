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
  capture_reconcile.py  捕获链对账：清单/观测校验、重传折叠、报告与 explain
  capture_match.py      对账的纯计算核心（delta 切点扫描 + 位并行 LCS
  flow/                  可恢复本地流式加工作业（asyncio + SQLite，见 flow/README.md）
tests/           unittest 模块（含对账与独立参考器）
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

---

# 采集范围准入（受控入口，本次新增能力）

> **权限边界声明：本次新增的范围准入能力只存在于受控入口
> `ControlledIngress`（及其 CLI `csubscribe` / `cpoll`）。**
> 旧的 `MqttIngress.ingest(SUBSCRIBE)` 与 `MqttIngress.poll` 保持原样、支持范围
> 不缩小；不限制能直接调用旧 API 或打开数据库文件的宿主管理员，也不给旧订阅
> 追溯撤权。本能力是一次性的订阅准入，不是权限管理服务器，不连接消息代理。

## 采集命名空间（受控入口的词汇表，刻意窄于通用 MQTT）

- 每层只由小写 ASCII 字母、数字、下划线、连字符组成（`[a-z0-9_-]+`）且非空；
  层间用 `/` 分隔；完整具体主题按 UTF-8 编码 **≤ 65535 字节**。
- 授权/请求过滤器每层可以是上述字面量或 `+`；最后一层还可以是 `#`。
  `+` 恰好占一层，`#` 占零层或更多层，所以 `plant/#` **包含 `plant` 本身**。
- 不接受空层（`a//b`、首尾 `/`）、系统主题/共享订阅前缀（`$...`、`$share/...`）
  或其他通配写法（`a+`、`a#`）。只有非空具体主题参与权限判断，`#` 不代表空主题。

## 策略文件（可读 JSON）

```json
{
  "version": 1,
  "principals": [
    {"principal_id": "team-plant", "groups": ["plant-team"]}
  ],
  "rules": [
    {"rule_id": "r1", "subject_type": "principal",
     "subject_id": "team-plant", "effect": "allow", "filter": "plant/+/#"},
    {"rule_id": "r2", "subject_type": "group",
     "subject_id": "plant-team", "effect": "deny", "filter": "plant/private/#"}
  ]
}
```

- `subject_type` 必须显式写 `principal` 或 `group`，**不能靠名字是否相同猜测**
  （身份名与组名可以同名，二者按类型区分）。
- 一个已登记身份的有效规则 = 直接规则 + 全部所属组规则。
- 裁定：任一有效 `deny` 命中即拒绝；无 deny 且至少一个 `allow` 命中才允许；
  其余默认拒绝（已登记但无规则的身份是正常的默认拒绝）。规则顺序无优先级。
- 未知身份、不存在的组引用、重复 `rule_id`/`principal_id`、类型不符、非法过滤器、
  未知字段、坏 JSON 都是**输入错误**，带 `location` 指出位置，绝不静默忽略后继续裁定。
- 策略规范化后计算 canonical JSON 的 SHA-256 摘要，随每个判断返回，便于同事用
  同一份策略重现。

## 两种可导入判断

```python
from offline_mqtt import load_policy_file, AdmissionEngine
engine = AdmissionEngine(load_policy_file("policy.json"))

# 1) 具体主题裁定
d = engine.decide_topic("team-plant", "plant/private/x")
# -> TopicDecision(allowed, allow_rule_ids, deny_rule_ids, reason, policy_summary)
#    reason: None（允许）| "DENY_RULE_MATCHED" | "NO_ALLOW_RULE_MATCHED"

# 2) 过滤器范围裁定（对过滤器表示的整个集合下结论，不枚举主题）
s = engine.decide_scope("team-plant", "plant/#")
# -> ScopeDecision(safe, witness, policy_summary, product_states_explored, ...)
#    safe=false 时 witness 给出一个真实、属于请求范围、且 decide_topic 确实拒绝的
#    具体主题，并标注它命中了哪些 deny 规则还是完全没有 allow 覆盖。
```

### 为什么不用枚举也能终止、反例为什么真实

把每个过滤器编译成一个 DFA（状态=已匹配层数，`#` 是接受态上的自环，`+` 是任意
层值转移，字面量是等值转移）。层值字符串虽无限，但转移只区分“等于某字面量”与
“其他任何合法层值（OTHER）”，故字母表有限。判定在“请求 DFA × 规则 DFA”的
**乘积 DFA** 上做最短路搜索：乘积状态/边数都有有限上界，`#` 的多层只是自环而不
产生无限新状态，因此必然终止——不采样、不枚举字符组合、不依赖当前 retained/ledger
里出现过哪些主题。OTHER 被还原成一个确定的、不属于任何策略字面量的合法层值；
返回前再跑一次 `decide_topic` 硬复核，确保 witness 真实、属于请求且确实被拒。

- **字节上限**：路径边权按真实字节计费，超过 65535 的路径直接剪枝；因此只能在
  超限长度出现的差异不算越权，也不会用超限串充当 witness。
- **资源上限**（`offline_mqtt/admission.py` 顶部可调）：规则数、过滤器深度、乘积
  状态/边数都有明确上限。触及上限抛 `AdmissionResourceLimit`，只报“无法处理”，
  绝不伪装成 `safe=true` 或普通越权。默认可正常处理 200 条规则、80 层路径。

## 受控订阅与读取（权限真正落地，而非静态展示）

```python
from offline_mqtt import MqttIngress, ControlledIngress, AdmissionEngine, load_policy_file
from offline_mqtt.generator import subscribe

with MqttIngress("mqtt.db") as ing:
    controlled = ControlledIngress(ing, AdmissionEngine(load_policy_file("policy.json")))
    # principal_id 由调用方认证后显式传入；绝不从记录的 session_id/client_id 推断
    result = controlled.controlled_subscribe(
        "team-plant", subscribe("any-session", ["plant/temp/#"]))
    if result.granted:
        sub_id = result.subscription_id          # 真实订阅 id
        controlled.controlled_poll(sub_id)       # 只交付采集命名空间内的消息
```

- 先验证整份输入（记录结构、过滤器合法性、重复过滤器=输入错误），再逐个过滤器做
  范围判断；**任何一项越权或输入无效都不创建订阅**——`subscriptions`、
  `subscription_retained`、`delivery_cursor` 不产生任何一行。
- 全部获准后才复用现有接入能力，在同一个立即写事务里建真实订阅，保留既有 retained
  初始交付与实时游标语义，返回实际 `subscription_id` 与逐过滤器准入依据。
- 受控读取只交付采集命名空间内的消息；库里其他用途的主题（大写、`$` 前缀、空层等）
  即便底层 `#` 匹配也不会混入，保留/实时交接语义不变。

## CLI（终端操作，输出均为 JSON）

```bash
python -m offline_mqtt.cli check-topic  POLICY PRINCIPAL TOPIC
python -m offline_mqtt.cli check-scope  POLICY PRINCIPAL FILTER
python -m offline_mqtt.cli csubscribe   DB POLICY PRINCIPAL FILTER [FILTER ...]
python -m offline_mqtt.cli cpoll        DB SUB_ID
```

**返回格式与退出码（区分三类结果）：**

| 情形 | JSON 形态 | 退出码 |
|---|---|---|
| 正常结论（含 `safe=false` / `allowed=false` 这种“越权但输入有效”） | 结论对象，`safe`/`allowed` 字段表达 | `0` |
| 输入/策略错误（未知身份、坏策略、非法过滤器、重复过滤器、坏帧） | `{"status":"error","error_kind":...,"errors":[{location,reason,detail}]}` | `2` |
| 资源不足，无法裁定 | `{"status":"error","error_kind":"resource_limit",...}` | `3` |

普通越权是**有效输入下的正常业务结论**（退出码 0，带可复核 witness）；输入错误与
资源不足是**无法给出裁定**的失败（退出码 2/3），二者不可混淆。

## 可复核演练与独立枚举器

- `demo/run_demo.py`：手工可复核的业务例子（两分支联合覆盖→加 deny 反例→换团队、
  深层/未见名称、空层、零层 `#`、同名身份/组、无规则身份、真实订阅与受控读取、
  混合请求全或无、坏策略/未知身份、200 规则/80 层、字节上限、资源上限），
  证据写入 `demo/evidence/demo_results.json`。
- `demo/cross_process_check.py`：在全新 `python -I` 进程里用同一策略复核判断与
  witness（摘要与结论逐字节一致），证据写入 `demo/evidence/cross_process.json`。
- `tests/_enumerator.py`：在缩小字母表 `{a,b}`、深度 ≤3 上**全枚举**具体主题的
  独立参照，用来核对正式乘积自动机的完整结果。它只是补充检查，不是正式判定算法。

## 新增/测试文件

```
offline_mqtt/
  namespace.py   采集命名空间严格校验（独立于旧 topic.py 的窄词汇表）
  policy.py      策略解析、带位置输入错误、canonical 摘要
  admission.py   具体主题裁定 + 过滤器范围裁定（乘积 DFA，非枚举）
  controlled.py  受控订阅/读取入口（权限边界）
tests/
  test_namespace.py / test_policy.py / test_admission.py
  test_controlled.py / test_cli_admission.py（子进程 -I）
  _enumerator.py 独立小枚举器（补充检查）
demo/            策略样例、可复核演练与跨进程复核脚本
```


---

# 捕获链对账（离线数据质量入口，本次新增能力）

> **权限与副作用边界：本能力只读两份已封存的来源账本与两份观测 JSONL，
> 将结果写成一份**新**报告文件。不新建订阅、不改授权、不改现有接收结果、
> 不连接任何消息代理。来源库以 `file:...?mode=ro` 只读 URI 打开，只执行
> `PRAGMA table_info` 与 `SELECT`，不调用会建表/改日志模式的 `MqttStore`。**

## 它回答什么问题

运维给出同一条采集转发链两端的捕获，需要核实“上游这条消息在下游有没有对应
记录”。两端的包号（`message_id`）、会话名（`session_id`）各自编号，时钟也有
偏差；同一正文可能只是正常的重复发布。因此：

- **不按摘要去重后相减**：`retry_of=null` 的每一行都是独立发布，即使 `dup=true`
  或包号/主题/正文完全相同，也不合并；跨 epoch 包号复用同样不合并。
- **不替每对消息各挑一个校时值**：同一次比较只有一个共同整数偏差
  `delta ∈ offset_ns` 加在右端时钟上，每一对都必须满足
  `transit_ns[0] ≤ R + delta − L ≤ transit_ns[1]`。

## 捕获顺序假设（合同适用边界）

本模块**只**适用于调用方明确指定的、具有下列顺序保证的一条有序转发链：

- 每次独立发布至多在另一端对应一次；
- 发布先后不颠倒；
- 允许丢失和单侧多出。

**不宣称任意 MQTT 部署都有这项性质。** 不满足该保证的捕获不能使用本模块的
结论去推断网络行为。

## 输入清单

```json
{
  "version": 1,
  "left":  {"capture_id": "cap-left",  "ledger": "left/ledger.db",
            "observations": "left/observations.jsonl"},
  "right": {"capture_id": "cap-right", "ledger": "right/ledger.db",
            "observations": "right/observations.jsonl"},
  "offset_ns": [-10, 0],
  "transit_ns": [0, 0]
}
```

- 相对路径按**清单所在目录**解析；两端 `capture_id` 必须不同。
- `offset_ns=[a,b]`、`transit_ns=[u,v]` 必须是整数闭区间，`a<=b`、`0<=u<=v`。
- 清单与观测都拒绝重复 JSON 键、额外字段、缺字段、坏引用；错误带
  `location`（文件 + 行号或 JSON 路径）。

观测 JSONL 每行**恰含**四个字段：

```json
{"event_seq": 1, "capture_epoch": "L1", "observed_ns": 0, "retry_of": null}
```

- `event_seq` 必须唯一对应本端 `ledger` 的一条记录；**每端 ledger 全部行都要有
  且只有一份观测**，遗漏后不会悄悄当作完整。
- `capture_epoch` 非空字符串；`observed_ns` 是有符号整数纳秒；`retry_of` 为
  `null` 或本端另一 `event_seq`。
- **以 `ledger.seq` 为本端顺序**（不是观测文件的行顺序、也不用浮点
  `ingested_at`）：按 seq 升序看，`observed_ns` 不得下降。
- 所有整数拒绝布尔值。来源库 `ledger` 表缺列/缺表也按输入失败处理。

## 重传折叠规则（只用采集方给出的确切证据）

`retry_of` 是采集方给出的**确切**重传证据，只有全部满足才折叠：

1. 指向本端**更早**的 `event_seq`，且该目标本身 `retry_of=null`（根）；
2. 两行 `capture_epoch`、`session_id`、`qos`、`message_id`、`topic`、
   `payload` 原字节、`retain` 全同；
3. `qos ∈ {1,2}`；重传行 `dup=true`；
4. 不接受链式 `retry_of`。

根与它的全部重传行构成**一次**发布，使用**根**的时间与次序，并保留全部原始
行定位。没有明确引用的单侧重复只能作为独立业务出现参与对账；本模块不声称
已证明它是网络重传。

## 配对与“最多可解释发布数”

- 内容条件**只有**：`topic` 原字符串、`payload` 原字节、`retain` 相等。
  不做 JSON 解析、Unicode 规范化或文本转码。`qos`/`message_id`/`session_id`
  是端内证据，不要求跨端一致。
- 配对方案 = 根之间**不交叉**的一对一关系：左端位置递增时右端位置严格递增。
- 在所有合法 `delta` 与配对方案中求最大配对数 **K**。

### 为什么完整且不拼凑局部方案

固定一对内容相等的根，使它成为合法边的 delta 集合是整数闭区间
`[L−R+u, L−R+v] ∩ [a,b]`。所有这样的区间端点把 delta 轴切成有限段；**同一段内
每个整数 delta 的边集完全相同**，因此该段的最大不交叉配对数恒定。于是：

1. 枚举切点段（数量 ≤ 2·内容相等对数 + 2），**不逐纳秒扫描、不抽样**；
2. 每段用位并行 LCS（Hyyrö 半字节位向量）精确求出配对数；
3. `K` = 所有段的最大值；最优 delta = 所有 K 段在整数轴上的并集，按相邻整数
   合并成有序闭区间，不做最小/最大包络填充；
4. 每个根的“候选对端 / 可不配 / forced”都**逐段**计算后再合并：
   - 候选 = 某个最优段中存在达到 K 的方案使用该边；
   - `can_be_unmatched` = 某个最优段中存在达到 K 的方案不使用该根；
   - `forced_pair` = 在所有最优段中，该对端都是该根唯一可能对端。
5. 每个局部结论都来自**单个 delta 下的一个完整方案**；合并时只做并/交，从未
   把两个不同 delta 的边拼进同一个方案。证据里的任一实例也只有一个 delta。

### 结论的边界

- `forced_pair`：同一对出现在**每一个**最优方案中。
- 只有一个候选、但该根也可能不配（`single_candidate_but_unmatchable`）：
  **不能**称确定对应。
- `no_counterpart_in_capture`：任何最优方案都不配该根。这只说明**指定捕获与
  合同内**没有对应证据，**不等于**证明网络真实丢包。
- `constrained_out`：受时间/顺序限制，该根无法被解释为任何对端。

## 规模与资源上限（写在代码里，超限明确失败）

| 上限 | 默认值 | 位置 |
|---|---|---|
| 单端独立发布（根） | 5000 | `capture_reconcile.MAX_ROOTS_PER_SIDE` |
| 单端原始行 | 200000 | `capture_reconcile.MAX_RAW_ROWS_PER_SIDE` |
| delta 切点段数 | 200000 | `capture_match.MAX_DELTA_CELLS` |
| 参与细算的最优段数 | 2048 | `capture_match.MAX_OPTIMAL_CELLS` |
| explain 单根重建方案数 | 8 | `capture_reconcile.MAX_EXPLAIN_SCENARIOS` |

任一上限触发都抛 `ReconcileResourceLimit`（CLI 退出码 3），只报“未能完成”，
**绝不**输出貌似完整的 K 或确定缺口。整份已封存捕获允许装入内存。

## 可导入接口

```python
from offline_mqtt import (
    reconcile, write_report, load_report, summarize_report,
    explain_report, verify_report, ReconcileInputError,
)

report = reconcile("manifest.json")              # 纯计算，不写文件
path = write_report(report, "report.json")       # 目标已存在则 ReportExistsError
summary = summarize_report(load_report(path))    # K / 区间 / 计数 / 结论
explanation = explain_report(load_report(path), "left", seq=1)
assert verify_report(load_report(path))["ok"]    # 不读来源的离线自检
```

`reconcile` 只读来源；`write_report` 用同目录临时文件 + 原子硬链接（或排他
创建），失败不会留下可正常打开、声称完整的半份报告。

## CLI

```bash
python -m offline_mqtt.cli reconcile MANIFEST --out REPORT      # 新报告，已存在则退出码 4
python -m offline_mqtt.cli reconcile MANIFEST --dry-run         # 只打印摘要
python -m offline_mqtt.cli summary REPORT
python -m offline_mqtt.cli explain REPORT --side left --seq 1
python -m offline_mqtt.cli explain REPORT --side right --position 0
```

| 情形 | 退出码 |
|---|---|
| 成功（含 K=0 这类正常结论） | 0 |
| 输入/来源/清单/观测错误 | 2 |
| 资源不足，未能完成 | 3 |
| 目标报告已存在（拒绝覆盖） | 4 |

## 报告内容与离线复核

`report.json` 先给两端原始条数、重传折叠数、独立发布数、`K`，再给：

- `optimal_delta_intervals`：达到 K 的全部 delta，有序闭区间；
- `optimal_witness`：一份可复核的最优配对实例（实际 delta + 逐对时延），
  同一输入重复执行稳定；
- `roots.left` / `roots.right`：每个根的全部候选、每对候选成立的一个 delta、
  `can_be_unmatched`、`forced_pair`、`counterpart_status`，以及根 + 重传行的
  原始 `seq` 回指（每根只消耗一次）；
- `retransmissions`：每一组根/重传关系的完整证据；
- 主题、`payload` 原字节表达（hex + base64 + 长度，可解码时附 UTF-8）、
  `observed_ns`、输入摘要（清单 sha256、来源库/观测 sha256）与所用条件；
- **不含可写来源连接**：来源只以 sha256 记录，关掉并移走两端后仍可复核。

`explain` 针对一个根展示不同候选或“可不配”结论：若能形成两种以上结论，给出
至少两个确实达到 K、各自内部共用同一 delta 的完整方案（不同方案可用不同
delta），并对每个方案做独立复核。

## 演练与测试

- `demo/run_reconcile_demo.py`（`python3 -I demo/run_reconcile_demo.py`）：
  先用现有接入器生成两份真实 ledger，再写独立观测材料运行新入口。覆盖用户
  指定的关键例子（K=1、两个单点 delta）、同刻 A,B,C 对 B,C,A（K=2 保留
  B/C）、双同正文根 + 明确重传、跨 epoch 包号复用、无引用 dup、跨端不同包号、
  任意二进制正文、空载荷 retain 删除、闭区间端点、10^30 量级时间基数、
  完全不相容、一端为空、五类真实失败（并核对来源与旧报告逐字节不变）、
  独立参考器核对与 200 根 + 巨大 delta 范围。证据写入
  `demo/reconcile_evidence/`。
- `tests/reference_reconciler.py`：另写的很小参考器，**全枚举**所有顺序配对与
  小范围 delta，不调用正式求解函数；`tests/test_capture_reconcile.py` 用它核对
  K、最优 delta 集合、每根候选与可不配标志，并验证 200 根生产路径没有枚举
  时间值。
- `tests/test_capture_reconcile.py`：25 个用例，含来源只读、目标拒绝覆盖、
  资源上限、报告在来源移除后的离线复核。

---

# 按描述符删除字段的离线外发（本次新增能力）

> **边界声明：本能力只是一个离线外发入口。** 它以只读方式打开一份**已封存**
> 的接入账本，把配置列出的主题按描述符删除指定身份字段后，写成一个
> **此前不存在的新目录**。不涉及订阅准入，不涉及两端对账，不修改接入器
> 已有行为，不连接消息代理，运行时不联网，也不依赖机器上预装 protoc
> （描述符只用二进制 `FileDescriptorSet`，protobuf 运行库随仓库 vendor 在
> `vendor/protobuf/`，环境已装 protobuf 时优先用环境版本）。

## 外发范围

- 只导出配置逐项列出的**精确主题**；未列入配置的主题一律不交付。
- 选中记录按 `ledger.seq` 升序逐条流式处理，**保留每次原始发布**：
  不按 `message_id`、`dup` 或相同正文折叠。
- 每行给出一个可交给现有接入 API 的 PUBLISH 记录（`payload_base64`、
  来源 `event_seq`、本次使用的 `message_type`，以及原样的
  `topic/qos/retain/dup/message_id/session_id`——保留原始会话身份，
  不新造认证）。
- 整份来源、配置、描述符及全部选中正文都核验成功才算交付；任何一条
  失败则整批不交付（见“失败含义”）。

## 为什么按“线上每次出现”删除，而不是解码后删

直接把 Protobuf 解成对象再序列化，会丢失重复出现、原始 varint 编码与
字段顺序；而只删除解码后的最终值，又会让**早先被覆盖的身份值**留在线上
字节里（singular 字段先旧后新的旧值、oneof 中后来被覆盖的成员、singular
message 前一段出现里的字段，解码器都会合并掉，但字节还在）。因此本入口
在 **wire 层逐字段**工作：命中字段的每一次实际出现都整段移除
（tag + 长度前缀 + 内容），singular message 的每段出现分别应用路径，
不做对象合并。

## 字节保真合同

- 未删除字段的原始 tag、数值编码（含非规范但合法的 varint）、先后顺序与
  每次出现都逐字节保留；
- 只有**内部发生删除**的长度定界 message 才重算自己的长度前缀（规范
  varint）；没有变化的容器连原长度前缀也逐字节不变；
- 删完后嵌套 message 即使为空，也保留父字段的存在（长度 0）；
- 绝不用对象序列化替代这份字节合同；输出只由未删字节与重算的长度前缀
  拼成。

## 删除路径（remove_paths）

路径是字段号数组，从根消息开始；中间每项必须是 message 字段，最后一项
整个删去。对 repeated message，路径作用于每个出现。map 按描述符里的
合成 entry 处理（key=1、value=2）：可删整个 map 字段；value 为 message
时可写成 `[map, 2, ...]` 继续向下删，或 `[map, 2]` 把每个 entry 的 value
字段整个删掉；**不允许单独删 map 的 key**，也不允许沿标量 map value
向下。路径必须在描述符图上真实存在（即使本批没有相应消息），穿过标量
或不存在的路径在配置阶段即拒绝。父路径与子路径同时列出按父路径覆盖；
清单次序与重复路径不改变结果。

## 严格核验（任一不满足即整批拒绝）

- 原始正文按 wire 层逐字段核验：tag、varint、定长数值、长度与消息边界；
  字段号 0、越界 tag、不完整 varint、错误 wire type、越界 length、坏
  packed 数值、非法 UTF-8 string 都拒绝。string 与 bytes 严格区分，
  bytes 不按 UTF-8 解读。repeated 可打包数值接受 packed 与 unpacked
  两种合法表示。
- **任何一层**出现描述符不认识的字段都拒绝本次外发——不静默放行未知
  数据，也不默默丢掉后宣称只删了指定字段。
- 错误只提供来源序号（`event_seq`）、字段号路径、字节位置与类别，
  不回显正文或字段值。
- 资源上限（单条字节数、嵌套深度、字段出现数，配置 `limits` 或 CLI
  `--max-*` 可调）在分配或递归前检查；超限返回“未完成”，不导出半条
  消息。

## 支持子集（明确不支持的不是“普通 bytes”）

仅 **proto3** 描述符：普通标量、嵌套 message、repeated、packed 数值、
oneof、map、proto3 optional、递归消息。以下在描述符载入时明确报
`descriptor_error`，绝不当作普通字节放行：proto2 或未声明 syntax、
extension/extension_range、group、required 字段、缺失依赖、同名符号
冲突、不可解析的类型引用、字段号 0/越界/落在保留段/同一消息内重复、
畸形 map entry（不是恰好 key=1/value=2）。目录中类型引用必须完整可
解析（纯 Python 连接池对符号冲突与悬空引用只惰性报错，本入口在载入时
自行核验）。

## 输出目录与失败含义

输出是一个**此前不存在**的新目录：

| 文件 | 内容 |
|---|---|
| `messages.jsonl` | 每行一条 PUBLISH 记录（`payload_base64` 为输出字节） |
| `report.json` | 描述符/配置摘要、选中/改动记录数、各删除路径命中的线上出现数、每条记录输入/输出长度与删除的原始半开字节区间 |
| `COMPLETE.json` | 完成标记：前两者的 sha256 与大小、记录数 |

报告**不保存**被删字节、原正文副本或可逆映射（长度与路径本身不隐藏）。
只有三个文件齐备且摘要吻合，`inspect` 与读取接口才认这份导出。

```bash
python -m offline_mqtt.cli export  LEDGER --descriptor FDS --config CONFIG --out DIR
python -m offline_mqtt.cli inspect EXPORT_DIR
```

| 情形 | 退出码 |
|---|---|
| 成功 | 0 |
| 描述符/配置/正文非法（含坏字段、未知字段） | 2 |
| 资源超限（未完成） | 3 |
| 输出目录已存在 | 4 |
| 来源账本或写出 IO 失败 | 5 |
| 导出目录不完整或核验不符（inspect） | 6 |

任何失败都**不留**可被 `inspect` 认为完整的结果：中途失败会移除整个
输出目录；即使残留残骸，缺少 `COMPLETE.json` 或摘要不符也会被拒绝。
来源账本以 `file:...?mode=ro&immutable=1` 打开（要求已 checkpoint、
无热 WAL，否则明确报错），源表、原正文、原有订阅与旁边文件保持原样。

## inspect 与读取接口

- `inspect` 只依赖导出目录本身：核对完成标记与两个交付文件的 sha256、
  导出条数、每条输出正文与报告中长度/序号的对应关系、改动计数一致性。
  源库与配置移开后仍能核实。
- 读取接口 `offline_mqtt.pb_reader.iter_export(EXPORT_DIR)` 把
  `payload_base64` 还原为 `bytes`，`to_ingest_record()` 产出可直接交给
  `MqttIngress.ingest` 的记录，重新接入后保持
  topic/qos/retain/dup/message_id 与原始 session_id。

## 可调用 API

```python
from offline_mqtt import (
    export_ledger, inspect_export, iter_export,
    load_descriptor_set, validate_remove_path, rewrite_payload, Limits,
    ExportError,
)

summary = export_ledger("ledger.db", "descriptor.bin", "config.json", "out/")
check = inspect_export("out/")                 # 不完整则抛 ExportError
for rec in iter_export("out/"):                # 流式，不把载荷装入列表
    ingress.ingest(rec.to_ingest_record())
```

规模：单条受限消息与描述符驻内存；选中记录逐条流式处理，不把全部载荷
装入列表。实跑 12.8 万条入账（12.5 万条选中）导出约 20 秒，导出进程
峰值内存约 27 MB。

## 演示与测试

- `demo/run_pb_export_demo.py`：手写 wire hex 与独立预期输出（敏感
  singular string 连续两次 + 非规范 varint、127/128 长度边界、同一
  message 分两段出现、oneof 旧成员、repeated message、map 的 message
  value、packed/unpacked 混用、空容器、递归描述符），逐字节核对未删
  区间并用官方动态消息复核输出语义；12.8 万条真实导出（展示峰值内存
  与输出首尾记录）；导出经读取接口接入空库核对发布次数与载荷；未知
  字段、错误 wire type、截短正文、非法 string、map key 删除、坏描述符
  依赖、最后一条损坏、目标已存在等真实拒绝证据；来源与既有文件未动。
  证据写入 `demo/pb_export_evidence/`。
- `tests/test_pb_wire.py` / `test_pb_descriptor.py` / `test_pb_export.py`
  / `test_pb_cli.py`：手写 hex 用例、描述符子集与路径校验、端到端
  导出/inspect/重新接入、CLI 退出码。

```
offline_mqtt/
  pb_errors.py       错误类别与退出码（不回显正文）
  pb_descriptor.py   FileDescriptorSet 载入校验、删除路径校验与计划编译
  pb_wire.py         wire 层逐字段核验与字节级删除（本能力的核心）
  pb_export.py       只读来源 → 流式导出 → 原子完成标记
  pb_inspect.py      导出目录离线核验
  pb_reader.py       导出文件读取接口（还原 bytes，对接现有接入 API）
  pb_export_cli.py   export / inspect 命令实现
  _protobuf_vendor.py  vendored 纯 Python protobuf 的 sys.path 引导
vendor/protobuf/     官方 protobuf 纯 Python 运行库（无 protoc、不联网）
```

## 可恢复的本地流式加工作业（offline_mqtt.flow）

在只读 MQTT 账本上跑 DAG 流式加工（source / scale / sum / union / sink），
边是带正整数容量的有界 FIFO 通道，满了背压；作业状态与检查点落在独立 SQLite。

- **一致检查点**：Chandy–Lamport 屏障沿有界边对齐，节点在首个屏障封存算子
  状态、把入边未处理贡献快照为在途；协调器在单个立即写事务里把源位置、sum
  状态、边在途、sink 累计截面与指标原子提升。每条贡献在截面内要么效果已进入
  封存状态，要么作为在途保存、恢复先重放，不重不漏。
- **运行中检查点**：控制入口 `request_checkpoint` 不持执行锁，可由另一进程
  调用；源在屏障后继续向有容量通道发数，不被统一停到检查点完成。
- **恢复**：fcntl 排他锁保证同作业单执行者；重启只认最近完整检查点，清理
  collecting 残留，从保存的源游标之后继续、先重放在途；绑定摘要（图/算子
  参数/源名称路径/固定范围）不符在读取前拒绝。
- **可见性**：sink 成品只在完整检查点可见，收集期暂存对外不可见；所有 sink
  与 sum 状态按同一检查点编号查询。
- 详见 `offline_mqtt/flow/README.md`；验收见 `tests/test_flow_*.py`
  （图校验、端到端正确性、屏障交错、SIGKILL 恢复、绑定/锁/写入故障、
  有界内存与水位、CLI）。
