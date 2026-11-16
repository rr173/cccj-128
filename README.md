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
  capture_match.py      对账的纯计算核心（delta 切点扫描 + 位并行 LCS）
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
