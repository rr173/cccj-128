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

