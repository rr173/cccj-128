# 可恢复的本地流式加工作业（offline_mqtt.flow）

Python + asyncio + SQLite 实现的有向无环图（DAG）流式加工，输入来自**只读**
打开的现有 MQTT 账本，作业状态与检查点写入独立的作业库。核心目标：长作业被
终止后，能从最近一次**完整检查点**一致恢复，正在加工的数据、算子状态、结果
三者不重不漏。

## 1. 图模型与算子

- 节点：`source`（无输入）、`scale`（单输入，整数因子）、`sum`（单输入，按
  topic 维护 count/total）、`union`（≥2 输入，不去重）、`sink`（恰 1 输入，
  无输出）。
- 边：唯一命名、正整数**容量**的有界 FIFO 通道；满了 `put` 挂起形成背压，
  业务数据不会随总输入量无限驻留内存。
- 每经过一条边，边名追加到贡献的 `route`；广播产生不同 route 的独立贡献，
  union 合流不去重，相同字节/相同 seq 的输入不合并。
- 坏图（重名、环、悬空边、错误输入数、到不了 sink、容量非正整数、scale 因子
  非整数）在打开任何源账本之前即被拒绝。

## 2. 一致检查点是怎么形成的

不是分别保存各节点的瞬时状态，而是 Chandy–Lamport 式的**屏障对齐**：

1. 协调器把同一个 `Barrier(cp_id)` 注入每个 source 的控制队列；源把截面位置
   之前的数据全部投递后沿出边发屏障，然后**立即继续**向仍有容量的边投递截面
   之后的新记录——源不会被统一停到检查点完成。
2. 屏障与贡献同走有界 FIFO 边，边内次序确定。
3. 节点在首个屏障到达时封存算子状态（sum 的 topic 字典深拷贝）；对每条入边，
   把“pump 已取出但未处理的 held 项 + 槽位项”中的**贡献**只读快照为在途，
   控制标记（屏障/EOS）不被冻结、继续走节点逻辑。
4. 协调器收齐全部节点报告后，在**单个 SQLite 立即写事务**里把源位置、sum
   状态、边在途、sink 累计截面与指标一起原子提升为完整检查点。

因此每条贡献在截面内非此即彼：其效果已进入封存的下游状态，**或者**它作为某
条边的在途项被保存、恢复后先于任何新读取被重放——不会两边都有或都没有。

- 检查点编号单调；已完成编号永不被未完成的新编号覆盖。
- sink 成品只在完整检查点可见；收集期间的暂存行（`sink_staging`）对外不可见，
  重启时整批清理。
- 全部输入处理完才形成最终检查点，作业随后标记 finished。

## 3. 恢复语义

- 启动时获取 fcntl 排他执行锁（`flow_locks/`），同作业第二执行者显式失败；
  进程被 SIGKILL 时内核自动释放锁。控制入口（请求检查点、查询）不持锁。
- 重启只认最近完整检查点：更早的 collecting 行与其暂存一律作废。
- 从快照重建 sum 状态、源游标，并把边在途贡献注回原边（先于任何新读取）。
- 源从保存位置之后继续读固定区间 `start_after < seq <= end_seq`，不重读已
  封存前缀，也不重新取建作业后追加的尾部。
- 绑定（图、算子参数、源名称/路径/过滤器/固定范围）的 SHA-256 摘要不一致时，
  在任何业务读取之前拒绝恢复。

## 4. 公共 API

```python
from offline_mqtt.flow import (
    create_job, run_job, request_checkpoint, checkpoint_status,
    get_results, get_sums, get_snapshot, job_diagnostics, create_ledger,
)

create_job("job.db", "job-1", graph_dict, source_dicts)      # 固定各源尾部
await run_job("job.db", "job-1")                              # 运行/恢复
request_checkpoint("job.db", "job-1", wait=True)              # 另一控制入口
cp_id, rows = get_results("job.db", "job-1", sink="s1")       # 仅完整截面
cp_id, sums = get_sums("job.db", "job-1")                     # 同编号 sum
```

## 5. 终端入口

```bash
python -m offline_mqtt.flow.cli create  JOB_DB JOB_ID --graph graph.json --sources sources.json
python -m offline_mqtt.flow.cli run     JOB_DB JOB_ID [--gates gates.json --gates-dir DIR]
python -m offline_mqtt.flow.cli request JOB_DB JOB_ID [--wait] [--timeout SEC]
python -m offline_mqtt.flow.cli status  JOB_DB JOB_ID
python -m offline_mqtt.flow.cli results JOB_DB JOB_ID [--sink SINK]
python -m offline_mqtt.flow.cli sums    JOB_DB JOB_ID
python -m offline_mqtt.flow.cli diag    JOB_DB JOB_ID [--run-id N]
```

退出码：0 成功；2 输入/绑定/图错误；3 执行锁被占用；4 运行期失败（可恢复）。

## 6. 恢复演练（跨进程 SIGKILL）

`Gates` 是测试/演练夹具，只在产品的真实同步点等待或注入故障，绝不替产品保存
状态。跨进程时以文件为媒介（`--gates`/`--gates-dir`）：

```bash
echo '{"wait": ["node.first_barrier.u"]}' > gates.json
python -m offline_mqtt.flow.cli run job.db job-1 --gates gates.json --gates-dir gdir &
# 等 gdir/gate_node.first_barrier.u.hit 出现后 kill -9，再用新进程恢复：
python -m offline_mqtt.flow.cli run job.db job-1
python -m offline_mqtt.flow.cli diag job.db job-1
```

## 7. 内存边界说明

- 边通道容量固定，`diag` 给出每条边的配置容量与实际最高水位；验收测试
  （`tests/test_flow_memory.py`）断言水位永不超过容量。
- 源按批（默认 256 行）读取，处理完一批再取下一批。
- sum 的不同 topic 状态随**键的数量**增长，这是算子语义状态，与固定容量的
  通道缓冲是两回事；sink 结果落 SQLite，不堆在进程内存。
