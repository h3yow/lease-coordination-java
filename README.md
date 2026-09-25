# 本地集群协调能力：租约 + Fencing 令牌 + 事件溯源恢复

在**单进程内用多个逻辑节点模拟集群成员**，模拟一个共享的一致性元数据存储（`CoordinationCluster`），
提供：

1. 租约的获取 / 续约 / 释放（互斥、TTL 过期、可区分的拒绝原因）；
2. 领导权变更下的**有序写入**（单调递增 fencing 令牌 + 栅栏校验 + 请求幂等去重）；
3. 节点崩溃 / 进程重启后从持久事件日志恢复，时钟回拨与瞬时抖动下判定依然单调。

## 一、整体结构

```
coordination/
├── clock/
│   ├── LogicalClock          统一逻辑时钟接口（所有判定只依赖它）
│   └── GuardedSystemClock    对系统时钟回拨/抖动做单调化保护
├── lease/
│   ├── LeaseGrant            租约凭证（resource/holder/token/expire/version）
│   ├── RejectReason          可区分的拒绝原因枚举
│   └── CoordinationException 携带 RejectReason 的拒绝异常
├── event/
│   └── CoordinationEvent     持久化事件（获取/续约/释放/写入，sealed + JSON 多态）
├── store/
│   └── DurableEventStore     JSONL 追加日志，append 后 fsync，replay 截断残缺尾行
├── write/
│   └── WriteResult           写入结果（replay=true 表示重放命中、无新副作用）
├── CoordinationCluster       协调核心：锁内“判定→事件→fsync→内存更新”
└── ClusterNode               逻辑节点（不持有权威状态，crash 后用同 id 重建即重启）
```

## 二、租约模型

- 一次授予生成不可变凭证 `LeaseGrant(resourceId, holderId, fencingToken, acquiredAt, expireAt, version)`。
- 有效性采用**半开区间**：`validAt(now) = now < expireAt`，`now == expireAt` 即过期，边界无歧义。
- **获取** `acquireLease(nodeId, resourceId, ttlMs)`
  - 资源上存在**他人**未过期、未释放的租约 → 拒绝 `LEASE_HELD_BY_OTHER`（不可抢占）；
  - 自己已是持有者 → 等价续约，令牌不变、过期时间顺延；
  - 当前租约已过期或已释放 → 任何人可重新获取，分配**新的、严格更大的**令牌与 `version`。
- **续约** `renewLease(nodeId, grant, ttlMs)`，拒绝原因可区分：

  | 情形 | RejectReason |
  |---|---|
  | 凭证不是资源当前租约（伪造/旧凭证） | `LEASE_NOT_OWNER` |
  | 请求节点不是持有者（越权） | `LEASE_NOT_OWNER` |
  | 租约已主动释放 | `LEASE_ALREADY_RELEASED` |
  | 租约已过期 | `LEASE_EXPIRED` |

- **释放** `releaseLease(nodeId, grant)`：成功后他人无需等待过期即可获取；
  对同一凭证再次释放 → `LEASE_ALREADY_RELEASED`；越权释放 → `LEASE_NOT_OWNER`。

## 三、Fencing 令牌模型（领导权与写入顺序）

- 每次**新授予**分配一个集群内全局单调递增的整数 `fencingToken`（续约不换令牌）。
  令牌序号本身也由事件日志恢复，重启后继续递增，**不会重新从小号发牌**（避免重复授予）。
- 写入 `submitWrite(nodeId, resourceId, requestId, token, payloadRef)` 按固定顺序做 5 道校验：

  1. `FENCING_TOKEN_UNKNOWN`：令牌 ≤ 0 或超过已授予的最大令牌（伪造）；
  2. `FENCING_TOKEN_STALE`：令牌不是资源的当前租约令牌（**旧领导**）；
  3. `LEASE_NOT_OWNER`：请求节点不是该令牌持有者（越权冒用）；
  4. `FENCING_LEASE_EXPIRED`：当前领导租约已过期或已释放（**过期令牌**）；
  5. 幂等：在领导权上下文仍成立的前提下，`(resourceId, requestId)` 命中历史提交，
     返回首次结果的副本（`replay=true`，`committedAtMs` 不变），**不追加事件、无重复副作用**；
     旧领导/过期领导拿旧 requestId 重放会先在第 2/4 步被拒，**不能借重放被静默接受**；
  6. 栅栏：新令牌不得低于资源上已提交写入的最高令牌，然后才落盘提交。

  下游存储只要也遵守“拒绝更小令牌的写入”，即可得到严格有序的写入（fencing token 模式）。

## 四、异常与恢复规则

### 4.1 持久化与原子性

- 所有被接受的状态变更都先写一条事件到 `coordination.log`（每行一个 JSON），
  `FileChannel.force(true)`（fsync）成功后才更新内存并返回。
- 集群用单把锁把“判定 → 生成事件 → 落盘 → 更新内存”串行化，
  因此并发争抢下同一资源**至多一人**成功，其余全部得到 `LEASE_HELD_BY_OTHER`。

### 4.2 节点崩溃 / 进程重启

- `ClusterNode` 不持有权威状态：`crash()` 只丢弃节点侧瞬时引用。
  用相同 `nodeId` 重新构造节点即为“重启”；租约凭证若仍在 TTL 内即可继续续约/写入，
  凭证丢失则必须等待过期后重新竞争——与真实分布式故障模型一致。
- 协调进程重启时，`new CoordinationCluster(...)` 重放全部事件重建：
  当前持有者、过期时间、令牌计数器、已提交最高令牌、requestId 去重表全部恢复。

### 4.3 崩溃瞬间的残缺日志

- 掉电只可能让最后一行写一半。`replay()` 逐行解析，遇到损坏行即
  **把文件截断回最后一个完整事件之后**并忽略残缺行，保证日志是“已提交事件”的严格前缀，
  不会重放出幽灵授予。

### 4.4 时钟回拨与瞬时抖动

- `GuardedSystemClock` 内部记录“观测到的最大读数”，返回 `max(历史最大, 原始时钟)`：
  - 回拨期间判定时间冻结在最高点 → 租约不会因回拨“复活”、不会提前过期、不会重复授予；
  - 正常的续约/写入在回拨期间也不会被错误拒绝（时间不回退，TTL 语义稳定）；
  - 原始时钟恢复并超过历史最大值后重新跟随真实时间。
- 进程内所有逻辑节点共用同一时钟实例，互斥判定具有统一时间基准。

## 五、验证方法

```bash
mvn test
```

测试日志通过 `logback-test.xml` 输出到控制台，关键时间点（now/expire/committedAt）、
判定依据与拒绝原因均以 `[时间点]` / `[判定]` 前缀打印。

| 测试类 | 覆盖内容 |
|---|---|
| `LeaseLifecycleTest` | 获取/续约/释放全流程；过期边界重获；6 类拒绝原因逐一区分；多资源并存 |
| `ConcurrentContentionTest` | 24 节点同时争抢仅 1 人获胜；50 并发续约仅持有者成功；过期后二次争抢仍唯一 |
| `FencedWriteTest` | 令牌栅栏、旧领导/伪造/过期令牌拒绝、越权冒用、重放 3 次零重复副作用 |
| `RecoveryTest` | 进程崩溃重启后状态/TTL/令牌序号/幂等表恢复；残缺日志截断；幽灵授予不出现 |
| `ClockAnomalyTest` | 时钟回拨 100s 与多次抖动：不回退、不重复授予、不错误拒绝、恢复后才允许过期 |

查看某次运行落盘的事件日志：

```
target/surefire 的 @TempDir/<clusterName>/coordination.log
```
