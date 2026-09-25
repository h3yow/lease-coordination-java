# 本地集群协调：租约、栅栏令牌与故障收敛（lease-coordination-java）

在**单个进程内**用多个逻辑节点模拟一个分布式集群，提供三类彼此配合的协调能力：

1. **租约（Lease）**：同一资源同一时刻至多一个有效持有者；未过期不可抢占，过期后可被重新获取；
2. **栅栏令牌（Fencing Token）+ 有序写入**：领导权每变更一次令牌递增，旧领导/过期令牌的写入被**显式拒绝**，同一请求重放不产生重复副作用；
3. **故障与时间异常收敛**：节点崩溃/进程重启后从 WAL 重放恢复；判定只依赖单调时钟，墙上时钟回拨/抖动不会造成重复授予或错误拒绝。

工程为 Spring Boot 3.4 / Java 21，核心协调逻辑**不依赖 Spring 容器**，可直接用 JUnit 运行。

---

## 1. 架构与组件

```
LocalCluster（进程内集群：1 个可控时钟 + 1 个共享状态库 + N 个逻辑节点）
├── SimulatedClock        墙上时钟（可回拨/抖动） + 单调时钟（只增不减）
├── ClusterStateStore     共享持久化状态库：每资源线性化 + WAL 追加(fsync) + 重放恢复
│   └── ResourceState     单资源状态：租约槽 / 已签发令牌集合 / 幂等写入台账 / 副作用序号
├── LogicalNode           逻辑成员：crash() 丢内存态，restart() 重新挂接存储
├── LeaseService          acquire / renew / release / inspect
└── WriteService          submit（栅栏令牌校验 + 幂等重放 + 顺序序号）
```

存储模拟的是 etcd / ZooKeeper 一类的**共识存储**：所有节点共享一份权威状态，
节点本身不保存权威数据。每个资源一把锁，对该资源的所有变更在该锁内线性化执行，
变更以 WAL 记录追加到 `wal/<urlencoded(resourceId)>.log`，追加后 `fsync`，返回成功即视为已持久化。

### 双时钟模型

| 时钟 | 接口 | 是否可回退 | 用途 |
|---|---|---|---|
| 墙上时钟 | `wallClockMillis()` | **可以**（NTP 回拨、人为调整、瞬时正负抖动） | 仅用于日志中的人类可读时间戳 |
| 单调时钟 | `monotonicMillis()` | **不可以**（`advance` 仅接受非负增量） | 租约过期、写入有效性判定的**唯一**时间依据 |

`SimulatedClock.shiftWallClock(delta)` 只拨动墙上时钟，单调时钟纹丝不动，
因此时间异常在结构上无法影响协调结论（见 `ClockAnomalyTest`）。

---

## 2. 租约模型

### 2.1 数据结构

`Lease(resourceId, holderId, fencingToken, acquiredAtWall, acquiredAtMono, expiresAtMono, released, revision)`

* `fencingToken`：**栅栏令牌**。资源内单调递增（从 1 开始），仅在**全新授予**（无租约/过期后/释放后获取）时签发新值；续约保持不变。
* `revision`：修订版本，初次授予为 1，每次续约/释放 +1。
* `expiresAtMono`：以单调时钟计量的过期时刻；`expiresAtMono <= nowMono` 即视为过期。
* `released`：是否被显式释放。

### 2.2 操作语义

| 操作 | 成功条件与效果 | 拒绝（`CoordinationException`，原因码可区分） |
|---|---|---|
| `acquire(holder, ttl)` | 无有效租约 → 全新授予，签发**新令牌**，revision=1 | `HELD_BY_OTHER`：他人租约未过期，**不可抢占** |
| | 当前持有者重复获取 → **幂等**返回既有租约（不发新令牌） | |
| `renew(holder, ttl)` | 持有者本人、未释放、未过期 → 令牌不变，过期时刻顺延，revision+1 | `UNKNOWN_LEASE`：从未授予 |
| | | `NOT_HOLDER`：越权（持有者是别人） |
| | | `ALREADY_RELEASED`：已被释放 |
| | | `EXPIRED`：已过期（含恰好到期），过期租约**不能靠续约复活**，必须重新获取 |
| `release(holder)` | 持有者本人、未释放、未过期 → 置 `released`，他人立即可重新获取 | 原因码同上；对已释放租约**重复释放** → `ALREADY_RELEASED` |
| `inspect()` | 返回当前租约快照（不做状态变更） | — |

**互斥不变量**：在任意单调时刻 t，对同一资源，至多一个 `(holder, token)` 满足
`!released && expiresAtMono > t`。该判定在资源临界区内完成，天然覆盖并发与崩溃场景。

---

## 3. 栅栏令牌与有序写入模型

解决的经典问题：旧领导 A 认为自己还在任，向存储发起延迟写入；此时领导权已转给 B。
仅靠租约 TTL 无法阻止 A 的请求在网络中"复活"，因此引入单调递增的**栅栏令牌**。

### 3.1 提交流程（全部在资源线性化临界区内，按序判定）

`submit(resourceId, nodeId, token, idempotencyKey, payload)` → `WriteOutcome(accepted, replay, appliedToken, sequence, idempotencyKey, message)`

1. **幂等重放优先**：幂等键已在台账中 → 原样返回首次结果（`replay=true`、原 `sequence`），
   即使提交者已不是领导、即使这是崩溃恢复后的重放。**绝不产生第二次副作用**。
2. **`UNKNOWN_TOKEN`**：令牌从未被本资源签发过（伪造/串资源）。
3. **`STALE_TOKEN`**：令牌签发过，但租约槽当前令牌更大 —— 已发生领导权变更，旧领导被拒。
4. **`LEASE_EXPIRED`**：令牌仍在槽位上，但提交者不是持有者、租约已释放或 `expiresAtMono <= nowMono`。
5. **接受**：分配资源内单调递增的副作用序号 `sequence`（1,2,3,…），与令牌、幂等键、载荷一起写 WAL；
   下游存储可用 `(token, sequence)` 丢弃任何乱序/重复请求。

> 拒绝都以异常显式抛出（`CoordinationException.reason()`），不存在"静默接受"。

### 3.2 领导权变更示例

```
A acquire → token=1，write(req-1) → seq=1
时间推进，A 过期；B acquire → token=2（>1）
A 用 token=1 迟到提交 → STALE_TOKEN
B write(req-2) → seq=2
req-1 重放（同幂等键）→ replay=true, seq=1，无新副作用
```

---

## 4. 异常、崩溃与恢复规则

### 4.1 拒绝原因总览

| 域 | 原因码 | 含义 |
|---|---|---|
| 租约 | `HELD_BY_OTHER` | 他人持有未过期租约，抢占被拒 |
| 租约 | `NOT_HOLDER` | 越权续约/释放 |
| 租约 | `EXPIRED` | 租约已过期：续约/释放失效租约 |
| 租约 | `ALREADY_RELEASED` | 已释放后重复释放 / 续约 |
| 租约 | `UNKNOWN_LEASE` | 资源上无任何租约 |
| 写入 | `UNKNOWN_TOKEN` | 令牌从未签发 |
| 写入 | `STALE_TOKEN` | 旧领导令牌（已有更大令牌） |
| 写入 | `LEASE_EXPIRED` | 非持有者 / 已释放 / 已过期 |

### 4.2 节点崩溃与重启

* `LogicalNode.crash()`：丢弃节点全部内存句柄；**已 fsync 的 WAL 不丢**，其他节点不受影响。
  崩溃领导者未过期的租约仍然有效 —— 其他节点此时获取只会得到 `HELD_BY_OTHER`，
  **不会出现两个节点同时自认为持有者**。
* `LogicalNode.restart()`：重新挂接共享状态库，视图立即恢复，`incarnation` 递增；
  租约剩余时间、幂等台账都在，迟到请求重放仍是幂等的。

### 4.3 进程级重启（WAL 重放）

用**同一数据目录**重新构造 `ClusterStateStore` 即模拟整个进程重启：

1. 扫描 `wal/` 目录逐资源重放；
2. `G`(授予) / `N`(续约) / `R`(释放) 重建租约槽（持有者、令牌、过期时刻、revision）；
3. `W`(写入) 重建幂等台账与 `lastSequence`；
4. `issuedTokens` 与 `nextToken` 一并恢复 —— 重启后新授予的令牌**接续递增，不会重复签发**。

原子性：WAL 追加在资源临界区内、操作返回前完成（行式记录 + fsync）。
崩溃只会发生在"记录落盘前"（调用方视为失败，可安全重试）或"落盘后"（重放恢复），
不存在"调用成功但状态丢失"或"半截记录"。

### 4.4 时间异常

* 墙上时钟回拨/抖动：单调时钟不变，过期判定不变 → 不会提前重授（重复授予），也不会误判有效租约过期（错误拒绝）。
* 单调时钟只进不退，测试中通过 `SimulatedClock.advance` 精确推进到"恰好过期""刚过期""长期有效"等边界。

---

## 5. WAL 记录格式（`wal/<resource>.log`，每行一条，文本）

| 类型 | 格式 | 含义 |
|---|---|---|
| `G` | `G\|token\|wall\|mono\|expiresMono\|revision\|holderB64` | 全新授予 |
| `N` | `N\|token\|expiresMono\|revision` | 续约 |
| `R` | `R\|token\|revision` | 释放 |
| `W` | `W\|token\|sequence\|keyB64\|payloadB64` | 写入生效 |

字符串字段以 Base64 编码，保证不含分隔符；文件名对 resourceId 做 URL 编码。

---

## 6. 验证方法

### 6.1 运行全部测试

```bash
mvn test
```

12 个用例（外加 Spring 上下文冒烟测试），覆盖：

| 测试类 | 覆盖点 |
|---|---|
| `LeaseLifecycleTest` | 获取/幂等重复获取/续约/释放/重复释放；未过期不可抢占；过期后续约被拒(`EXPIRED`)、可被重新获取；`NOT_HOLDER`/`UNKNOWN_LEASE` 等原因区分 |
| `FencedWriteTest` | 领导权变更后旧令牌 `STALE_TOKEN`；未换领导但租约过期 `LEASE_EXPIRED`；未知令牌 `UNKNOWN_TOKEN`；同键重放无重复副作用、序号不跳 |
| `ConcurrentContentionTest` | 16 线程同起跑线竞争 → **恰好 1 个成功**、其余全部 `HELD_BY_OTHER`；12 线程同键并发提交 → 恰好 1 次副作用、其余为重放 |
| `NodeCrashRecoveryTest` | 领导者崩溃期间无双重持有者；重启后续约/幂等重放正常；进程级重启重放 WAL，令牌接续递增、旧令牌仍被拒 |
| `ClockAnomalyTest` | 墙上时钟回拨 1 小时 + 抖动不改变任何判定；单调时钟严格只增 |

### 6.2 查看关键时间点与判定依据的日志

日志直接打印 wall/mono 时间、过期时刻、剩余毫秒与拒绝原因，例如：

```bash
mvn -Dtest=LeaseLifecycleTest test
# 或查看落盘报告
cat target/surefire-reports/com.github.h3yow.leasecoordinationjava.tests.coordination.*-output.txt
```

典型日志：

```
[ACQUIRE-拒绝] resource=/res/order-1 node=node-2 持有者=node-1 token=1 未过期 (expiresAtMono=1000 > nowMono=100, 剩余=900ms) → HELD_BY_OTHER
[RENEW-成功] resource=/res/order-1 node=node-1 token=1 revision=1→2 nowMono=200 ttl=1000ms 新expiresAtMono=1200 → WAL落盘
[WRITE-拒绝] resource=/res/account-7 node=node-1 key=req-stale token=1 当前token=2 nowMono=1000 → STALE_TOKEN（旧领导，已发生领导权变更）
[WRITE-重放] resource=/res/account-8 node=node-1 key=order-1001 token=1 命中首次结果 seq=1 首次token=1 nowMono=600 → replay=true，无新副作用
WAL 重放完成 resource=/res/durable-kv file=%2Fres%2Fdurable-kv.log 记录数=3 lastToken=1 lastSeq=1
[CLOCK] 墙上时钟回拨/抖动后 wall=... ，单调时钟仍为 mono=0
```

### 6.3 直接检查 WAL

测试结束前数据目录为 JDK 临时目录 `lease-cluster-*`；
`NodeCrashRecoveryTest.full_process_restart_replays_wal...` 使用 JUnit `@TempDir`，
可在测试中打印路径后直接 `cat <dir>/wal/*.log` 查看原始记录。

---

## 7. 包结构

```
coordination/
├── clock/    TimeSource, SimulatedClock
├── storage/  ResourceState, ClusterStateStore（含 WAL 记录 sealed 模型）
├── lease/    Lease, LeaseRejectReason, CoordinationException, LeaseService
├── write/    WriteRejectReason, WriteOutcome, WriteService
└── cluster/  LogicalNode, LocalCluster
```
