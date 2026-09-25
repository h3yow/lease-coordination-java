package com.github.h3yow.leasecoordinationjava.tests.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.h3yow.leasecoordinationjava.coordination.cluster.LocalCluster;
import com.github.h3yow.leasecoordinationjava.coordination.cluster.LogicalNode;
import com.github.h3yow.leasecoordinationjava.coordination.clock.SimulatedClock;
import com.github.h3yow.leasecoordinationjava.coordination.lease.CoordinationException;
import com.github.h3yow.leasecoordinationjava.coordination.lease.Lease;
import com.github.h3yow.leasecoordinationjava.coordination.write.WriteOutcome;
import com.github.h3yow.leasecoordinationjava.coordination.write.WriteRejectReason;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 故障收敛：逻辑节点崩溃/重启，以及整个"进程"重启（重新打开数据目录、重放 WAL）。
 * 任何时刻同一资源只有一个有效持有者；幂等台账恢复后重放依旧不产生重复副作用。
 */
class NodeCrashRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(NodeCrashRecoveryTest.class);

    @Test
    void leader_crash_and_restart_keeps_single_holder_and_idempotent_table() {
        try (LocalCluster cluster = new LocalCluster(3)) {
            LogicalNode a = cluster.node(0);
            LogicalNode b = cluster.node(1);
            String res = "/res/leader-job";

            Lease leaseA = a.leaseService().acquire(res, a.nodeId(), 10_000);
            long tokenA = leaseA.fencingToken();
            a.writeService().submit(res, a.nodeId(), tokenA, "job-step-1", "checkpoint#1");
            log.info("[CRASH-前置] A 是领导 token={} 已写1条 nowMono={}", tokenA,
                    cluster.clock().monotonicMillis());

            // A 崩溃：B 在 A 租约未过期时仍无法获取（不出现两个有效持有者）
            a.crash();
            assertThatThrownBy(() -> b.leaseService().acquire(res, b.nodeId(), 10_000))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason().name())
                    .isEqualTo("HELD_BY_OTHER");
            log.info("[CRASH] A 崩溃后 B 抢锁仍被拒绝 → 同一时刻不存在两个有效持有者");

            // A 重启：视图恢复，可继续续约，且 incarnation 递增
            a.restart();
            assertThat(a.incarnation()).isEqualTo(2);
            Lease renewed = a.leaseService().renew(res, a.nodeId(), 10_000);
            assertThat(renewed.fencingToken()).isEqualTo(tokenA);
            log.info("[RECOVER] A 重启后续约成功 token={} revision={}", renewed.fencingToken(),
                    renewed.revision());

            // 幂等台账在共享存储上：重启后重放同一请求不产生重复副作用
            WriteOutcome replay = a.writeService().submit(res, a.nodeId(), tokenA,
                    "job-step-1", "checkpoint#1");
            assertThat(replay.replay()).isTrue();
            assertThat(replay.sequence()).isEqualTo(1L);

            // B 用伪造的新写入（自己没领导权）被 STALE_TOKEN 拒绝
            assertThatThrownBy(() -> b.writeService().submit(res, b.nodeId(), tokenA,
                    "job-step-b", "x"))
                    .isInstanceOf(CoordinationException.class);
        }
    }

    @Test
    void full_process_restart_replays_wal_and_preserves_fencing_decisions(@TempDir Path dir) {
        SimulatedClock clock = new SimulatedClock();
        String res = "/res/durable-kv";

        // ---- 第一个"进程"生命周期 ----
        try (LocalCluster first = new LocalCluster(dir, clock, 2)) {
            LogicalNode a = first.node(0);
            Lease lease = a.leaseService().acquire(res, a.nodeId(), 5_000);
            a.writeService().submit(res, a.nodeId(), lease.fencingToken(), "k-1", "v1");
            clock.advance(100);
            a.leaseService().renew(res, a.nodeId(), 5_000);
            log.info("[PROC-1] 授予token={} 写入1条 续约1次 关闭前mono={}",
                    lease.fencingToken(), clock.monotonicMillis());
        } // 所有节点崩溃 + 状态库关闭（模拟进程退出，内存全部丢失）

        // ---- 第二个"进程"：同目录、同时钟，构造时重放 WAL ----
        try (LocalCluster second = new LocalCluster(dir, clock, 2)) {
            LogicalNode a2 = second.node(0);
            LogicalNode b2 = second.node(1);

            Lease restored = a2.leaseService().inspect(res).orElseThrow();
            log.info("[PROC-2] WAL重放后 inspect: holder={} token={} revision={} released={} "
                            + "expiresAtMono={} nowMono={}",
                    restored.holderId(), restored.fencingToken(), restored.revision(),
                    restored.released(), restored.expiresAtMono(), clock.monotonicMillis());
            assertThat(restored.holderId()).isEqualTo(a2.nodeId());
            assertThat(restored.fencingToken()).isEqualTo(1L);
            assertThat(restored.revision()).isEqualTo(2); // 续约记录被重放

            // 旧请求重放：幂等表恢复，仍是同一条副作用，不产生 seq=2
            WriteOutcome replay = a2.writeService().submit(res, a2.nodeId(), 1L, "k-1", "v1");
            assertThat(replay.replay()).isTrue();
            assertThat(replay.sequence()).isEqualTo(1L);

            // 未过期：B 不能抢占
            assertThatThrownBy(() -> b2.leaseService().acquire(res, b2.nodeId(), 5_000))
                    .isInstanceOf(CoordinationException.class);

            // 推进到过期：新授予的令牌必须接续为 2（nextToken 也从 WAL 恢复）
            clock.advance(5_000);
            Lease newLease = b2.leaseService().acquire(res, b2.nodeId(), 5_000);
            log.info("[PROC-2] 过期后 B 获取 token={}（令牌从WAL恢复点继续递增，无重复授予）",
                    newLease.fencingToken());
            assertThat(newLease.fencingToken()).isEqualTo(2L);

            WriteOutcome w = b2.writeService().submit(res, b2.nodeId(), 2L, "k-2", "v2");
            assertThat(w.sequence()).isEqualTo(2L);

            // 旧令牌写入被拒（领导权已变更）
            assertThatThrownBy(() -> a2.writeService().submit(res, a2.nodeId(), 1L,
                    "k-old", "old"))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(WriteRejectReason.STALE_TOKEN);
        }
    }
}
