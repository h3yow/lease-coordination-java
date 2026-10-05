package com.github.h3yow.leasecoordinationjava.tests.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.h3yow.leasecoordinationjava.coordination.cluster.LocalCluster;
import com.github.h3yow.leasecoordinationjava.coordination.cluster.LogicalNode;
import com.github.h3yow.leasecoordinationjava.coordination.lease.CoordinationException;
import com.github.h3yow.leasecoordinationjava.coordination.lease.HandoverRejectReason;
import com.github.h3yow.leasecoordinationjava.coordination.lease.Lease;
import com.github.h3yow.leasecoordinationjava.coordination.write.WriteOutcome;
import com.github.h3yow.leasecoordinationjava.coordination.write.WriteRejectReason;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 计划内换主：当前持有者主动把领导权交接给指定继任者。
 * 覆盖：交接成功立即生效、旧令牌写入边界（含曾落库请求）、真重放仍幂等、
 * 序号连续可查、各类拒绝原因、并发交错下任一时刻只有一个有效持有者。
 */
class LeadershipHandoverTest {

    private static final Logger log = LoggerFactory.getLogger(LeadershipHandoverTest.class);

    @Test
    void handover_grants_successor_immediately_and_old_token_is_hard_rejected() {
        try (LocalCluster cluster = new LocalCluster(3)) {
            LogicalNode a = cluster.node(0);
            LogicalNode b = cluster.node(1);
            String res = "/res/rolling-upgrade";

            // A 是现任领导，已提交 2 条
            Lease leaseA = a.leaseService().acquire(res, a.nodeId(), 60_000);
            long tokenA = leaseA.fencingToken();
            a.writeService().submit(res, a.nodeId(), tokenA, "step-1", "data-1");
            a.writeService().submit(res, a.nodeId(), tokenA, "step-2", "data-2");
            log.info("[HANDOVER-前置] A token={} 已提交2条 lastSeq=2 租约远未到期", tokenA);

            // A 主动交接给 B：B 立即成为有效持有者，令牌严格更大，不等旧租约到期
            Lease leaseB = a.leaseService().handover(res, a.nodeId(), b.nodeId(), 60_000);
            log.info("[HANDOVER] 交接生效 B token={} 旧租约剩余远未到期 nowMono={}",
                    leaseB.fencingToken(), cluster.clock().monotonicMillis());
            assertThat(leaseB.holderId()).isEqualTo(b.nodeId());
            assertThat(leaseB.fencingToken()).isGreaterThan(tokenA);
            assertThat(leaseB.expiresAtMono()).isGreaterThan(cluster.clock().monotonicMillis());

            // 继任者能查到已提交进度，从已知序号接着写
            long committed = b.writeService().committedSequence(res);
            log.info("[HANDOVER-接续] B 查到 lastSequence={}，从 {} 继续",
                    committed, committed + 1);
            assertThat(committed).isEqualTo(2L);
            WriteOutcome w3 = b.writeService().submit(res, b.nodeId(),
                    leaseB.fencingToken(), "step-3", "data-3");
            assertThat(w3.sequence()).isEqualTo(3L); // 顺排不跳号

            // 旧令牌的新提交：明确拒绝 HANDOVER_SUPERSEDED
            assertThatThrownBy(() -> a.writeService().submit(res, a.nodeId(), tokenA,
                    "step-old", "stale"))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(WriteRejectReason.HANDOVER_SUPERSEDED);

            // 旧令牌重发"之前确实成功落过库"的请求：同样拒绝，绝不按重复提交放行
            assertThatThrownBy(() -> a.writeService().submit(res, a.nodeId(), tokenA,
                    "step-1", "data-1"))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(WriteRejectReason.HANDOVER_SUPERSEDED);
            log.info("[HANDOVER-边界] 旧令牌新写与旧令牌重放均被 HANDOVER_SUPERSEDED 拒绝");

            // 真正的重放（当前持有者用有效令牌重发同一请求）：返回首次结果，无第二次副作用
            WriteOutcome replay = b.writeService().submit(res, b.nodeId(),
                    leaseB.fencingToken(), "step-3", "data-3");
            assertThat(replay.replay()).isTrue();
            assertThat(replay.sequence()).isEqualTo(3L);
            assertThat(b.writeService().committedSequence(res)).isEqualTo(3L); // 序号未涨

            // 旧主 A 不能再获取（B 有效持有），交接后互斥不变量仍成立
            assertThatThrownBy(() -> a.leaseService().acquire(res, a.nodeId(), 1000))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason().name())
                    .isEqualTo("HELD_BY_OTHER");
        }
    }

    @Test
    void handover_rejections_carry_distinguishable_reasons() {
        try (LocalCluster cluster = new LocalCluster(3)) {
            LogicalNode a = cluster.node(0);
            LogicalNode b = cluster.node(1);
            LogicalNode c = cluster.node(2);
            String res = "/res/handover-rejects";

            // 无租约：UNKNOWN_LEASE
            assertThatThrownBy(() -> a.leaseService().handover(res, a.nodeId(), b.nodeId(), 1000))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(HandoverRejectReason.UNKNOWN_LEASE);

            Lease leaseA = a.leaseService().acquire(res, a.nodeId(), 1000);

            // 越权发起：NOT_HOLDER
            assertThatThrownBy(() -> b.leaseService().handover(res, b.nodeId(), c.nodeId(), 1000))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(HandoverRejectReason.NOT_HOLDER);

            // 继任者非法（自己/空）：INVALID_SUCCESSOR
            assertThatThrownBy(() -> a.leaseService().handover(res, a.nodeId(), a.nodeId(), 1000))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(HandoverRejectReason.INVALID_SUCCESSOR);
            assertThatThrownBy(() -> a.leaseService().handover(res, a.nodeId(), "  ", 1000))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(HandoverRejectReason.INVALID_SUCCESSOR);

            // 过期后不能交接：EXPIRED（只能等自然失效后重新获取）
            cluster.clock().advance(1000);
            assertThatThrownBy(() -> a.leaseService().handover(res, a.nodeId(), b.nodeId(), 1000))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(HandoverRejectReason.EXPIRED);

            // 过期租约被 B 重新获取后释放，再交接：ALREADY_RELEASED
            Lease leaseB = b.leaseService().acquire(res, b.nodeId(), 60_000);
            b.leaseService().release(res, b.nodeId());
            assertThatThrownBy(() -> b.leaseService().handover(res, b.nodeId(), c.nodeId(), 1000))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(HandoverRejectReason.ALREADY_RELEASED);
            log.info("[HANDOVER-拒绝] UNKNOWN_LEASE/NOT_HOLDER/INVALID_SUCCESSOR/EXPIRED/"
                    + "ALREADY_RELEASED 全部可区分，原租约 tokenA={} tokenB={}",
                    leaseA.fencingToken(), leaseB.fencingToken());
        }
    }

    @Test
    void concurrent_handover_and_acquire_never_yield_two_valid_holders() throws Exception {
        try (LocalCluster cluster = new LocalCluster(4)) {
            LogicalNode a = cluster.node(0);
            LogicalNode b = cluster.node(1);
            LogicalNode c = cluster.node(2);
            String res = "/res/handover-race";

            Lease leaseA = a.leaseService().acquire(res, a.nodeId(), 60_000);
            log.info("[RACE-前置] A token={} 有效持有，A 发起交接给 B 的同时 C 抢锁",
                    leaseA.fencingToken());

            int threads = 8;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<String>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                final boolean handover = i % 2 == 0;
                futures.add(pool.submit(() -> {
                    start.await();
                    try {
                        if (handover) {
                            Lease l = a.leaseService().handover(res, a.nodeId(),
                                    b.nodeId(), 60_000);
                            return "HANDOVER-OK token=" + l.fencingToken();
                        }
                        Lease l = c.leaseService().acquire(res, c.nodeId(), 60_000);
                        return "ACQUIRE-OK token=" + l.fencingToken();
                    } catch (CoordinationException e) {
                        return "REJECTED:" + e.reason();
                    }
                }));
            }
            start.countDown();
            List<String> results = new ArrayList<>();
            for (Future<String> f : futures) {
                results.add(f.get(10, TimeUnit.SECONDS));
            }
            pool.shutdown();

            long handoverOk = results.stream().filter(r -> r.startsWith("HANDOVER-OK")).count();
            long acquireOk = results.stream().filter(r -> r.startsWith("ACQUIRE-OK")).count();
            log.info("[RACE-结果] 交接成功={} 获取成功={} 明细={}", handoverOk, acquireOk, results);

            // 交接幂等性：A 重复发起交接，第二次起 A 已非持有者 → NOT_HOLDER，恰好 1 次成功
            assertThat(handoverOk).isEqualTo(1);
            // C 在 B 有效租约期间抢锁：全部 HELD_BY_OTHER，绝不成功
            assertThat(acquireOk).isZero();

            // 终态：B 是唯一有效持有者
            Lease current = b.leaseService().inspect(res).orElseThrow();
            assertThat(current.holderId()).isEqualTo(b.nodeId());
            assertThat(current.fencingToken()).isEqualTo(leaseA.fencingToken() + 1);
            log.info("[RACE-终态] 唯一有效持有者={} token={} → 并发交错下无双主",
                    current.holderId(), current.fencingToken());
        }
    }
}
