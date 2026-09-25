package com.github.h3yow.leasecoordinationjava.tests.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.h3yow.leasecoordinationjava.coordination.cluster.LocalCluster;
import com.github.h3yow.leasecoordinationjava.coordination.lease.CoordinationException;
import com.github.h3yow.leasecoordinationjava.coordination.lease.Lease;
import com.github.h3yow.leasecoordinationjava.coordination.write.WriteOutcome;
import com.github.h3yow.leasecoordinationjava.coordination.write.WriteRejectReason;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 领导权变更下的有序写入：栅栏令牌递增、旧领导/过期令牌被拒、重放幂等。
 */
class FencedWriteTest {

    private static final Logger log = LoggerFactory.getLogger(FencedWriteTest.class);

    @Test
    void stale_leader_and_expired_token_are_rejected_not_silently_accepted() {
        try (LocalCluster cluster = new LocalCluster(2)) {
            var a = cluster.node(0);
            var b = cluster.node(1);
            String res = "/res/account-7";

            // --- A 成为第一任领导，token=1 ---
            Lease leaseA = a.leaseService().acquire(res, a.nodeId(), 1000);
            long tokenA = leaseA.fencingToken();
            WriteOutcome w1 = a.writeService().submit(res, a.nodeId(), tokenA, "req-1", "v1");
            assertThat(w1.accepted()).isTrue();
            assertThat(w1.replay()).isFalse();
            assertThat(w1.sequence()).isEqualTo(1L);

            // 未知令牌直接拒绝
            assertThatThrownBy(() -> a.writeService().submit(res, a.nodeId(), 999L, "req-x", "?"))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(WriteRejectReason.UNKNOWN_TOKEN);

            // --- t=1000 A 失联过期；B 获取新领导权，token=2 ---
            cluster.clock().advance(1000); // A 租约到期
            Lease leaseB = b.leaseService().acquire(res, b.nodeId(), 1000);
            long tokenB = leaseB.fencingToken();
            log.info("[LEADERSHIP] 领导权变更 A(token={}已过期) → B(token={}) nowMono={}",
                    tokenA, tokenB, cluster.clock().monotonicMillis());
            assertThat(tokenB).isGreaterThan(tokenA);

            // 旧领导 A 用过期 token=1 提交：STALE_TOKEN（不是静默接受）
            assertThatThrownBy(() -> a.writeService().submit(res, a.nodeId(), tokenA,
                    "req-stale", "old"))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(WriteRejectReason.STALE_TOKEN);

            // B 正常写入 seq=2
            WriteOutcome w2 = b.writeService().submit(res, b.nodeId(), tokenB, "req-2", "v2");
            assertThat(w2.sequence()).isEqualTo(2L);

            // --- B 的租约过期（未换领导），B 再用旧 token 提交：LEASE_EXPIRED ---
            cluster.clock().advance(1001);
            assertThatThrownBy(() -> b.writeService().submit(res, b.nodeId(), tokenB,
                    "req-exp", "late"))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(WriteRejectReason.LEASE_EXPIRED);
        }
    }

    @Test
    void replay_same_request_has_no_duplicate_side_effect_even_after_leadership_change() {
        try (LocalCluster cluster = new LocalCluster(2)) {
            var a = cluster.node(0);
            var b = cluster.node(1);
            String res = "/res/account-8";

            Lease leaseA = a.leaseService().acquire(res, a.nodeId(), 500);
            WriteOutcome first = a.writeService().submit(res, a.nodeId(),
                    leaseA.fencingToken(), "order-1001", "debit-10");
            assertThat(first.sequence()).isEqualTo(1L);

            // 领导权切到 B
            cluster.clock().advance(600);
            long tokenB = b.leaseService().acquire(res, b.nodeId(), 500).fencingToken();
            assertThat(tokenB).isEqualTo(2L);
            b.writeService().submit(res, b.nodeId(), tokenB, "order-1002", "credit-10");
            assertThat(b.leaseService().inspect(res).orElseThrow().fencingToken())
                    .isEqualTo(2L);

            // A 的旧请求被重放（同幂等键，即便用旧 token）：返回首次结果，无新副作用
            WriteOutcome replay = a.writeService().submit(res, a.nodeId(),
                    leaseA.fencingToken(), "order-1001", "debit-10");
            log.info("[REPLAY] 重放 order-1001 accepted={} replay={} seq={}",
                    replay.accepted(), replay.replay(), replay.sequence());
            assertThat(replay.replay()).isTrue();
            assertThat(replay.sequence()).isEqualTo(1L);

            // 新写入继续占用后续序号，证明重放没有插入副作用
            WriteOutcome w4 = b.writeService().submit(res, b.nodeId(), tokenB,
                    "order-1003", "debit-5");
            assertThat(w4.sequence()).isEqualTo(3L);
        }
    }
}
