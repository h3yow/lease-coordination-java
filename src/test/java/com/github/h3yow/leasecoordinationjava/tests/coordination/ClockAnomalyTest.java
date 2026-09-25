package com.github.h3yow.leasecoordinationjava.tests.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.h3yow.leasecoordinationjava.coordination.cluster.LocalCluster;
import com.github.h3yow.leasecoordinationjava.coordination.cluster.LogicalNode;
import com.github.h3yow.leasecoordinationjava.coordination.lease.CoordinationException;
import com.github.h3yow.leasecoordinationjava.coordination.lease.Lease;
import com.github.h3yow.leasecoordinationjava.coordination.lease.LeaseRejectReason;
import com.github.h3yow.leasecoordinationjava.coordination.write.WriteOutcome;
import com.github.h3yow.leasecoordinationjava.coordination.write.WriteRejectReason;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 时间异常：墙上时钟大幅回拨、正负抖动均不得影响租约结论
 * （不重复授予、不错误拒绝）——所有判定只看单调时钟。
 */
class ClockAnomalyTest {

    private static final Logger log = LoggerFactory.getLogger(ClockAnomalyTest.class);

    @Test
    void wall_clock_rollback_and_jitter_do_not_change_lease_decisions() {
        try (LocalCluster cluster = new LocalCluster(2)) {
            LogicalNode a = cluster.node(0);
            LogicalNode b = cluster.node(1);
            String res = "/res/clock-sensitive";
            long wallBefore = cluster.clock().wallClockMillis();

            // 单调 t=0 授予，TTL=1000
            Lease lease = a.leaseService().acquire(res, a.nodeId(), 1000);
            assertThat(lease.fencingToken()).isEqualTo(1L);

            // 墙上时钟大幅回拨 1 小时，再正向跳 2 小时，反复抖动
            cluster.clock().shiftWallClock(-3_600_000L);
            cluster.clock().shiftWallClock(7_200_000L);
            cluster.clock().shiftWallClock(-3_600_000L + 7);
            log.info("[CLOCK] 墙上时钟回拨/抖动后 wall={} (初始锚点={})，单调时钟仍为 mono={}",
                    cluster.clock().wallClockMillis(), wallBefore,
                    cluster.clock().monotonicMillis());
            assertThat(cluster.clock().monotonicMillis()).isZero();

            // 单调时间未推进 → B 仍不能抢占（没有因墙上时钟抖动而错误地"过期重授"）
            assertThatThrownBy(() -> b.leaseService().acquire(res, b.nodeId(), 1000))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(LeaseRejectReason.HELD_BY_OTHER);

            // A 自己的续约/写入仍正常（没有被错误拒绝）
            Lease renewed = a.leaseService().renew(res, a.nodeId(), 1000);
            assertThat(renewed.revision()).isEqualTo(2);
            WriteOutcome w = a.writeService().submit(res, a.nodeId(), renewed.fencingToken(),
                    "c-1", "ok");
            assertThat(w.accepted()).isTrue();

            // 单调时间推进到租约真正过期：此时才允许 B 获取，且令牌正确递增（无重复授予 token=1）
            cluster.clock().advance(1001);
            Lease leaseB = b.leaseService().acquire(res, b.nodeId(), 1000);
            log.info("[CLOCK] 单调时间推进到 {} 后 B 才获得 token={}，判定不被墙上时钟影响",
                    cluster.clock().monotonicMillis(), leaseB.fencingToken());
            assertThat(leaseB.fencingToken()).isEqualTo(2L);

            // 抖动期间 A 持 token=1 的延迟写入：STALE_TOKEN，而不会因 wall 时间混乱被接受
            assertThatThrownBy(() -> a.writeService().submit(res, a.nodeId(), 1L,
                    "c-late", "late"))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(WriteRejectReason.STALE_TOKEN);
        }
    }

    @Test
    void monotonic_time_never_goes_backwards() {
        try (LocalCluster cluster = new LocalCluster(1)) {
            long t0 = cluster.clock().monotonicMillis();
            cluster.clock().shiftWallClock(-10_000L);
            cluster.clock().advance(50);
            assertThat(cluster.clock().monotonicMillis()).isEqualTo(t0 + 50);
            cluster.clock().shiftWallClock(10_000L);
            assertThat(cluster.clock().monotonicMillis()).isEqualTo(t0 + 50);
        }
    }
}
