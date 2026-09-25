package com.github.h3yow.leasecoordinationjava.tests.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.h3yow.leasecoordinationjava.coordination.cluster.LocalCluster;
import com.github.h3yow.leasecoordinationjava.coordination.lease.CoordinationException;
import com.github.h3yow.leasecoordinationjava.coordination.lease.Lease;
import com.github.h3yow.leasecoordinationjava.coordination.lease.LeaseRejectReason;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 租约获取/续约/释放的生命周期与拒绝原因区分。
 * 关键时间点与判定依据见日志（wall/mono/expiresAtMono/剩余毫秒）。
 */
class LeaseLifecycleTest {

    private static final Logger log = LoggerFactory.getLogger(LeaseLifecycleTest.class);

    @Test
    void acquire_renew_release_and_reacquire() {
        try (LocalCluster cluster = new LocalCluster(2)) {
            var a = cluster.node(0);
            var b = cluster.node(1);

            // t=0：A 获取 /res/order-1，TTL=1000ms
            Lease l1 = a.leaseService().acquire("/res/order-1", a.nodeId(), 1000);
            log.info("[T1] A 首次获取 token={} expiresAtMono={}", l1.fencingToken(),
                    l1.expiresAtMono());
            assertThat(l1.fencingToken()).isEqualTo(1L);
            assertThat(l1.revision()).isEqualTo(1);

            // t=100：B 抢占被拒：HELD_BY_OTHER
            cluster.clock().advance(100);
            assertThatThrownBy(() -> b.leaseService().acquire("/res/order-1", b.nodeId(), 1000))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(LeaseRejectReason.HELD_BY_OTHER);

            // t=100：B 越权续约/释放被拒：NOT_HOLDER
            assertThatThrownBy(() -> b.leaseService().renew("/res/order-1", b.nodeId(), 1000))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(LeaseRejectReason.NOT_HOLDER);

            // t=100：A 重复获取是幂等的：不发新令牌
            Lease l1again = a.leaseService().acquire("/res/order-1", a.nodeId(), 1000);
            assertThat(l1again.fencingToken()).isEqualTo(1L);

            // t=200：A 续约成功，revision=2，token 不变，过期顺延
            cluster.clock().advance(100);
            Lease l2 = a.leaseService().renew("/res/order-1", a.nodeId(), 1000);
            log.info("[T2] A 续约成功 token={} revision={} expiresAtMono={}",
                    l2.fencingToken(), l2.revision(), l2.expiresAtMono());
            assertThat(l2.fencingToken()).isEqualTo(1L);
            assertThat(l2.revision()).isEqualTo(2);
            assertThat(l2.expiresAtMono()).isEqualTo(1200L);

            // t=300：A 显式释放
            cluster.clock().advance(100);
            a.leaseService().release("/res/order-1", a.nodeId());

            // 重复释放被拒：ALREADY_RELEASED
            assertThatThrownBy(() -> a.leaseService().release("/res/order-1", a.nodeId()))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(LeaseRejectReason.ALREADY_RELEASED);
            // 对已释放租约续约也被拒：ALREADY_RELEASED（与 EXPIRED 可区分）
            assertThatThrownBy(() -> a.leaseService().renew("/res/order-1", a.nodeId(), 1000))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(LeaseRejectReason.ALREADY_RELEASED);

            // 释放后 B 可立即重新获取，且拿到新栅栏令牌 2
            Lease l3 = b.leaseService().acquire("/res/order-1", b.nodeId(), 1000);
            log.info("[T3] 释放后 B 重新获取 token={} holder={}", l3.fencingToken(),
                    l3.holderId());
            assertThat(l3.fencingToken()).isEqualTo(2L);
            assertThat(l3.holderId()).isEqualTo(b.nodeId());
        }
    }

    @Test
    void expiry_blocks_renew_but_allows_reacquire() {
        try (LocalCluster cluster = new LocalCluster(2)) {
            var a = cluster.node(0);
            var b = cluster.node(1);

            Lease l1 = a.leaseService().acquire("/res/order-2", a.nodeId(), 500);
            assertThat(l1.fencingToken()).isEqualTo(1L);

            // t=500：恰好到期（expiresAtMono<=nowMono 视为过期）→ 续约被拒 EXPIRED
            cluster.clock().advance(500);
            assertThatThrownBy(() -> a.leaseService().renew("/res/order-2", a.nodeId(), 500))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(LeaseRejectReason.EXPIRED);

            // 过期后可被他人重新获取，令牌递增
            Lease l2 = b.leaseService().acquire("/res/order-2", b.nodeId(), 500);
            log.info("[T4] 过期后 B 获取 token={} nowMono={}", l2.fencingToken(),
                    cluster.clock().monotonicMillis());
            assertThat(l2.fencingToken()).isEqualTo(2L);
            assertThat(l2.holderId()).isEqualTo(b.nodeId());

            // A 再续约自己的旧租约：NOT_HOLDER（持有者已是 B）
            assertThatThrownBy(() -> a.leaseService().renew("/res/order-2", a.nodeId(), 500))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(LeaseRejectReason.NOT_HOLDER);
        }
    }

    @Test
    void unknown_lease_is_distinct_reason() {
        try (LocalCluster cluster = new LocalCluster(1)) {
            var a = cluster.node(0);
            assertThatThrownBy(() -> a.leaseService().renew("/res/never", a.nodeId(), 100))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(LeaseRejectReason.UNKNOWN_LEASE);
        }
    }
}
