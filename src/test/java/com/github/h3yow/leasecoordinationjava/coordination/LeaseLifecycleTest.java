package com.github.h3yow.leasecoordinationjava.coordination;

import com.github.h3yow.leasecoordinationjava.coordination.clock.GuardedSystemClock;
import com.github.h3yow.leasecoordinationjava.coordination.lease.CoordinationException;
import com.github.h3yow.leasecoordinationjava.coordination.lease.LeaseGrant;
import com.github.h3yow.leasecoordinationjava.coordination.lease.RejectReason;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 租约生命周期测试：获取/续约/释放、过期重获、以及每一种拒绝原因必须可区分。
 */
class LeaseLifecycleTest {

    private final long t0 = 1_000_000L;

    @TempDir
    Path tempDir;

    private void banner(String s) {
        System.out.println("==== " + s + " ====");
    }

    private CoordinationException expectReject(RejectReason expected, Runnable action) {
        CoordinationException ex = assertThrows(CoordinationException.class, action::run);
        System.out.printf("  [判定] 预期拒绝=%s 实际拒绝=%s :: %s%n",
                expected, ex.getReason(), ex.getMessage());
        assertEquals(expected, ex.getReason(), "拒绝原因必须可区分");
        return ex;
    }

    @Test
    void leaseAcquireRenewReleaseAndDistinguishableRejections() {
        MutableClock raw = new MutableClock(t0);
        // 生产路径也经过 GuardedSystemClock：注入可控原始时钟
        GuardedSystemClock clock = new GuardedSystemClock(raw::nowMillis);
        CoordinationCluster cluster = new CoordinationCluster("cluster-life", tempDir, clock);
        ClusterNode a = new ClusterNode("nodeA", cluster);
        ClusterNode b = new ClusterNode("nodeB", cluster);

        banner("t0 初始获取: A 获得 r1, TTL=1000ms");
        LeaseGrant ga = a.acquireLease("r1", 1000);
        System.out.printf("  [时间点] now=%d token=%d version=%d expire=%d%n",
                clock.nowMillis(), ga.fencingToken(), ga.version(), ga.expireAtMs());
        assertEquals(1, ga.fencingToken());
        assertTrue(ga.isValidAt(clock.nowMillis()));

        banner("未过期不得被抢占: B 获取被拒");
        expectReject(RejectReason.LEASE_HELD_BY_OTHER, () -> b.acquireLease("r1", 1000));

        banner("越权续约: B 持有 A 的凭证续约被拒(NOT_OWNER)");
        expectReject(RejectReason.LEASE_NOT_OWNER, () -> b.renewLease(ga, 1000));

        banner("越权释放: B 释放 A 的租约被拒(NOT_OWNER)");
        expectReject(RejectReason.LEASE_NOT_OWNER, () -> b.releaseLease(ga));

        banner("时间推进 1000ms, now == expire, 租约视为过期");
        raw.advance(1000);
        System.out.printf("  [时间点] now=%d expire=%d isValid=%s%n",
                clock.nowMillis(), ga.expireAtMs(), ga.isValidAt(clock.nowMillis()));
        assertFalse(ga.isValidAt(clock.nowMillis()));

        banner("过期后对失效租约续约 -> LEASE_EXPIRED");
        expectReject(RejectReason.LEASE_EXPIRED, () -> a.renewLease(ga, 1000));

        banner("过期后 B 可重新获取: 新令牌=2, version=2");
        LeaseGrant gb = b.acquireLease("r1", 1000);
        System.out.printf("  [时间点] now=%d token=%d version=%d holder=%s%n",
                clock.nowMillis(), gb.fencingToken(), gb.version(), gb.holderId());
        assertEquals(2, gb.fencingToken());
        assertEquals(2, gb.version());
        assertEquals("nodeB", gb.holderId());

        banner("A 用旧凭证续约 -> NOT_OWNER（当前持有者已是 B 的新令牌）");
        expectReject(RejectReason.LEASE_NOT_OWNER, () -> a.renewLease(ga, 1000));

        banner("B 正常续约延长 TTL");
        raw.advance(200);
        LeaseGrant gb2 = b.renewLease(gb, 1000);
        System.out.printf("  [时间点] now=%d 旧expire=%d 新expire=%d token保持=%d%n",
                clock.nowMillis(), gb.expireAtMs(), gb2.expireAtMs(), gb2.fencingToken());
        assertEquals(gb.fencingToken(), gb2.fencingToken());
        assertTrue(gb2.expireAtMs() >= gb.expireAtMs());

        banner("B 释放, 再重复释放 -> LEASE_ALREADY_RELEASED");
        b.releaseLease(gb2);
        System.out.printf("  [时间点] now=%d 首次释放成功 token=%d%n", clock.nowMillis(), gb2.fencingToken());
        expectReject(RejectReason.LEASE_ALREADY_RELEASED, () -> b.releaseLease(gb2));

        banner("释放后他人可立即获取, 无需等待过期");
        LeaseGrant gc = new ClusterNode("nodeC", cluster).acquireLease("r1", 500);
        System.out.printf("  [时间点] now=%d C 获取 token=%d%n", clock.nowMillis(), gc.fencingToken());
        assertEquals(3, gc.fencingToken());

        banner("非法 TTL -> ILLEGAL_ARGUMENT");
        expectReject(RejectReason.ILLEGAL_ARGUMENT, () -> a.acquireLease("r2", 0));

        banner("不同资源互不影响: A 可获取 r2 同时 C 持有 r1");
        LeaseGrant ga2 = a.acquireLease("r2", 1000);
        assertEquals(4, ga2.fencingToken());
        assertTrue(gc.isValidAt(clock.nowMillis()) && ga2.isValidAt(clock.nowMillis()));
        System.out.printf("  [判定] r1 holder=%s token=%d; r2 holder=%s token=%d 同时有效（不同资源允许）%n",
                gc.holderId(), gc.fencingToken(), ga2.holderId(), ga2.fencingToken());
    }
}
