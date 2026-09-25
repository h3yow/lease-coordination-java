package com.github.h3yow.leasecoordinationjava.coordination;

import com.github.h3yow.leasecoordinationjava.coordination.clock.GuardedSystemClock;
import com.github.h3yow.leasecoordinationjava.coordination.lease.CoordinationException;
import com.github.h3yow.leasecoordinationjava.coordination.lease.LeaseGrant;
import com.github.h3yow.leasecoordinationjava.coordination.lease.RejectReason;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 时钟异常测试：原始系统时钟发生回拨/瞬时抖动时，受保护时钟保证判定单调、
 * 不出现重复授予，也不错误拒绝仍在 TTL 内的正常操作。
 */
class ClockAnomalyTest {

    @TempDir Path tempDir;

    @Test
    void rollbackAndJitterNeverBreakExclusivity() {
        AtomicLong raw = new AtomicLong(5_000_000L);
        GuardedSystemClock clock = new GuardedSystemClock(raw::get);
        CoordinationCluster cluster = new CoordinationCluster("cluster-clock", tempDir, clock);
        ClusterNode a = new ClusterNode("nodeA", cluster);
        ClusterNode b = new ClusterNode("nodeB", cluster);

        System.out.println("==== t=5000000 A 获取租约 TTL=1000, expire=5001000 ====");
        LeaseGrant ga = a.acquireLease("disk-1", 1000);
        assertEquals(5001000L, ga.expireAtMs());

        System.out.println("==== 原始时钟大幅回拨 5000000 -> 4900000（NTP/虚拟机迁移） ====");
        raw.set(4_900_000L);
        long guarded = clock.nowMillis();
        System.out.printf("  [时间点] 原始=%d 受保护=%d（必须保持 5000000 不回退）%n", raw.get(), guarded);
        assertEquals(5_000_000L, guarded, "时钟回拨不得改变判定时间");

        System.out.println("==== 回拨期间不得重复授予：B 仍被拒绝 ====");
        CoordinationException ex = assertThrows(CoordinationException.class,
                () -> b.acquireLease("disk-1", 1000));
        System.out.printf("  [判定] %s :: %s%n", ex.getReason(), ex.getMessage());
        assertEquals(RejectReason.LEASE_HELD_BY_OTHER, ex.getReason());

        System.out.println("==== 回拨期间不得错误拒绝：A 正常续约/写入仍成功 ====");
        LeaseGrant renewed = a.renewLease(ga, 1000);
        assertEquals(5_001_000L, renewed.expireAtMs());
        assertDoesNotThrow(() -> a.submitWrite("disk-1", "w1", ga.fencingToken(), 1L));
        System.out.printf("  [判定] 回拨期间续约/写入成功 token=%d expire=%d now=%d%n",
                renewed.fencingToken(), renewed.expireAtMs(), clock.nowMillis());

        System.out.println("==== 瞬时抖动: 4900000 <-> 4999999 <-> 4850000, 受保护时间纹丝不动 ====");
        for (long v : new long[]{4_999_999L, 4_850_000L, 4_999_000L, 4_900_000L}) {
            raw.set(v);
            assertEquals(5_000_000L, clock.nowMillis(), "抖动期间时间必须冻结在历史最高点");
        }
        System.out.printf("  [判定] 抖动后 now=%d 租约互斥仍成立（未提前过期、未重复授予）%n", clock.nowMillis());

        System.out.println("==== 原始时钟恢复并越过真实过期点 5001001: 租约才允许过期 ====");
        raw.set(5_001_001L);
        assertEquals(5_001_001L, clock.nowMillis());
        assertFalse(ga.isValidAt(clock.nowMillis()));
        LeaseGrant gb = b.acquireLease("disk-1", 1000);
        System.out.printf("  [判定] 真实过界后 B 才获得新租约 token=%d version=%d now=%d%n",
                gb.fencingToken(), gb.version(), clock.nowMillis());
        assertEquals(2, gb.fencingToken());

        System.out.println("==== 再次回拨：旧持有者 A 不会“复活”，其旧令牌写入仍被栅栏拒绝 ====");
        raw.set(4_000_000L);
        assertEquals(5_001_001L, clock.nowMillis(), "回拨后判定时间仍不回退");
        ex = assertThrows(CoordinationException.class,
                () -> a.submitWrite("disk-1", "late", ga.fencingToken(), 2L));
        System.out.printf("  [判定] %s :: %s%n", ex.getReason(), ex.getMessage());
        assertEquals(RejectReason.FENCING_TOKEN_STALE, ex.getReason());
    }
}
