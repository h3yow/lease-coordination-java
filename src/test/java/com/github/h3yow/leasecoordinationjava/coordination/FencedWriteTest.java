package com.github.h3yow.leasecoordinationjava.coordination;

import com.github.h3yow.leasecoordinationjava.coordination.clock.GuardedSystemClock;
import com.github.h3yow.leasecoordinationjava.coordination.event.CoordinationEvent;
import com.github.h3yow.leasecoordinationjava.coordination.lease.CoordinationException;
import com.github.h3yow.leasecoordinationjava.coordination.lease.LeaseGrant;
import com.github.h3yow.leasecoordinationjava.coordination.lease.RejectReason;
import com.github.h3yow.leasecoordinationjava.coordination.store.DurableEventStore;
import com.github.h3yow.leasecoordinationjava.coordination.write.WriteResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 领导权变更下的有序写入测试：fencing 令牌栅栏、过期领导拒绝、请求重放幂等去重。
 */
class FencedWriteTest {

    private final long t0 = 3_000_000L;
    @TempDir Path tempDir;

    private void expect(RejectReason reason, Runnable r) {
        CoordinationException ex = assertThrows(CoordinationException.class, r::run);
        System.out.printf("  [判定] 预期=%s 实际=%s :: %s%n", reason, ex.getReason(), ex.getMessage());
        assertEquals(reason, ex.getReason());
    }

    private long countWriteCommitted(String clusterName) {
        List<CoordinationEvent> events = new DurableEventStore(tempDir, clusterName).replay();
        return events.stream().filter(e -> e instanceof CoordinationEvent.WriteCommitted).count();
    }

    @Test
    void fencedWritesReplayIdempotencyAndStaleLeader() {
        MutableClock raw = new MutableClock(t0);
        GuardedSystemClock clock = new GuardedSystemClock(raw::nowMillis);
        String name = "cluster-fence";
        CoordinationCluster cluster = new CoordinationCluster(name, tempDir, clock);
        ClusterNode a = new ClusterNode("nodeA", cluster);
        ClusterNode b = new ClusterNode("nodeB", cluster);

        System.out.println("==== A 成为领导并以 token=1 提交写入 ====");
        LeaseGrant ga = a.acquireLease("db", 1000);
        WriteResult w1 = a.submitWrite("db", "req-1", ga.fencingToken(), 1001L);
        System.out.printf("  [时间点] now=%d token=%d committedAt=%d replay=%s%n",
                clock.nowMillis(), w1.fencingToken(), w1.committedAtMs(), w1.replay());
        assertFalse(w1.replay());
        assertEquals(1, countWriteCommitted(name));

        System.out.println("==== 同一请求重放 3 次: 必须返回首次结果且无重复副作用 ====");
        for (int i = 0; i < 3; i++) {
            WriteResult replay = a.submitWrite("db", "req-1", ga.fencingToken(), 1001L);
            assertEquals(w1.committedAtMs(), replay.committedAtMs(), "重放必须返回首次提交时间");
            assertTrue(replay.replay(), "重放结果必须显式标记 replay=true");
        }
        System.out.printf("  [判定] 重放3次后持久化写入事件数仍=%d（期望1）, now=%d%n",
                countWriteCommitted(name), clock.nowMillis());
        assertEquals(1, countWriteCommitted(name), "重放不得产生重复副作用");

        System.out.println("==== 不同 requestId 正常产生第二条副作用 ====");
        WriteResult w2 = a.submitWrite("db", "req-2", ga.fencingToken(), 1002L);
        assertFalse(w2.replay());
        assertEquals(2, countWriteCommitted(name));

        System.out.println("==== 伪造令牌(从未授予) -> FENCING_TOKEN_UNKNOWN ====");
        expect(RejectReason.FENCING_TOKEN_UNKNOWN,
                () -> a.submitWrite("db", "req-x", 999L, 1L));

        System.out.println("==== A 释放领导权, B 接管(token=2) ====");
        a.releaseLease(ga);
        LeaseGrant gb = b.acquireLease("db", 1000);
        System.out.printf("  [时间点] now=%d 新领导=%s token=%d%n",
                clock.nowMillis(), gb.holderId(), gb.fencingToken());
        assertEquals(2, gb.fencingToken());

        System.out.println("==== 旧领导 A 用过期令牌 token=1 写入 -> FENCING_TOKEN_STALE ====");
        expect(RejectReason.FENCING_TOKEN_STALE,
                () -> a.submitWrite("db", "req-old", ga.fencingToken(), 2001L));

        System.out.println("==== B 用 token=2 写入成功；越权者 A 冒用 token=2 -> NOT_OWNER ====");
        WriteResult w3 = b.submitWrite("db", "req-3", gb.fencingToken(), 2003L);
        assertFalse(w3.replay());
        expect(RejectReason.LEASE_NOT_OWNER,
                () -> a.submitWrite("db", "req-hijack", gb.fencingToken(), 2004L));

        System.out.println("==== 时钟越过 TTL: 持令牌但领导权已过期 -> FENCING_LEASE_EXPIRED ====");
        raw.advance(1001);
        System.out.printf("  [时间点] now=%d token=2 expire=%d%n", clock.nowMillis(), gb.expireAtMs());
        expect(RejectReason.FENCING_LEASE_EXPIRED,
                () -> b.submitWrite("db", "req-late", gb.fencingToken(), 2005L));

        System.out.println("==== 过期写入即使 requestId 与历史相同也不允许命中幂等绕过校验 ====");
        expect(RejectReason.FENCING_LEASE_EXPIRED,
                () -> b.submitWrite("db", "req-3", gb.fencingToken(), 2003L));

        System.out.println("==== 新领导 C(token=3) 的写入栅栏高于一切旧令牌 ====");
        LeaseGrant gc = new ClusterNode("nodeC", cluster).acquireLease("db", 1000);
        WriteResult w4 = new ClusterNode("nodeC", cluster)
                .submitWrite("db", "req-4", gc.fencingToken(), 3001L);
        assertEquals(3, w4.fencingToken());
        System.out.printf("  [判定] 最高令牌写入成功 token=%d committedAt=%d, 持久化写入事件总数=%d%n",
                w4.fencingToken(), w4.committedAtMs(), countWriteCommitted(name));
        assertEquals(4, countWriteCommitted(name));
    }
}
