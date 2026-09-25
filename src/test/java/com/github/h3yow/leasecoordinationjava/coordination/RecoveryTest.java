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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 节点故障 / 进程重启 / 日志尾部损坏的收敛测试。
 */
class RecoveryTest {

    private final long t0 = 4_000_000L;
    @TempDir Path tempDir;

    private void expect(RejectReason reason, Runnable r) {
        CoordinationException ex = assertThrows(CoordinationException.class, r::run);
        System.out.printf("  [判定] 预期=%s 实际=%s :: %s%n", reason, ex.getReason(), ex.getMessage());
        assertEquals(reason, ex.getReason());
    }

    @Test
    void crashAndRestartRebuildsStateFromDurableLog() {
        MutableClock raw = new MutableClock(t0);
        GuardedSystemClock clock = new GuardedSystemClock(raw::nowMillis);
        String name = "cluster-restart";

        // ---- 崩溃前：A 持有租约(token=1)并提交一条写入 ----
        LeaseGrant saved;
        CoordinationCluster cluster = new CoordinationCluster(name, tempDir, clock);
        ClusterNode a = new ClusterNode("nodeA", cluster);
        saved = a.acquireLease("vol", 5000);
        WriteResult before = a.submitWrite("vol", "boot-req", saved.fencingToken(), 777L);
        System.out.printf("  [时间点] 崩溃前 now=%d token=%d expire=%d 写入committedAt=%d%n",
                clock.nowMillis(), saved.fencingToken(), saved.expireAtMs(), before.committedAtMs());

        System.out.println("==== 模拟整个协调进程崩溃：丢弃集群与节点对象 ====");
        a.crash();
        cluster = null;
        raw.advance(1000);

        System.out.println("==== 重启：相同数据目录重建集群，重放事件日志 ====");
        CoordinationCluster restored = new CoordinationCluster(name, tempDir, clock);
        ClusterNode aRestarted = new ClusterNode("nodeA", restored);
        ClusterNode b = new ClusterNode("nodeB", restored);

        System.out.printf("  [时间点] 重启后 now=%d, 原租约 expire=%d, 仍在TTL内=%s%n",
                clock.nowMillis(), saved.expireAtMs(), saved.isValidAt(clock.nowMillis()));

        System.out.println("==== 恢复后互斥不变：B 仍不能抢占 ====");
        expect(RejectReason.LEASE_HELD_BY_OTHER, () -> b.acquireLease("vol", 5000));

        System.out.println("==== 恢复后 A 凭持久凭证可续约（领导权延续） ====");
        LeaseGrant renewed = aRestarted.renewLease(saved, 5000);
        System.out.printf("  [判定] 续约成功 token保持=%d 新expire=%d now=%d%n",
                renewed.fencingToken(), renewed.expireAtMs(), clock.nowMillis());
        assertEquals(saved.fencingToken(), renewed.fencingToken());

        System.out.println("==== 恢复后请求重放仍幂等：不产生第二条写入事件 ====");
        long writesBefore = writeCount(name);
        WriteResult replay = aRestarted.submitWrite("vol", "boot-req", saved.fencingToken(), 777L);
        assertTrue(replay.replay());
        assertEquals(before.committedAtMs(), replay.committedAtMs(), "提交时间必须取自首次记录");
        assertEquals(writesBefore, writeCount(name));
        System.out.printf("  [判定] 重放命中首次committedAt=%d, 写入事件数=%d（未增加）%n",
                replay.committedAtMs(), writeCount(name));

        System.out.println("==== 恢复后令牌序号严格延续：新资源租约拿到 token=2 ====");
        LeaseGrant g2 = b.acquireLease("other-vol", 5000);
        assertEquals(2, g2.fencingToken());
        System.out.printf("  [判定] 新授予 token=%d（非从1重新开始，杜绝重复授予）%n", g2.fencingToken());

        System.out.println("==== 再次崩溃重启：已续约的过期时间同样恢复 ====");
        restored = null;
        raw.advance(4500); // 距首次获取已过 5500ms，但续约后 expire 已顺延
        CoordinationCluster restored2 = new CoordinationCluster(name, tempDir, clock);
        ClusterNode a2 = new ClusterNode("nodeA", restored2);
        System.out.printf("  [时间点] now=%d 续约后expire=%d 有效=%s%n",
                clock.nowMillis(), renewed.expireAtMs(), renewed.isValidAt(clock.nowMillis()));
        assertTrue(renewed.isValidAt(clock.nowMillis()), "续约必须跨重启生效");
        LeaseGrant renewed2 = a2.renewLease(renewed, 5000);
        assertEquals(saved.fencingToken(), renewed2.fencingToken());
    }

    @Test
    void tornLogLineFromCrashIsTruncatedNotReplayed() throws Exception {
        MutableClock raw = new MutableClock(t0);
        GuardedSystemClock clock = new GuardedSystemClock(raw::nowMillis);
        String name = "cluster-torn";

        DurableEventStore store = new DurableEventStore(tempDir, name);
        CoordinationCluster cluster = new CoordinationCluster(name, tempDir, clock);
        new ClusterNode("nodeA", cluster).acquireLease("r", 1000);

        System.out.println("==== 模拟崩溃瞬间写坏日志末行（半条 JSON） ====");
        Path logFile = store.getLogFile();
        Files.writeString(logFile,
                "{\"type\":\"LEASE_ACQUIRED\",\"resourceId\":\"r\",\"holderId\":\"ghos",
                StandardOpenOption.APPEND);
        System.out.printf("  [时间点] 损坏前已持久化完整事件=%d 条, 追加残缺行 now=%d%n",
                store.replay().size(), clock.nowMillis());

        List<CoordinationEvent> replayed = store.replay();
        assertEquals(1, replayed.size(), "残缺行必须被丢弃，只重放完整事件");
        System.out.printf("  [判定] 重放完整事件数=%d（幽灵授予未进入状态）%n", replayed.size());

        System.out.println("==== 损坏修复后集群仍可继续工作且不授予幽灵持有者 ====");
        CoordinationCluster rebuilt = new CoordinationCluster(name, tempDir, clock);
        ClusterNode b = new ClusterNode("nodeB", rebuilt);
        // nodeA 的租约仍未过期：ghost 不存在，B 被 A 阻挡
        expect(RejectReason.LEASE_HELD_BY_OTHER, () -> b.acquireLease("r", 1000));
    }

    private long writeCount(String clusterName) {
        return new DurableEventStore(tempDir, clusterName).replay().stream()
                .filter(e -> e instanceof CoordinationEvent.WriteCommitted).count();
    }
}
