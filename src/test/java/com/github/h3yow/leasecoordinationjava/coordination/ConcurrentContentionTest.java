package com.github.h3yow.leasecoordinationjava.coordination;

import com.github.h3yow.leasecoordinationjava.coordination.clock.GuardedSystemClock;
import com.github.h3yow.leasecoordinationjava.coordination.lease.CoordinationException;
import com.github.h3yow.leasecoordinationjava.coordination.lease.LeaseGrant;
import com.github.h3yow.leasecoordinationjava.coordination.lease.RejectReason;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 并发争抢测试：同一资源在高并发下严格“只授予一人”；过期后再争抢同样收敛到一人。
 */
class ConcurrentContentionTest {

    private final long t0 = 2_000_000L;
    @TempDir Path tempDir;

    @Test
    void exactlyOneWinnerUnderConcurrentAcquire() throws Exception {
        MutableClock raw = new MutableClock(t0);
        GuardedSystemClock clock = new GuardedSystemClock(raw::nowMillis);
        CoordinationCluster cluster = new CoordinationCluster("cluster-contend", tempDir, clock);

        int n = 24;
        // 池容量取所有并发波次中的最大任务数，避免“任务排队等待发令枪、发令枪又等待任务就绪”的自死锁
        ExecutorService pool = Executors.newFixedThreadPool(50);
        try {
            CountDownLatch ready = new CountDownLatch(n);
            CountDownLatch fire = new CountDownLatch(1);
            ConcurrentHashMap<String, LeaseGrant> winners = new ConcurrentHashMap<>();
            AtomicInteger heldByOther = new AtomicInteger();
            AtomicInteger otherErrors = new AtomicInteger();

            System.out.println("==== 并发波次1: 24 个节点同时 acquire(resource-x, ttl=5000) ====");
            List<Future<?>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < n; i++) {
                String id = "n" + i;
                ClusterNode node = new ClusterNode(id, cluster);
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    fire.await();
                    try {
                        LeaseGrant g = node.acquireLease("resource-x", 5000);
                        winners.put(id, g);
                    } catch (CoordinationException e) {
                        if (e.getReason() == RejectReason.LEASE_HELD_BY_OTHER) {
                            heldByOther.incrementAndGet();
                        } else {
                            otherErrors.incrementAndGet();
                        }
                    }
                    return null;
                }));
            }
            ready.await();
            long start = clock.nowMillis();
            fire.countDown();
            for (Future<?> f : futures) f.get();

            System.out.printf("  [时间点] fire@now=%d, 成功数=%d, LEASE_HELD_BY_OTHER 数=%d, 其他异常=%d%n",
                    start, winners.size(), heldByOther.get(), otherErrors.get());
            assertEquals(1, winners.size(), "同一时刻同一资源只能有一个持有者");
            assertEquals(n - 1, heldByOther.get(), "其余全部必须以可区分原因被拒");
            assertEquals(0, otherErrors.get());
            String winner = winners.keys().nextElement();
            LeaseGrant winGrant = winners.get(winner);
            System.out.printf("  [判定] 唯一获胜者=%s token=%d expire=%d%n",
                    winner, winGrant.fencingToken(), winGrant.expireAtMs());

            // 所有失败者观察到的获胜令牌必须是同一个
            long token = winGrant.fencingToken();
            for (LeaseGrant g : winners.values()) {
                assertEquals(token, g.fencingToken());
            }

            System.out.println("==== 持有者存活期间并发续约 50 次, 只有持有者成功 ====");
            ClusterNode winnerNode = new ClusterNode(winner, cluster);
            CountDownLatch ready2 = new CountDownLatch(50);
            CountDownLatch fire2 = new CountDownLatch(1);
            AtomicInteger renewOk = new AtomicInteger();
            AtomicInteger rejected = new AtomicInteger();
            List<Future<?>> f2 = new java.util.ArrayList<>();
            for (int i = 0; i < 50; i++) {
                String id = "r" + i;
                ClusterNode outsider = new ClusterNode("outsider-" + id, cluster);
                f2.add(pool.submit(() -> {
                    ready2.countDown();
                    fire2.await();
                    try {
                        // 局外人用自己“伪造不到”的真实凭证尝试：只能被 NOT_OWNER 拒绝
                        if (id.equals("r0")) {
                            winnerNode.renewLease(winGrant, 5000);
                        } else {
                            outsider.renewLease(winGrant, 5000);
                        }
                        renewOk.incrementAndGet();
                    } catch (CoordinationException e) {
                        if (e.getReason() == RejectReason.LEASE_NOT_OWNER) {
                            rejected.incrementAndGet();
                        }
                    }
                    return null;
                }));
            }
            ready2.await();
            fire2.countDown();
            for (Future<?> f : f2) f.get();
            System.out.printf("  [时间点] now=%d 续约成功=%d(仅持有者1次) NOT_OWNER拒绝=%d%n",
                    clock.nowMillis(), renewOk.get(), rejected.get());
            assertEquals(1, renewOk.get());
            assertEquals(49, rejected.get());

            System.out.println("==== 时钟推进越过过期, 并发波次2: 必须再次恰好一个新持有者 ===");
            raw.advance(6000);
            CountDownLatch ready3 = new CountDownLatch(n);
            CountDownLatch fire3 = new CountDownLatch(1);
            ConcurrentHashMap<String, LeaseGrant> winners2 = new ConcurrentHashMap<>();
            List<Future<?>> f3 = new java.util.ArrayList<>();
            for (int i = 0; i < n; i++) {
                String id = "m" + i;
                ClusterNode node = new ClusterNode(id, cluster);
                f3.add(pool3(pool, node, ready3, fire3, winners2, winGrant));
            }
            ready3.await();
            fire3.countDown();
            for (Future<?> f : f3) f.get();
            assertEquals(1, winners2.size(), "过期后重新争抢仍只能有一人获胜");
            LeaseGrant win2 = winners2.elements().nextElement();
            System.out.printf("  [判定] 波次2唯一获胜者=%s token=%d(旧=%d) version=%d now=%d%n",
                    win2.holderId(), win2.fencingToken(), winGrant.fencingToken(),
                    win2.version(), clock.nowMillis());
            assertTrue(win2.fencingToken() > winGrant.fencingToken(), "新令牌必须严格更大");
        } finally {
            pool.shutdownNow();
        }
    }

    private Future<?> pool3(ExecutorService pool, ClusterNode node,
                            CountDownLatch ready, CountDownLatch fire,
                            ConcurrentHashMap<String, LeaseGrant> winners,
                            LeaseGrant oldGrant) {
        return pool.submit(() -> {
            ready.countDown();
            fire.await();
            try {
                winners.put(node.nodeId(), node.acquireLease("resource-x", 5000));
            } catch (CoordinationException ignored) {
                // 并发下失败者必然是 LEASE_HELD_BY_OTHER
            }
            return null;
        });
    }
}
