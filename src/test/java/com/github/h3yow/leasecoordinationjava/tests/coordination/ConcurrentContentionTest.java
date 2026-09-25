package com.github.h3yow.leasecoordinationjava.tests.coordination;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.h3yow.leasecoordinationjava.coordination.cluster.LocalCluster;
import com.github.h3yow.leasecoordinationjava.coordination.cluster.LogicalNode;
import com.github.h3yow.leasecoordinationjava.coordination.lease.CoordinationException;
import com.github.h3yow.leasecoordinationjava.coordination.lease.Lease;
import com.github.h3yow.leasecoordinationjava.coordination.lease.LeaseRejectReason;
import com.github.h3yow.leasecoordinationjava.coordination.write.WriteOutcome;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 并发争抢：同一起跑线多节点竞争同一租约，恰好一个成功；
 * 同一幂等键并发重放，恰好一次副作用。
 */
class ConcurrentContentionTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentContentionTest.class);

    @Test
    void exactly_one_holder_under_parallel_acquire() throws Exception {
        int n = 16;
        try (LocalCluster cluster = new LocalCluster(n)) {
            String res = "/res/lock-contended";
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(n);
            List<java.util.concurrent.Future<?>> futures = new ArrayList<>();

            // 失败者的原因分布
            ConcurrentHashMap<String, AtomicInteger> rejectReasons = new ConcurrentHashMap<>();
            Set<String> winners = ConcurrentHashMap.newKeySet();

            for (int i = 0; i < n; i++) {
                LogicalNode node = cluster.node(i);
                futures.add(pool.submit(() -> {
                    try {
                        start.await();
                        Lease lease = node.leaseService().acquire(res, node.nodeId(), 5000);
                        winners.add(lease.holderId());
                        log.info("[CONTEND] {} 获取成功 token={}", node.nodeId(),
                                lease.fencingToken());
                    } catch (CoordinationException e) {
                        rejectReasons.computeIfAbsent(e.reason().name(),
                                k -> new AtomicInteger()).incrementAndGet();
                        log.info("[CONTEND] {} 被拒原因={} msg={}", node.nodeId(), e.reason(),
                                e.getMessage());
                    }
                    return null;
                }));
            }

            start.countDown();
            for (var f : futures) {
                f.get(10, TimeUnit.SECONDS);
            }
            pool.shutdown();

            log.info("[CONTEND-结果] 胜者={} 成功者数={} 拒绝分布={}", winners, winners.size(),
                    rejectReasons);
            assertThat(winners).hasSize(1); // 同一时刻恰好一个有效持有者
            int rejected = rejectReasons.values().stream().mapToInt(AtomicInteger::get).sum();
            assertThat(rejected).isEqualTo(n - 1);
            assertThat(rejectReasons.keySet()).containsOnly(
                    LeaseRejectReason.HELD_BY_OTHER.name());
        }
    }

    @Test
    void concurrent_same_key_submits_apply_exactly_once() throws Exception {
        int threads = 12;
        try (LocalCluster cluster = new LocalCluster(2)) {
            String res = "/res/dedup";
            Lease leader = cluster.node(0).leaseService()
                    .acquire(res, cluster.node(0).nodeId(), 60_000);
            long token = leader.fencingToken();

            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            List<java.util.concurrent.Future<WriteOutcome>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                // 同一请求的并发重试都来自原领导（重试/重放语义），载荷与幂等键完全一致
                var node = cluster.node(0);
                futures.add(pool.submit(() -> {
                    start.await();
                    return node.writeService().submit(res, node.nodeId(), token,
                            "idempotent-X", "payload-X");
                }));
            }
            start.countDown();
            Set<Long> seqs = new HashSet<>();
            int replays = 0;
            for (var f : futures) {
                WriteOutcome o = f.get(10, TimeUnit.SECONDS);
                seqs.add(o.sequence());
                if (o.replay()) {
                    replays++;
                }
            }
            pool.shutdown();

            log.info("[DEDUP-结果] 返回序号集合={} 首次数=1 重放数={} (总线程={})",
                    seqs, replays, threads);
            assertThat(seqs).hasSize(1);            // 全部返回同一个 seq
            assertThat(replays).isEqualTo(threads - 1); // 恰好一次真实副作用
        }
    }
}
