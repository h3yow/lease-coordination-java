package com.github.h3yow.leasecoordinationjava.tests.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.h3yow.leasecoordinationjava.coordination.cluster.LocalCluster;
import com.github.h3yow.leasecoordinationjava.coordination.cluster.LogicalNode;
import com.github.h3yow.leasecoordinationjava.coordination.clock.SimulatedClock;
import com.github.h3yow.leasecoordinationjava.coordination.lease.CoordinationException;
import com.github.h3yow.leasecoordinationjava.coordination.lease.Lease;
import com.github.h3yow.leasecoordinationjava.coordination.lease.LeaseRejectReason;
import com.github.h3yow.leasecoordinationjava.coordination.write.WriteOutcome;
import com.github.h3yow.leasecoordinationjava.coordination.write.WriteRejectReason;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 计划内换主（主动交接）：当前持有者指名继任者，继任者立即以严格更新的令牌接手；
 * 交接生效后旧令牌的任何提交（含重放）一律显式拒绝；继任者可查到已提交序号继续写。
 */
class PlannedHandoverTest {

    private static final Logger log = LoggerFactory.getLogger(PlannedHandoverTest.class);

    @Test
    void handover_transfers_leadership_immediately_and_successor_resumes_from_committed_seq() {
        try (LocalCluster cluster = new LocalCluster(3)) {
            LogicalNode a = cluster.node(0);
            LogicalNode b = cluster.node(1);
            LogicalNode c = cluster.node(2);
            String res = "/res/pipeline";

            Lease la = a.leaseService().acquire(res, a.nodeId(), 60_000);
            assertThat(la.fencingToken()).isEqualTo(1L);
            a.writeService().submit(res, a.nodeId(), 1L, "step-1", "payload-1");
            a.writeService().submit(res, a.nodeId(), 1L, "step-2", "payload-2");
            log.info("[HANDOVER-前置] A token=1 已提交到 seq=2，租约远未到期，发起交接 A→B");

            // 交接成功：B 立即成为有效持有者，令牌严格更新（不等旧租约到期）
            Lease lb = a.leaseService().handover(res, a.nodeId(), b.nodeId(), 60_000);
            assertThat(lb.holderId()).isEqualTo(b.nodeId());
            assertThat(lb.fencingToken()).isEqualTo(2L);
            assertThat(lb.fencingToken()).isGreaterThan(la.fencingToken());

            // 任何时刻只有一个有效持有者：C 抢锁被拒；B 重复获取幂等返回同一租约
            assertThatThrownBy(() -> c.leaseService().acquire(res, c.nodeId(), 60_000))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(LeaseRejectReason.HELD_BY_OTHER);
            Lease bAgain = b.leaseService().acquire(res, b.nodeId(), 60_000);
            assertThat(bAgain.fencingToken()).isEqualTo(2L);

            // 旧持有者失去一切权限：续约/释放都被 NOT_HOLDER 拒绝
            assertThatThrownBy(() -> a.leaseService().renew(res, a.nodeId(), 60_000))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(LeaseRejectReason.NOT_HOLDER);
            assertThatThrownBy(() -> a.leaseService().release(res, a.nodeId()))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(LeaseRejectReason.NOT_HOLDER);

            // 继任者查到已提交序号，从已知进度接着写，序号连续不跳号
            long committed = b.writeService().committedSequence(res);
            log.info("[HANDOVER-接手] B 查到 committedSeq={}，从下一序号继续写", committed);
            assertThat(committed).isEqualTo(2L);
            WriteOutcome w3 = b.writeService().submit(res, b.nodeId(), 2L, "step-3", "payload-3");
            assertThat(w3.accepted()).isTrue();
            assertThat(w3.sequence()).isEqualTo(3L);
        }
    }

    @Test
    void handover_rejections_have_distinguishable_reasons() {
        try (LocalCluster cluster = new LocalCluster(2)) {
            LogicalNode a = cluster.node(0);
            LogicalNode b = cluster.node(1);
            String res = "/res/handover-reject";

            // 从未授予：UNKNOWN_LEASE
            assertThatThrownBy(() -> a.leaseService().handover(res, a.nodeId(), b.nodeId(), 1000))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(LeaseRejectReason.UNKNOWN_LEASE);

            a.leaseService().acquire(res, a.nodeId(), 1000);

            // 越权发起：NOT_HOLDER
            assertThatThrownBy(() -> b.leaseService().handover(res, b.nodeId(), b.nodeId(), 1000))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(LeaseRejectReason.NOT_HOLDER);

            // 继任者非法：交给自己 / 空白 → INVALID_SUCCESSOR
            assertThatThrownBy(() -> a.leaseService().handover(res, a.nodeId(), a.nodeId(), 1000))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(LeaseRejectReason.INVALID_SUCCESSOR);
            assertThatThrownBy(() -> a.leaseService().handover(res, a.nodeId(), "  ", 1000))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(LeaseRejectReason.INVALID_SUCCESSOR);

            // 已过期：EXPIRED（过期租约不能拿来交接，继任者需重新获取）
            cluster.clock().advance(1000);
            assertThatThrownBy(() -> a.leaseService().handover(res, a.nodeId(), b.nodeId(), 1000))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(LeaseRejectReason.EXPIRED);

            // 已释放：ALREADY_RELEASED
            Lease la2 = a.leaseService().acquire(res, a.nodeId(), 1000);
            a.leaseService().release(res, a.nodeId());
            assertThatThrownBy(() -> a.leaseService().handover(res, a.nodeId(), b.nodeId(), 1000))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(LeaseRejectReason.ALREADY_RELEASED);
            log.info("[HANDOVER-拒绝] 五种拒绝原因均可区分，token={} 未被消耗",
                    la2.fencingToken());
        }
    }

    @Test
    void after_handover_old_token_is_rejected_even_for_previously_applied_requests() {
        try (LocalCluster cluster = new LocalCluster(2)) {
            LogicalNode a = cluster.node(0);
            LogicalNode b = cluster.node(1);
            String res = "/res/handover-boundary";

            a.leaseService().acquire(res, a.nodeId(), 60_000);
            WriteOutcome first = a.writeService().submit(res, a.nodeId(), 1L, "req-1", "v1");
            assertThat(first.sequence()).isEqualTo(1L);

            a.leaseService().handover(res, a.nodeId(), b.nodeId(), 60_000);
            log.info("[BOUNDARY] 交接生效 token 1→2，旧令牌的提交边界开始收紧");

            // 旧令牌的新提交：明确拒绝 SUPERSEDED_BY_HANDOVER
            assertThatThrownBy(() -> a.writeService().submit(res, a.nodeId(), 1L, "req-new", "x"))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(WriteRejectReason.SUPERSEDED_BY_HANDOVER);

            // 旧令牌重放"之前确实成功落过库"的请求：绝不按重复提交放行，同样拒绝
            assertThatThrownBy(() -> a.writeService().submit(res, a.nodeId(), 1L, "req-1", "v1"))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(WriteRejectReason.SUPERSEDED_BY_HANDOVER);
            log.info("[BOUNDARY] 旧令牌重放已落库请求也被 SUPERSEDED_BY_HANDOVER 拒绝");

            // 真正的重放（当前持有者 + 有效令牌 + 同一请求）：仍返回首次结果，无二次副作用
            WriteOutcome bFirst = b.writeService().submit(res, b.nodeId(), 2L, "req-2", "v2");
            assertThat(bFirst.sequence()).isEqualTo(2L);
            WriteOutcome bReplay = b.writeService().submit(res, b.nodeId(), 2L, "req-2", "v2");
            assertThat(bReplay.replay()).isTrue();
            assertThat(bReplay.sequence()).isEqualTo(2L);

            // 新提交顺着已有序号往下排：不跳号、不覆盖
            WriteOutcome w3 = b.writeService().submit(res, b.nodeId(), 2L, "req-3", "v3");
            assertThat(w3.sequence()).isEqualTo(3L);
            assertThat(b.writeService().committedSequence(res)).isEqualTo(3L);
        }
    }

    @Test
    void handover_survives_full_process_restart(@TempDir Path dir) {
        SimulatedClock clock = new SimulatedClock();
        String res = "/res/handover-durable";

        try (LocalCluster first = new LocalCluster(dir, clock, 2)) {
            LogicalNode a = first.node(0);
            a.leaseService().acquire(res, a.nodeId(), 60_000);
            a.writeService().submit(res, a.nodeId(), 1L, "k-1", "v1");
            a.leaseService().handover(res, a.nodeId(), first.node(1).nodeId(), 60_000);
            first.node(1).writeService().submit(res, first.node(1).nodeId(), 2L, "k-2", "v2");
            log.info("[PROC-1] 交接完成并写入 seq=2，进程退出");
        }

        try (LocalCluster second = new LocalCluster(dir, clock, 2)) {
            LogicalNode a2 = second.node(0);
            LogicalNode b2 = second.node(1);

            // WAL 重放后：B 仍是持有者、令牌 2、交接边界恢复
            Lease restored = b2.leaseService().inspect(res).orElseThrow();
            assertThat(restored.holderId()).isEqualTo(b2.nodeId());
            assertThat(restored.fencingToken()).isEqualTo(2L);
            assertThat(b2.writeService().committedSequence(res)).isEqualTo(2L);

            // 旧令牌（含重放已落库请求）仍被 SUPERSEDED_BY_HANDOVER 拒绝
            assertThatThrownBy(() -> a2.writeService().submit(res, a2.nodeId(), 1L, "k-1", "v1"))
                    .isInstanceOf(CoordinationException.class)
                    .extracting(e -> ((CoordinationException) e).reason())
                    .isEqualTo(WriteRejectReason.SUPERSEDED_BY_HANDOVER);

            // B 的重放与新写入照常：重放返回原序号，新写入接续 seq=3
            WriteOutcome replay = b2.writeService().submit(res, b2.nodeId(), 2L, "k-2", "v2");
            assertThat(replay.replay()).isTrue();
            assertThat(replay.sequence()).isEqualTo(2L);
            WriteOutcome w3 = b2.writeService().submit(res, b2.nodeId(), 2L, "k-3", "v3");
            assertThat(w3.sequence()).isEqualTo(3L);
            log.info("[PROC-2] 交接边界经 WAL 重放恢复，旧令牌仍被拒，序号接续无重号");
        }
    }
}
