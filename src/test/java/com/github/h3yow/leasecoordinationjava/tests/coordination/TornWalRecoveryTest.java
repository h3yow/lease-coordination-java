package com.github.h3yow.leasecoordinationjava.tests.coordination;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.h3yow.leasecoordinationjava.coordination.cluster.LocalCluster;
import com.github.h3yow.leasecoordinationjava.coordination.cluster.LogicalNode;
import com.github.h3yow.leasecoordinationjava.coordination.clock.SimulatedClock;
import com.github.h3yow.leasecoordinationjava.coordination.lease.Lease;
import com.github.h3yow.leasecoordinationjava.coordination.write.WriteOutcome;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 最坏情况崩溃收敛：进程在 WAL 落盘到一半时崩掉，文件尾部留下一段不完整记录。
 * 重启后必须：正常起来、回到最后一次完整提交的状态、残缺尾巴不算已生效写入、
 * 已确认的写入不丢、序号不重复发放。
 */
class TornWalRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(TornWalRecoveryTest.class);

    @Test
    void restart_with_torn_wal_tail_recovers_to_last_complete_commit(@TempDir Path dir)
            throws Exception {
        SimulatedClock clock = new SimulatedClock();
        String res = "/res/torn-tail";
        long tokenB;

        // ---- 第一个"进程"：授予 → 写2条 → 交接给 B → B 再写1条，然后"落盘到一半崩掉" ----
        try (LocalCluster first = new LocalCluster(dir, clock, 2)) {
            LogicalNode a = first.node(0);
            LogicalNode b = first.node(1);
            Lease leaseA = a.leaseService().acquire(res, a.nodeId(), 60_000);
            a.writeService().submit(res, a.nodeId(), leaseA.fencingToken(), "c-1", "v1");
            a.writeService().submit(res, a.nodeId(), leaseA.fencingToken(), "c-2", "v2");
            Lease leaseB = a.leaseService().handover(res, a.nodeId(), b.nodeId(), 60_000);
            tokenB = leaseB.fencingToken();
            b.writeService().submit(res, b.nodeId(), tokenB, "c-3", "v3");
            log.info("[TORN-前置] 完整记录：G + W(c-1) + W(c-2) + H(token={}) + W(c-3)，"
                    + "lastSeq=3", tokenB);
        } // 进程退出

        // 模拟"落盘到一半"：往 WAL 尾部追加半条残缺记录（未确认成功的写入）
        Path wal = dir.resolve("wal").resolve("%2Fres%2Ftorn-tail.log");
        assertThat(wal).exists();
        Files.writeString(wal, "W|" + tokenB + "|4|aGVh", // 半截行：字段不全、无换行
                StandardOpenOption.APPEND);
        log.info("[TORN-注入] 向 {} 追加残缺尾部记录（模拟落盘中崩溃）", wal.getFileName());

        // ---- 第二个"进程"：重放 WAL，必须正常起来并停在最后一次完整提交 ----
        try (LocalCluster second = new LocalCluster(dir, clock, 2)) {
            LogicalNode b2 = second.node(1);

            // 交接状态被 H 记录恢复：B 仍是持有者、token 连续
            Lease restored = b2.leaseService().inspect(res).orElseThrow();
            log.info("[TORN-恢复] holder={} token={} lastSeq={}",
                    restored.holderId(), restored.fencingToken(),
                    b2.writeService().committedSequence(res));
            assertThat(restored.holderId()).isEqualTo(b2.nodeId());
            assertThat(restored.fencingToken()).isEqualTo(tokenB);

            // 残缺尾巴不算已生效：已确认的 3 条还在，序号停在 3
            assertThat(b2.writeService().committedSequence(res)).isEqualTo(3L);

            // 已确认写入重放仍幂等（崩溃前已确认的不丢）
            WriteOutcome replay = b2.writeService().submit(res, b2.nodeId(), tokenB,
                    "c-3", "v3");
            assertThat(replay.replay()).isTrue();
            assertThat(replay.sequence()).isEqualTo(3L);

            // 新提交接续发号：seq=4 重新分配给真正落盘的写入，不重复、不跳号
            WriteOutcome w4 = b2.writeService().submit(res, b2.nodeId(), tokenB,
                    "c-4", "v4");
            assertThat(w4.sequence()).isEqualTo(4L);
            assertThat(b2.writeService().committedSequence(res)).isEqualTo(4L);
            log.info("[TORN-验证] 残缺尾巴被丢弃，已确认写入保留，新写入 seq={} 接续发放",
                    w4.sequence());
        }
    }
}
