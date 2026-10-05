package com.github.h3yow.leasecoordinationjava.tests.coordination;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.h3yow.leasecoordinationjava.coordination.cluster.LocalCluster;
import com.github.h3yow.leasecoordinationjava.coordination.cluster.LogicalNode;
import com.github.h3yow.leasecoordinationjava.coordination.clock.SimulatedClock;
import com.github.h3yow.leasecoordinationjava.coordination.lease.Lease;
import com.github.h3yow.leasecoordinationjava.coordination.write.WriteOutcome;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 最坏情况崩溃收敛：进程在落盘途中崩掉，WAL 尾部留下一段不完整记录。
 * 重启必须能起来、回到最后一次完整提交的状态：残缺尾巴既不算已生效写入，
 * 也不阻止启动；已确认成功的写入不丢，序号不重复发号。
 */
class TornWalRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(TornWalRecoveryTest.class);

    private static Path walFile(Path dir, String resourceId) {
        String name = URLEncoder.encode(resourceId, StandardCharsets.UTF_8)
                .replace("+", "%20") + ".log";
        return dir.resolve("wal").resolve(name);
    }

    private static void appendTornBytes(Path file, String fragment) {
        try {
            Files.writeString(file, fragment, StandardCharsets.UTF_8,
                    StandardOpenOption.APPEND);
            log.info("[TORN] 模拟落盘中断：向 {} 追加残缺片段 '{}'（无换行、字段不全）",
                    file.getFileName(), fragment);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void torn_write_record_tail_is_discarded_and_service_recovers_to_last_committed(
            @TempDir Path dir) {
        SimulatedClock clock = new SimulatedClock();
        String res = "/res/torn-write";
        Path wal = walFile(dir, res);
        long sizeBeforeTorn;

        // ---- 第一个进程：两条写入都确认成功（已 fsync），随后在第三条写入落盘途中崩溃 ----
        try (LocalCluster first = new LocalCluster(dir, clock, 2)) {
            LogicalNode a = first.node(0);
            a.leaseService().acquire(res, a.nodeId(), 60_000);
            a.writeService().submit(res, a.nodeId(), 1L, "k-1", "v1");
            a.writeService().submit(res, a.nodeId(), 1L, "k-2", "v2");
            try {
                sizeBeforeTorn = Files.size(wal);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            log.info("[PROC-1] 已确认写入 seq=1,2 后进程崩溃，WAL 当前 {} 字节", sizeBeforeTorn);
        }
        // 崩溃时第三条记录只写了一半（缺字段、无换行）
        appendTornBytes(wal, "W|1|3|a2");

        // ---- 第二个进程：必须能起来，并回到最后一次完整提交 ----
        try (LocalCluster second = new LocalCluster(dir, clock, 2)) {
            LogicalNode a2 = second.node(0);

            // 残缺尾巴没被当成已生效写入：已提交序号仍是 2
            assertThat(a2.writeService().committedSequence(res)).isEqualTo(2L);

            // 已确认成功的写入不丢：重放仍命中首次结果
            WriteOutcome replay = a2.writeService().submit(res, a2.nodeId(), 1L, "k-2", "v2");
            assertThat(replay.replay()).isTrue();
            assertThat(replay.sequence()).isEqualTo(2L);

            // 序号不重复发号：崩溃时未确认的那条（seq=3）可以安全重试并拿到 3
            WriteOutcome w3 = a2.writeService().submit(res, a2.nodeId(), 1L, "k-3", "v3");
            assertThat(w3.accepted()).isTrue();
            assertThat(w3.sequence()).isEqualTo(3L);

            // 残缺尾部已被截断，新记录从干净位置继续追加
            try {
                assertThat(Files.size(wal)).isGreaterThan(sizeBeforeTorn);
                String content = Files.readString(wal);
                assertThat(content).doesNotContain("W|1|3|a2\n"); // 残缺片段不复存在
                assertThat(content.lines().filter(l -> l.startsWith("W|")).count()).isEqualTo(3);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            log.info("[PROC-2] 恢复完成：committedSeq=3，残缺尾部已截断，序号无重号");
        }

        // ---- 第三个进程：截断后的日志可再次干净重放 ----
        try (LocalCluster third = new LocalCluster(dir, clock, 2)) {
            LogicalNode a3 = third.node(0);
            assertThat(a3.writeService().committedSequence(res)).isEqualTo(3L);
            WriteOutcome replay3 = a3.writeService().submit(res, a3.nodeId(), 1L, "k-3", "v3");
            assertThat(replay3.replay()).isTrue();
            assertThat(replay3.sequence()).isEqualTo(3L);
        }
    }

    @Test
    void torn_lease_record_tail_does_not_prevent_startup_or_corrupt_lease_state(
            @TempDir Path dir) {
        SimulatedClock clock = new SimulatedClock();
        String res = "/res/torn-lease";
        Path wal = walFile(dir, res);

        // 授予 + 一次完整续约，随后第二次续约记录只落了一半就崩了
        try (LocalCluster first = new LocalCluster(dir, clock, 1)) {
            LogicalNode a = first.node(0);
            a.leaseService().acquire(res, a.nodeId(), 5_000);
            clock.advance(100);
            a.leaseService().renew(res, a.nodeId(), 5_000); // revision=2，完整落盘
            log.info("[PROC-1] 授予+续约(revision=2)完成，第二次续约落盘途中崩溃");
        }
        appendTornBytes(wal, "N|1|99999"); // 残缺的续约记录：缺 revision 字段、无换行

        try (LocalCluster second = new LocalCluster(dir, clock, 1)) {
            LogicalNode a2 = second.node(0);
            // 服务正常起来，租约回到最后一次完整提交的状态（revision=2 的那次续约）
            Lease restored = a2.leaseService().inspect(res).orElseThrow();
            assertThat(restored.revision()).isEqualTo(2);
            assertThat(restored.expiresAtMono()).isEqualTo(100 + 5_000);
            assertThat(restored.holderId()).isEqualTo(a2.nodeId());

            // 租约未过期，可继续正常续约（残缺尾巴不影响后续操作）
            Lease renewed = a2.leaseService().renew(res, a2.nodeId(), 5_000);
            assertThat(renewed.revision()).isEqualTo(3);
            log.info("[PROC-2] 残缺续约记录被截断，租约状态来自完整记录，可继续续约");
        }
    }
}
