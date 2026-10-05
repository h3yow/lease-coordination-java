package com.github.h3yow.leasecoordinationjava.coordination.storage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 集群共享的持久化状态库（进程内模拟"共享磁盘/共识日志"）。
 *
 * <p>保证：</p>
 * <ul>
 *   <li><b>线性化</b>：同一资源的所有 {@link #compute} 调用按每资源锁串行执行；</li>
 *   <li><b>持久性</b>：变更以 WAL 记录追加到 {@code <dataDir>/wal/<resource>.log}，
 *       追加后立即 {@code fsync}，返回成功即视为已落盘；</li>
 *   <li><b>可恢复</b>：打开目录时（以及节点重启重新打开时）重放全部 WAL 重建内存态。</li>
 * </ul>
 */
public class ClusterStateStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ClusterStateStore.class);

    // ===================== WAL 记录模型 =====================

    /** 一次原子变更产生的结果：返回值 value + 需按序落盘的 WAL 记录。 */
    public record Result<T>(T value, List<WalRecord> records) {
        public static <T> Result<T> of(T value) {
            return new Result<>(value, List.of());
        }

        public static <T> Result<T> of(T value, WalRecord record) {
            return new Result<>(value, List.of(record));
        }
    }

    /** WAL 记录：单资源日志，故记录内不再携带 resourceId。 */
    public sealed interface WalRecord
            permits LeaseGrant, LeaseRenew, LeaseRelease, LeaseHandover, WriteApplied {

        String encode();

        static WalRecord decode(String line) {
            String[] p = line.split("\\|", -1);
            Base64.Decoder b64 = Base64.getDecoder();
            return switch (p[0]) {
                case "G" -> new LeaseGrant(
                        new String(b64.decode(p[6])), Long.parseLong(p[1]),
                        Long.parseLong(p[2]), Long.parseLong(p[3]),
                        Long.parseLong(p[4]), Integer.parseInt(p[5]));
                case "N" -> new LeaseRenew(
                        Long.parseLong(p[1]), Long.parseLong(p[2]), Integer.parseInt(p[3]));
                case "R" -> new LeaseRelease(
                        Long.parseLong(p[1]), Integer.parseInt(p[2]));
                case "H" -> new LeaseHandover(
                        Long.parseLong(p[1]), Long.parseLong(p[2]),
                        Long.parseLong(p[3]), Long.parseLong(p[4]),
                        Long.parseLong(p[5]), Integer.parseInt(p[6]),
                        new String(b64.decode(p[7])));
                case "W" -> new WriteApplied(
                        Long.parseLong(p[1]), Long.parseLong(p[2]),
                        new String(b64.decode(p[3])), new String(b64.decode(p[4])));
                default -> throw new IllegalStateException("未知 WAL 记录: " + line);
            };
        }
    }

    /** 全新授予（或过期后重新获取）租约。 */
    public record LeaseGrant(String holderId, long fencingToken, long acquiredAtWall,
                             long acquiredAtMono, long expiresAtMono, int revision)
            implements WalRecord {
        @Override
        public String encode() {
            return "G|" + fencingToken + '|' + acquiredAtWall + '|' + acquiredAtMono + '|'
                    + expiresAtMono + '|' + revision + '|'
                    + Base64.getEncoder().encodeToString(holderId.getBytes(StandardCharsets.UTF_8));
        }
    }

    /** 续约成功：令牌不变，过期时刻与修订版本更新。 */
    public record LeaseRenew(long fencingToken, long expiresAtMono, int revision)
            implements WalRecord {
        @Override
        public String encode() {
            return "N|" + fencingToken + '|' + expiresAtMono + '|' + revision;
        }
    }

    /** 显式释放。 */
    public record LeaseRelease(long fencingToken, int revision) implements WalRecord {
        @Override
        public String encode() {
            return "R|" + fencingToken + '|' + revision;
        }
    }

    /**
     * 计划内换主：旧令牌被显式作废，继任者立即以严格更大的新令牌上任。
     * 重放时旧令牌进入 {@code handoverSupersededTokens}，其后的旧令牌提交一律拒绝。
     */
    public record LeaseHandover(long oldToken, long newToken, long acquiredAtWall,
                                long acquiredAtMono, long expiresAtMono, int revision,
                                String successorId) implements WalRecord {
        @Override
        public String encode() {
            return "H|" + oldToken + '|' + newToken + '|' + acquiredAtWall + '|'
                    + acquiredAtMono + '|' + expiresAtMono + '|' + revision + '|'
                    + Base64.getEncoder().encodeToString(
                            successorId.getBytes(StandardCharsets.UTF_8));
        }
    }

    /** 一次真正产生副作用的写入。 */
    public record WriteApplied(long fencingToken, long sequence, String idempotencyKey,
                               String payload) implements WalRecord {
        @Override
        public String encode() {
            return "W|" + fencingToken + '|' + sequence + '|'
                    + Base64.getEncoder().encodeToString(
                            idempotencyKey.getBytes(StandardCharsets.UTF_8)) + '|'
                    + Base64.getEncoder().encodeToString(
                            payload.getBytes(StandardCharsets.UTF_8));
        }
    }

    // ===================== 存储实现 =====================

    private final Path dataDir;
    private final Path walDir;
    private final ConcurrentHashMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ResourceState> states = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, java.io.FileOutputStream> streams =
            new ConcurrentHashMap<>();

    public ClusterStateStore(Path dataDirectory) {
        try {
            this.dataDir = dataDirectory;
            this.walDir = dataDir.resolve("wal");
            Files.createDirectories(walDir);
            recover();
        } catch (IOException e) {
            throw new UncheckedIOException("初始化状态库失败: " + dataDirectory, e);
        }
    }

    /**
     * 以线性化方式对单个资源状态做一次原子读改写。
     * mutation 返回 {@link Result}；其 WAL 记录在临界区内、返回前顺序追加并 fsync。
     * mutation 抛异常则不写入任何记录，状态保持不变。
     */
    public <T> T compute(String resourceId, Function<ResourceState, Result<T>> mutation) {
        ReentrantLock lock = locks.computeIfAbsent(resourceId, k -> new ReentrantLock());
        lock.lock();
        try {
            ResourceState state = states.computeIfAbsent(resourceId, k -> new ResourceState());
            Result<T> result = mutation.apply(state);
            if (!result.records().isEmpty()) {
                appendRecords(resourceId, result.records());
            }
            return result.value();
        } finally {
            lock.unlock();
        }
    }

    private void appendRecords(String resourceId, List<WalRecord> records) {
        try {
            java.io.FileOutputStream out = streams.computeIfAbsent(resourceId, this::openLog);
            for (WalRecord r : records) {
                out.write(r.encode().getBytes(StandardCharsets.UTF_8));
                out.write('\n');
            }
            out.flush();
            out.getFD().sync(); // 模拟 fsync：返回成功即持久化
        } catch (IOException e) {
            throw new UncheckedIOException("WAL 追加失败: " + resourceId, e);
        }
    }

    private java.io.FileOutputStream openLog(String resourceId) {
        try {
            return new java.io.FileOutputStream(logFile(resourceId).toFile(), true);
        } catch (IOException e) {
            throw new UncheckedIOException("打开 WAL 失败: " + resourceId, e);
        }
    }

    private Path logFile(String resourceId) {
        String name = URLEncoder.encode(resourceId, StandardCharsets.UTF_8).replace("+", "%20");
        return walDir.resolve(name + ".log");
    }

    /** 重放 wal 目录下全部日志，重建每个资源的内存态。 */
    public void recover() {
        List<Path> files;
        try (var paths = Files.list(walDir)) {
            files = paths.filter(Files::isRegularFile).sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException("扫描 WAL 目录失败", e);
        }
        for (Path f : files) {
            String encoded = f.getFileName().toString();
            encoded = encoded.substring(0, encoded.length() - ".log".length());
            String resourceId = URLDecoder.decode(encoded, StandardCharsets.UTF_8);
            ResourceState state = new ResourceState();
            int count = replayInto(f, state);
            states.put(resourceId, state);
            log.info("WAL 重放完成 resource={} file={} 记录数={} lastToken={} lastSeq={}",
                    resourceId, f.getFileName(), count,
                    state.nextToken - 1, state.lastSequence);
        }
    }

    /**
     * 重放单个日志文件。追加是顺序的，损坏只可能出现在尾部（落盘到一半进程崩溃），
     * 因此遇到第一条无法解码的记录即停止重放该文件：残缺尾巴既不阻止启动，
     * 也不被当作已生效写入；之前的完整记录全部保留，序号从恢复点继续发放。
     */
    private static int replayInto(Path file, ResourceState s) {
        int n = 0;
        try (BufferedReader br = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                WalRecord record;
                try {
                    record = WalRecord.decode(line);
                } catch (RuntimeException corrupt) {
                    log.warn("[WAL-截断] file={} 第{}条记录无法解码（落盘到一半崩溃的残缺尾巴），"
                                    + "停止重放该文件，已恢复{}条完整记录。坏行内容={}",
                            file.getFileName(), n + 1, n, abbreviate(line));
                    break;
                }
                applyRecord(s, record);
                n++;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("重放 WAL 失败: " + file, e);
        }
        return n;
    }

    private static String abbreviate(String line) {
        return line.length() <= 80 ? line : line.substring(0, 80) + "…";
    }

    private static void applyRecord(ResourceState s, WalRecord r) {
        switch (r) {
            case LeaseGrant g -> {
                s.hasLease = true;
                s.holderId = g.holderId();
                s.fencingToken = g.fencingToken();
                s.acquiredAtWall = g.acquiredAtWall();
                s.acquiredAtMono = g.acquiredAtMono();
                s.expiresAtMono = g.expiresAtMono();
                s.released = false;
                s.revision = g.revision();
                trackToken(s, g.fencingToken());
            }
            case LeaseRenew n -> {
                if (s.hasLease && s.fencingToken == n.fencingToken()) {
                    s.expiresAtMono = n.expiresAtMono();
                    s.revision = n.revision();
                }
            }
            case LeaseRelease rel -> {
                if (s.hasLease && s.fencingToken == rel.fencingToken()) {
                    s.released = true;
                    s.revision = rel.revision();
                }
            }
            case LeaseHandover h -> {
                s.hasLease = true;
                s.holderId = h.successorId();
                s.fencingToken = h.newToken();
                s.acquiredAtWall = h.acquiredAtWall();
                s.acquiredAtMono = h.acquiredAtMono();
                s.expiresAtMono = h.expiresAtMono();
                s.released = false;
                s.revision = h.revision();
                s.handoverSupersededTokens.add(h.oldToken());
                trackToken(s, h.newToken());
            }
            case WriteApplied w -> {
                trackToken(s, w.fencingToken());
                s.lastSequence = Math.max(s.lastSequence, w.sequence());
                s.appliedWrites.put(w.idempotencyKey(),
                        new ResourceState.AppliedWrite(w.fencingToken(), w.sequence(),
                                w.payload()));
            }
        }
    }

    private static void trackToken(ResourceState s, long token) {
        s.issuedTokens.add(token);
        s.nextToken = Math.max(s.nextToken, token + 1);
    }

    @Override
    public void close() {
        List<IOException> errors = new ArrayList<>();
        for (java.io.FileOutputStream out : streams.values()) {
            try {
                out.getFD().sync();
                out.close();
            } catch (IOException e) {
                errors.add(e);
            }
        }
        if (!errors.isEmpty()) {
            throw new UncheckedIOException("关闭状态库失败", errors.get(0));
        }
    }
}
