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
 *   <li><b>可恢复</b>：打开目录时（以及节点重启重新打开时）重放全部 WAL 重建内存态；
 *       落盘途中崩溃留下的不完整尾部记录会被截断丢弃，既不阻止启动也不算已生效写入。</li>
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
                        new String(b64.decode(p[7])), Long.parseLong(p[1]),
                        Long.parseLong(p[2]), Long.parseLong(p[3]),
                        Long.parseLong(p[4]), Integer.parseInt(p[5]),
                        Long.parseLong(p[6]));
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
     * 计划内交接：当前持有者把领导权交给指定继任者。
     * 与 {@link LeaseGrant} 一样占据租约槽并签发新令牌，
     * 同时记录被取代的旧令牌 {@code supersededToken} 作为交接边界。
     */
    public record LeaseHandover(String holderId, long fencingToken, long acquiredAtWall,
                                long acquiredAtMono, long expiresAtMono, int revision,
                                long supersededToken) implements WalRecord {
        @Override
        public String encode() {
            return "H|" + fencingToken + '|' + acquiredAtWall + '|' + acquiredAtMono + '|'
                    + expiresAtMono + '|' + revision + '|' + supersededToken + '|'
                    + Base64.getEncoder().encodeToString(holderId.getBytes(StandardCharsets.UTF_8));
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

    private static int replayInto(Path file, ResourceState s) {
        int n = 0;
        long goodOffset = 0; // 已确认完好的字节数（残缺尾部从这里截断）
        try (BufferedReader br = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                // 本资源日志由我们自己以 '\n' 追加，行字节数 + 1 即含换行的偏移
                long lineBytes = line.getBytes(StandardCharsets.UTF_8).length + 1L;
                if (line.isBlank()) {
                    goodOffset += lineBytes;
                    continue;
                }
                WalRecord record;
                try {
                    record = WalRecord.decode(line);
                } catch (RuntimeException e) {
                    // 落盘途中崩溃留下的不完整尾部：不是已生效写入，截断丢弃，
                    // 既不能阻止启动，也不能参与状态重建
                    log.warn("WAL 尾部残缺，截断并忽略 file={} 截断偏移={} 已重放={}条 "
                                    + "残缺内容='{}' 判定依据={}",
                            file, goodOffset, n, abbreviate(line), e);
                    truncateTo(file, goodOffset);
                    return n;
                }
                applyRecord(s, record);
                n++;
                goodOffset += lineBytes;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("重放 WAL 失败: " + file, e);
        }
        return n;
    }

    private static void truncateTo(Path file, long size) {
        try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(file,
                java.nio.file.StandardOpenOption.WRITE)) {
            ch.truncate(size);
            ch.force(true);
            log.info("WAL 残缺尾部已截断 file={} 保留字节={}", file, size);
        } catch (IOException e) {
            throw new UncheckedIOException("截断残缺 WAL 尾部失败: " + file, e);
        }
    }

    private static String abbreviate(String line) {
        return line.length() <= 64 ? line : line.substring(0, 64) + "…";
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
                s.holderId = h.holderId();
                s.fencingToken = h.fencingToken();
                s.acquiredAtWall = h.acquiredAtWall();
                s.acquiredAtMono = h.acquiredAtMono();
                s.expiresAtMono = h.expiresAtMono();
                s.released = false;
                s.revision = h.revision();
                s.handoverBoundaryToken =
                        Math.max(s.handoverBoundaryToken, h.supersededToken());
                trackToken(s, h.fencingToken());
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
