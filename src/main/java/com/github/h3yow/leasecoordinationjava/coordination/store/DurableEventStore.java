package com.github.h3yow.leasecoordinationjava.coordination.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.leasecoordinationjava.coordination.event.CoordinationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * 追加式持久事件存储（JSON Lines 格式）。
 *
 * <p>持久性：{@link #append} 先写入完整的一行 JSON，再调用 {@code force(true)}
 * （对应 fsync）才返回。因此返回成功的事件一定落盘；只可能出现“最后一行写了一半”的情况。</p>
 *
 * <p>恢复规则：{@link #replay()} 逐行解析；遇到损坏/写了一半的末行（崩溃瞬间产生），
 * 将文件截断回上一条完整事件之后并忽略该残缺行——保证日志是“已提交事件”的严格前缀，
 * 不会出现重复授予或幽灵写入。</p>
 */
public final class DurableEventStore {

    private static final Logger log = LoggerFactory.getLogger(DurableEventStore.class);
    private static final String LOG_FILE_NAME = "coordination.log";

    private final Path logFile;
    private final ObjectMapper mapper;
    private final Object ioLock = new Object();

    public DurableEventStore(Path dataDir, String clusterName) {
        Path clusterDir = dataDir.resolve(clusterName);
        try {
            Files.createDirectories(clusterDir);
        } catch (IOException e) {
            throw new IllegalStateException("无法创建集群数据目录: " + clusterDir, e);
        }
        this.logFile = clusterDir.resolve(LOG_FILE_NAME);
        this.mapper = new ObjectMapper();
    }

    /** 持久化追加一条事件，整行写入并 fsync 后才返回。 */
    public void append(CoordinationEvent event) {
        byte[] line;
        try {
            line = (mapper.writeValueAsString(event) + System.lineSeparator())
                    .getBytes(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("事件序列化失败: " + event, e);
        }
        synchronized (ioLock) {
            try (FileChannel ch = FileChannel.open(logFile,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                ByteBuffer buf = ByteBuffer.wrap(line);
                while (buf.hasRemaining()) {
                    ch.write(buf);
                }
                ch.force(true); // fsync：返回后即使进程/机器崩溃事件也已持久化
            } catch (IOException e) {
                throw new IllegalStateException("追加持久事件失败: " + logFile, e);
            }
        }
        log.debug("事件已持久化: {}", event);
    }

    /** 按追加顺序读出全部已持久化事件；自动截断并丢弃崩溃留下的残缺末行。 */
    public List<CoordinationEvent> replay() {
        synchronized (ioLock) {
            if (!Files.exists(logFile)) {
                return List.of();
            }
            List<String> lines;
            try {
                lines = Files.readAllLines(logFile, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new IllegalStateException("读取事件日志失败: " + logFile, e);
            }

            List<CoordinationEvent> events = new ArrayList<>();
            long goodOffset = 0L;
            boolean corruptedTail = false;
            int lineSepLen = System.lineSeparator().getBytes(StandardCharsets.UTF_8).length;

            for (String rawLine : lines) {
                String line = rawLine.trim();
                long lineBytes = rawLine.getBytes(StandardCharsets.UTF_8).length + lineSepLen;
                if (line.isEmpty()) {
                    goodOffset += lineBytes;
                    continue;
                }
                try {
                    events.add(mapper.readValue(line, CoordinationEvent.class));
                    goodOffset += lineBytes;
                } catch (IOException parseFailure) {
                    // 只可能是崩溃瞬间写了一半的末行：截断到最后一个完整事件之后
                    log.warn("检测到损坏/不完整的日志行，将截断丢弃（offset={}）: {}", goodOffset, line);
                    corruptedTail = true;
                    break;
                }
            }

            if (corruptedTail) {
                truncateAt(goodOffset);
            }
            return List.copyOf(events);
        }
    }

    private void truncateAt(long offset) {
        try (FileChannel ch = FileChannel.open(logFile, StandardOpenOption.WRITE)) {
            ch.truncate(offset);
            ch.force(true);
        } catch (IOException e) {
            throw new IllegalStateException("截断损坏日志尾部失败: " + logFile, e);
        }
    }

    public Path getLogFile() {
        return logFile;
    }
}
