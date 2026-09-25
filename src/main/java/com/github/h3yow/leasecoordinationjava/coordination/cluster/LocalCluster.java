package com.github.h3yow.leasecoordinationjava.coordination.cluster;

import com.github.h3yow.leasecoordinationjava.coordination.clock.SimulatedClock;
import com.github.h3yow.leasecoordinationjava.coordination.storage.ClusterStateStore;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 进程内本地集群：一个可控时钟 + 一块共享"持久磁盘"目录（一个共享状态库）+ N 个逻辑节点。
 * 节点的并发争抢、崩溃、重启都在同一 JVM 内真实发生（真实线程、真实文件 WAL）。
 *
 * <p>用同一 dataDirectory 先后构造两个 LocalCluster，即模拟"整个进程重启"：
 * 第二个集群打开目录时重放全部 WAL 恢复状态。</p>
 */
public class LocalCluster implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LocalCluster.class);

    private final SimulatedClock clock;
    private final Path dataDirectory;
    private final ClusterStateStore store;
    private final List<LogicalNode> nodes = new ArrayList<>();

    public LocalCluster(int nodeCount) {
        this(newSharedTempDir(), new SimulatedClock(), nodeCount);
    }

    /** 测试可指定固定数据目录与共享时钟：用于模拟进程重启后的 WAL 重放恢复。 */
    public LocalCluster(Path dataDirectory, SimulatedClock clock, int nodeCount) {
        this.dataDirectory = dataDirectory;
        this.clock = clock;
        this.store = new ClusterStateStore(dataDirectory);
        for (int i = 0; i < nodeCount; i++) {
            nodes.add(new LogicalNode("node-" + (i + 1), store, clock));
        }
        log.info("[CLUSTER] 本地集群装配完成 nodeCount={} dataDir={} wall={} mono={}",
                nodeCount, dataDirectory, clock.wallClockMillis(), clock.monotonicMillis());
    }

    private static Path newSharedTempDir() {
        try {
            return Files.createTempDirectory("lease-cluster-");
        } catch (IOException e) {
            throw new UncheckedIOException("创建临时数据目录失败", e);
        }
    }

    public SimulatedClock clock() {
        return clock;
    }

    public Path dataDirectory() {
        return dataDirectory;
    }

    public ClusterStateStore store() {
        return store;
    }

    public LogicalNode node(int index) {
        return nodes.get(index);
    }

    public int nodeCount() {
        return nodes.size();
    }

    @Override
    public void close() {
        for (LogicalNode n : nodes) {
            if (n.isUp()) {
                n.crash();
            }
        }
        store.close();
        log.info("[CLUSTER] 集群关闭，WAL 已同步落盘: {}", dataDirectory);
    }
}
