package com.github.h3yow.leasecoordinationjava.coordination.cluster;

import com.github.h3yow.leasecoordinationjava.coordination.clock.TimeSource;
import com.github.h3yow.leasecoordinationjava.coordination.lease.LeaseService;
import com.github.h3yow.leasecoordinationjava.coordination.storage.ClusterStateStore;
import com.github.h3yow.leasecoordinationjava.coordination.write.WriteService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 进程内的一个逻辑集群成员（共享持久化存储上的"客户端"）。
 *
 * <p>节点本身无权威状态：租约/令牌/写入台账都在共享的 {@link ClusterStateStore} 中
 * （进程内模拟共识存储）。因此：</p>
 * <ul>
 *   <li>{@link #crash()} 只丢弃节点内存中的服务句柄；已 fsync 的数据不丢，
 *       其他节点照常工作，同一资源不会因崩溃出现两个有效持有者；</li>
 *   <li>{@link #restart()} 重新挂接持久化存储，其视图立即恢复，
 *       {@code incarnation} 递增；</li>
 *   <li>进程级重启（所有内存都没了）由重新构造 {@code ClusterStateStore} 重放 WAL 恢复，
 *       见 {@code NodeCrashRecoveryTest}。</li>
 * </ul>
 */
public class LogicalNode {

    private static final Logger log = LoggerFactory.getLogger(LogicalNode.class);

    private final String nodeId;
    private final ClusterStateStore store;
    private final TimeSource clock;

    private LeaseService leaseService;
    private WriteService writeService;
    private boolean up;
    private long incarnation;

    public LogicalNode(String nodeId, ClusterStateStore store, TimeSource clock) {
        this.nodeId = nodeId;
        this.store = store;
        this.clock = clock;
        boot();
    }

    private void boot() {
        this.leaseService = new LeaseService(store, clock);
        this.writeService = new WriteService(store, clock);
        this.up = true;
        this.incarnation++;
        log.info("[NODE] 节点 {} 上线 incarnation={}，已挂接共享持久化存储",
                nodeId, incarnation);
    }

    public String nodeId() {
        return nodeId;
    }

    public long incarnation() {
        return incarnation;
    }

    public boolean isUp() {
        return up;
    }

    public LeaseService leaseService() {
        ensureUp();
        return leaseService;
    }

    public WriteService writeService() {
        ensureUp();
        return writeService;
    }

    /** 模拟崩溃：丢弃节点全部内存态；持久化数据与其他节点不受影响。 */
    public void crash() {
        log.warn("[NODE] 节点 {} 崩溃！incarnation={} 丢弃内存服务句柄（持久化数据保留）",
                nodeId, incarnation);
        this.leaseService = null;
        this.writeService = null;
        this.up = false;
    }

    /** 模拟重启：重新挂接共享持久化存储，节点视图恢复。 */
    public void restart() {
        if (up) {
            throw new IllegalStateException("节点 " + nodeId + " 未崩溃，无需重启");
        }
        log.info("[NODE] 节点 {} 重启中… 重新挂接持久化存储", nodeId);
        boot();
        log.info("[NODE] 节点 {} 重启完成 incarnation={}，租约/令牌/写入视图已恢复",
                nodeId, incarnation);
    }

    private void ensureUp() {
        if (!up) {
            throw new IllegalStateException("节点 " + nodeId + " 已崩溃，无法提供服务");
        }
    }
}
