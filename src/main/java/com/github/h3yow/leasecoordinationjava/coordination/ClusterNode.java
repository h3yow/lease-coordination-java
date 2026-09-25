package com.github.h3yow.leasecoordinationjava.coordination;

import com.github.h3yow.leasecoordinationjava.coordination.lease.LeaseGrant;
import com.github.h3yow.leasecoordinationjava.coordination.write.WriteResult;

import java.util.Objects;

/**
 * 集群中的一个逻辑节点（进程内成员）。
 *
 * <p>节点不保存任何权威状态：租约与写入的判定、持久化全部由共享的 {@link CoordinationCluster}
 * 完成，凭证（{@link LeaseGrant}）由调用方持有。因此 {@link #crash()} 只是丢弃节点侧的
 * 瞬时引用；之后用相同 nodeId 重新构造节点即“重启”，只要凭证仍在 TTL 内就能继续续约/写入，
 * 凭证不在手上则必须等过期后重新获取——与真实分布式系统的故障模型一致。</p>
 */
public final class ClusterNode {

    private final String nodeId;
    private final CoordinationCluster cluster;
    private volatile boolean alive = true;

    public ClusterNode(String nodeId, CoordinationCluster cluster) {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId 不能为空");
        }
        this.nodeId = nodeId;
        this.cluster = Objects.requireNonNull(cluster, "cluster");
    }

    public String nodeId() {
        return nodeId;
    }

    /** 模拟节点崩溃：节点上的瞬时状态全部失效。 */
    public void crash() {
        alive = false;
    }

    private void ensureAlive() {
        if (!alive) {
            throw new IllegalStateException("节点 " + nodeId + " 已崩溃，必须先重启（重新加入集群）");
        }
    }

    public LeaseGrant acquireLease(String resourceId, long ttlMs) {
        ensureAlive();
        return cluster.acquireLease(nodeId, resourceId, ttlMs);
    }

    public LeaseGrant renewLease(LeaseGrant lease, long ttlMs) {
        ensureAlive();
        return cluster.renewLease(nodeId, lease, ttlMs);
    }

    public void releaseLease(LeaseGrant lease) {
        ensureAlive();
        cluster.releaseLease(nodeId, lease);
    }

    public WriteResult submitWrite(String resourceId, String requestId,
                                   long fencingToken, long payloadRef) {
        ensureAlive();
        return cluster.submitWrite(nodeId, resourceId, requestId, fencingToken, payloadRef);
    }
}
