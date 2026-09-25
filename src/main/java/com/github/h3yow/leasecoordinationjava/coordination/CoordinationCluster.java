package com.github.h3yow.leasecoordinationjava.coordination;

import com.github.h3yow.leasecoordinationjava.coordination.clock.LogicalClock;
import com.github.h3yow.leasecoordinationjava.coordination.event.CoordinationEvent;
import com.github.h3yow.leasecoordinationjava.coordination.lease.CoordinationException;
import com.github.h3yow.leasecoordinationjava.coordination.lease.LeaseGrant;
import com.github.h3yow.leasecoordinationjava.coordination.lease.RejectReason;
import com.github.h3yow.leasecoordinationjava.coordination.store.DurableEventStore;
import com.github.h3yow.leasecoordinationjava.coordination.write.WriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 进程内模拟的本地集群协调核心。
 *
 * <p><b>状态模型</b>：集群的全部权威状态是一条 fsync 追加的事件日志（{@link DurableEventStore}）。
 * 内存中的每个资源状态都由日志重放得到，且每次状态变更都在同一把锁内“判定 → 生成事件 →
 * 持久化 → 更新内存”原子完成，因此并发下对同一资源的判定串行化，任何时刻最多只有一个
 * 未过期持有者。</p>
 *
 * <p><b>租约</b>：{@code acquire / renew / release}；过期租约不可续约、释放幂等拒绝原因可区分。</p>
 * <p><b>有序写入</b>：每次授予租约分配全局单调递增的 fencing 令牌；资源上记录已提交的最高
 * 令牌，旧领导（低令牌）或过期领导（租约失效）的写入被拒绝；requestId 去重保证重放无副作用。</p>
 */
public final class CoordinationCluster {

    private static final Logger log = LoggerFactory.getLogger(CoordinationCluster.class);

    private final String clusterName;
    private final DurableEventStore store;
    private final LogicalClock clock;
    private final Object lock = new Object();

    /** resourceId -> 资源当前状态 */
    private final Map<String, ResourceState> resources = new HashMap<>();

    /** 集群内单调递增的令牌/版本计数，仅由事件重放恢复，从 0 开始。 */
    private long tokenCounter;

    private record ResourceState(
            LeaseGrant current,
            boolean released,
            long highestAppliedToken,
            Map<String, WriteResult> resultsByRequestId
    ) {
        ResourceState withCurrent(LeaseGrant grant, boolean released) {
            return new ResourceState(grant, released, highestAppliedToken, resultsByRequestId);
        }
        ResourceState withWrite(long token, WriteResult result) {
            Map<String, WriteResult> copy = new LinkedHashMap<>(resultsByRequestId);
            copy.put(result.requestId(), result);
            return new ResourceState(current, released, Math.max(token, highestAppliedToken), copy);
        }
    }

    public CoordinationCluster(String clusterName, Path dataDir, LogicalClock clock) {
        this.clusterName = Objects.requireNonNull(clusterName, "clusterName");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.store = new DurableEventStore(dataDir, clusterName);
        synchronized (lock) {
            recover();
        }
    }

    // ------------------------------------------------------------------
    // 租约操作
    // ------------------------------------------------------------------

    /**
     * 获取租约。若自己已是当前未过期持有者，则等价于一次续约（沿用同一令牌，顺延过期时间）。
     * 资源上存在他人未过期租约时拒绝；当前租约已过期（无论是否释放）时可被任何人重新获取。
     */
    public LeaseGrant acquireLease(String nodeId, String resourceId, long ttlMs) {
        requireText(nodeId, "nodeId");
        requireText(resourceId, "resourceId");
        if (ttlMs <= 0) {
            throw new CoordinationException(RejectReason.ILLEGAL_ARGUMENT,
                    "ttlMs 必须为正数，实际: " + ttlMs);
        }
        long now = clock.nowMillis();
        synchronized (lock) {
            ResourceState st = resources.get(resourceId);
            LeaseGrant current = st == null ? null : st.current();

            if (current != null && current.isValidAt(now) && !st.released()) {
                if (!current.holderId().equals(nodeId)) {
                    throw rejected(RejectReason.LEASE_HELD_BY_OTHER,
                            "资源 %s 在 t=%d 仍被 %s 持有（令牌=%d，过期时刻=%d），%s 不可抢占",
                            resourceId, now, current.holderId(), current.fencingToken(),
                            current.expireAtMs(), nodeId);
                }
                // 持有者重复获取：幂等续约
                long newExpire = now + ttlMs;
                if (newExpire <= current.expireAtMs()) {
                    log.info("[{}] acquire(续约保留) node={} resource={} token={} now={} expire={}",
                            clusterName, nodeId, resourceId, current.fencingToken(), now, current.expireAtMs());
                    return current;
                }
                LeaseGrant renewed = new LeaseGrant(resourceId, nodeId, current.fencingToken(),
                        current.acquiredAtMs(), newExpire, current.version());
                persist(new CoordinationEvent.LeaseRenewed(resourceId, nodeId,
                        current.fencingToken(), now, newExpire));
                resources.put(resourceId, st.withCurrent(renewed, false));
                log.info("[{}] acquire(持有者续约) node={} resource={} token={} now={} newExpire={}",
                        clusterName, nodeId, resourceId, renewed.fencingToken(), now, newExpire);
                return renewed;
            }

            long token = ++tokenCounter;
            long version = (current == null ? 0L : current.version()) + 1L;
            LeaseGrant grant = new LeaseGrant(resourceId, nodeId, token, now, now + ttlMs, version);
            persist(new CoordinationEvent.LeaseAcquired(resourceId, nodeId, token,
                    now, now + ttlMs, version));
            ResourceState base = st == null
                    ? new ResourceState(null, false, st == null ? 0L : st.highestAppliedToken(),
                        st == null ? new LinkedHashMap<>() : st.resultsByRequestId())
                    : st;
            resources.put(resourceId, base.withCurrent(grant, false));
            log.info("[{}] acquire(新授予) node={} resource={} token={} version={} now={} expire={}",
                    clusterName, nodeId, resourceId, token, version, now, now + ttlMs);
            return grant;
        }
    }

    /**
     * 续约。拒绝原因可区分：
     * 非持有者 → {@link RejectReason#LEASE_NOT_OWNER}；
     * 已主动释放 → {@link RejectReason#LEASE_ALREADY_RELEASED}；
     * 已过期   → {@link RejectReason#LEASE_EXPIRED}。
     */
    public LeaseGrant renewLease(String nodeId, LeaseGrant lease, long ttlMs) {
        requireText(nodeId, "nodeId");
        Objects.requireNonNull(lease, "lease");
        if (ttlMs <= 0) {
            throw new CoordinationException(RejectReason.ILLEGAL_ARGUMENT,
                    "ttlMs 必须为正数，实际: " + ttlMs);
        }
        long now = clock.nowMillis();
        synchronized (lock) {
            ResourceState st = resources.get(lease.resourceId());
            LeaseGrant current = st == null ? null : st.current();
            checkOwnership(nodeId, lease, current, st != null && st.released(), now, "renew");

            long newExpire = now + ttlMs;
            LeaseGrant renewed = new LeaseGrant(lease.resourceId(), nodeId, lease.fencingToken(),
                    current.acquiredAtMs(), Math.max(newExpire, current.expireAtMs()),
                    current.version());
            persist(new CoordinationEvent.LeaseRenewed(lease.resourceId(), nodeId,
                    lease.fencingToken(), now, renewed.expireAtMs()));
            resources.put(lease.resourceId(), st.withCurrent(renewed, false));
            log.info("[{}] renew 通过 node={} resource={} token={} now={} newExpire={}",
                    clusterName, nodeId, lease.resourceId(), lease.fencingToken(), now,
                    renewed.expireAtMs());
            return renewed;
        }
    }

    /** 主动释放。重复释放以 {@link RejectReason#LEASE_ALREADY_RELEASED} 拒绝；越权以 NOT_OWNER 拒绝。 */
    public void releaseLease(String nodeId, LeaseGrant lease) {
        requireText(nodeId, "nodeId");
        Objects.requireNonNull(lease, "lease");
        long now = clock.nowMillis();
        synchronized (lock) {
            ResourceState st = resources.get(lease.resourceId());
            LeaseGrant current = st == null ? null : st.current();
            if (st != null && st.released()
                    && current != null && current.fencingToken() == lease.fencingToken()) {
                throw rejected(RejectReason.LEASE_ALREADY_RELEASED,
                        "重复释放被拒绝: resource=%s node=%s token=%d now=%d（该租约已释放）",
                        lease.resourceId(), nodeId, lease.fencingToken(), now);
            }
            checkOwnership(nodeId, lease, current, st != null && st.released(), now, "release");
            persist(new CoordinationEvent.LeaseReleased(lease.resourceId(), nodeId,
                    lease.fencingToken(), now));
            resources.put(lease.resourceId(), st.withCurrent(current, true));
            log.info("[{}] release 通过 node={} resource={} token={} now={}",
                    clusterName, nodeId, lease.resourceId(), lease.fencingToken(), now);
        }
    }

    // ------------------------------------------------------------------
    // 有序写入（fencing）
    // ------------------------------------------------------------------

    public WriteResult submitWrite(String nodeId,
                                   String resourceId,
                                   String requestId,
                                   long fencingToken,
                                   long payloadRef) {
        requireText(nodeId, "nodeId");
        requireText(resourceId, "resourceId");
        requireText(requestId, "requestId");
        long now = clock.nowMillis();
        synchronized (lock) {
            ResourceState st = resources.get(resourceId);
            LeaseGrant current = st == null ? null : st.current();

            // 1) 令牌必须来自真实授予
            if (fencingToken <= 0 || fencingToken > tokenCounter) {
                throw rejected(RejectReason.FENCING_TOKEN_UNKNOWN,
                        "写入拒绝: resource=%s token=%d 不存在任何对应授予(最大令牌=%d) now=%d",
                        resourceId, fencingToken, tokenCounter, now);
            }

            // 2) 令牌必须仍是资源的当前领导租约
            if (current == null || current.fencingToken() != fencingToken) {
                long currentToken = current == null ? -1L : current.fencingToken();
                throw rejected(RejectReason.FENCING_TOKEN_STALE,
                        "写入拒绝(旧领导/旧令牌): resource=%s token=%d 当前令牌=%d now=%d",
                        resourceId, fencingToken, currentToken, now);
            }
            if (!current.holderId().equals(nodeId)) {
                throw rejected(RejectReason.LEASE_NOT_OWNER,
                        "写入拒绝(越权): resource=%s node=%s 非令牌持有者 %s token=%d",
                        resourceId, nodeId, current.holderId(), fencingToken);
            }

            // 3) 领导租约必须未过期（含已释放的情况）
            boolean expired = st.released() || !current.isValidAt(now);
            if (expired) {
                throw rejected(RejectReason.FENCING_LEASE_EXPIRED,
                        "写入拒绝(领导权已失效): resource=%s token=%d expire=%d released=%s now=%d",
                        resourceId, fencingToken, current.expireAtMs(), st.released(), now);
            }

            // 4) 幂等：只有在领导权上下文（持有者/令牌/有效期）仍然成立时才承认重放。
            //    旧领导或过期领导拿旧 requestId 重放，会在上面的栅栏校验处被拒绝，
            //    不可能借“重放”之名被静默接受。
            WriteResult cached = st.resultsByRequestId().get(requestId);
            if (cached != null) {
                if (cached.fencingToken() != fencingToken) {
                    throw rejected(RejectReason.ILLEGAL_ARGUMENT,
                            "requestId=%s 重放时令牌不一致: 首次=%d 本次=%d",
                            requestId, cached.fencingToken(), fencingToken);
                }
                log.info("[{}] write(重放命中,无副作用) node={} resource={} requestId={} token={} now={}",
                        clusterName, nodeId, resourceId, requestId, fencingToken, now);
                // 返回带 replay 标记的视图；缓存中仍保存首次结果（committedAt 不变、不新增事件）
                return new WriteResult(cached.resourceId(), cached.requestId(),
                        cached.fencingToken(), cached.committedAtMs(), true);
            }

            // 5) 栅栏：令牌必须不小于资源上已提交过的最高令牌
            if (fencingToken < st.highestAppliedToken()) {
                throw rejected(RejectReason.FENCING_TOKEN_STALE,
                        "写入拒绝(栅栏): resource=%s token=%d 已提交最高令牌=%d now=%d",
                        resourceId, fencingToken, st.highestAppliedToken(), now);
            }

            WriteResult result = new WriteResult(resourceId, requestId, fencingToken, now, false);
            persist(new CoordinationEvent.WriteCommitted(resourceId, nodeId, requestId,
                    fencingToken, now, payloadRef));
            resources.put(resourceId, st.withWrite(fencingToken, result));
            log.info("[{}] write(提交成功) node={} resource={} requestId={} token={} now={} payload={}",
                    clusterName, nodeId, resourceId, requestId, fencingToken, now, payloadRef);
            return result;
        }
    }

    public long nowMs() {
        return clock.nowMillis();
    }

    public String clusterName() {
        return clusterName;
    }

    // ------------------------------------------------------------------
    // 内部：判定辅助 / 持久化 / 恢复
    // ------------------------------------------------------------------

    private void checkOwnership(String nodeId, LeaseGrant lease, LeaseGrant current,
                                boolean released, long now, String op) {
        String resourceId = lease.resourceId();
        if (current == null || current.fencingToken() != lease.fencingToken()) {
            throw rejected(RejectReason.LEASE_NOT_OWNER,
                    "%s 拒绝(越权/未知租约): resource=%s node=%s token=%d now=%d",
                    op, resourceId, nodeId, lease.fencingToken(), now);
        }
        if (!current.holderId().equals(nodeId) || !lease.holderId().equals(nodeId)) {
            throw rejected(RejectReason.LEASE_NOT_OWNER,
                    "%s 拒绝(持有者不匹配): resource=%s 请求方=%s 实际持有者=%s token=%d now=%d",
                    op, resourceId, nodeId, current.holderId(), lease.fencingToken(), now);
        }
        if (released) {
            throw rejected(RejectReason.LEASE_ALREADY_RELEASED,
                    "%s 拒绝(租约已释放): resource=%s node=%s token=%d now=%d",
                    op, resourceId, nodeId, lease.fencingToken(), now);
        }
        if (!current.isValidAt(now)) {
            throw rejected(RejectReason.LEASE_EXPIRED,
                    "%s 拒绝(租约已过期): resource=%s node=%s token=%d expire=%d now=%d",
                    op, resourceId, nodeId, lease.fencingToken(), current.expireAtMs(), now);
        }
    }

    private void persist(CoordinationEvent event) {
        store.append(event); // 调用点已持锁；落盘成功才返回，之后才更新内存
    }

    private void recover() {
        long t0 = clock.nowMillis();
        var events = store.replay();
        for (CoordinationEvent e : events) {
            apply(e);
        }
        log.info("[{}] 恢复完成: 重放事件 {} 条, 资源 {} 个, 最大令牌={}, now={}",
                clusterName, events.size(), resources.size(), tokenCounter, t0);
    }

    /** 纯函数式地把事件应用到内存状态（恢复与正常路径共用）。 */
    private void apply(CoordinationEvent e) {
        switch (e) {
            case CoordinationEvent.LeaseAcquired a -> {
                tokenCounter = Math.max(tokenCounter, a.fencingToken());
                ResourceState existing = resources.get(a.resourceId());
                Map<String, WriteResult> dedup = existing == null
                        ? new LinkedHashMap<>() : existing.resultsByRequestId();
                long high = existing == null ? 0L : existing.highestAppliedToken();
                LeaseGrant grant = new LeaseGrant(a.resourceId(), a.holderId(), a.fencingToken(),
                        a.acquiredAtMs(), a.expireAtMs(), a.version());
                resources.put(a.resourceId(),
                        new ResourceState(grant, false, high, dedup));
            }
            case CoordinationEvent.LeaseRenewed r -> {
                ResourceState st = mustHave(r.resourceId());
                LeaseGrant c = st.current();
                LeaseGrant grant = new LeaseGrant(r.resourceId(), r.holderId(), r.fencingToken(),
                        c.acquiredAtMs(), r.newExpireAtMs(), c.version());
                resources.put(r.resourceId(), st.withCurrent(grant, false));
            }
            case CoordinationEvent.LeaseReleased r -> {
                ResourceState st = mustHave(r.resourceId());
                resources.put(r.resourceId(), st.withCurrent(st.current(), true));
            }
            case CoordinationEvent.WriteCommitted w -> {
                tokenCounter = Math.max(tokenCounter, w.fencingToken());
                ResourceState st = mustHave(w.resourceId());
                WriteResult result = new WriteResult(w.resourceId(), w.requestId(),
                        w.fencingToken(), w.committedAtMs(), false);
                resources.put(w.resourceId(), st.withWrite(w.fencingToken(), result));
            }
        }
    }

    private ResourceState mustHave(String resourceId) {
        ResourceState st = resources.get(resourceId);
        if (st == null || st.current() == null) {
            throw new IllegalStateException("日志损坏: 找不到资源 " + resourceId + " 的租约状态");
        }
        return st;
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new CoordinationException(RejectReason.ILLEGAL_ARGUMENT, name + " 不能为空");
        }
    }

    private static CoordinationException rejected(RejectReason reason, String fmt, Object... args) {
        return new CoordinationException(reason, String.format(fmt, args));
    }
}
