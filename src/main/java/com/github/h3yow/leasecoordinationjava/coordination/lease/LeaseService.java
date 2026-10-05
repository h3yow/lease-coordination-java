package com.github.h3yow.leasecoordinationjava.coordination.lease;

import com.github.h3yow.leasecoordinationjava.coordination.clock.TimeSource;
import com.github.h3yow.leasecoordinationjava.coordination.storage.ClusterStateStore;
import com.github.h3yow.leasecoordinationjava.coordination.storage.ResourceState;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 租约协调服务。
 *
 * <p>判定规则（所有"是否过期"判断只用单调时钟，与墙上时钟无关）：</p>
 * <ul>
 *   <li><b>acquire</b>：存在他人有效租约 → {@link LeaseRejectReason#HELD_BY_OTHER}；
 *       自己仍持有有效租约时重复获取 → 幂等返回当前租约（不签发新令牌）；
 *       无租约/已过期/已释放 → 以新栅栏令牌全新授予；</li>
 *   <li><b>renew</b>：未知 → {@code UNKNOWN_LEASE}；越权 → {@code NOT_HOLDER}；
 *       已释放 → {@code ALREADY_RELEASED}；已过期 → {@code EXPIRED}；
 *       成功则令牌不变、过期时刻顺延、revision+1；</li>
 *   <li><b>release</b>：拒绝原因与 renew 对应；成功后他人方可重新获取。</li>
 * </ul>
 */
public class LeaseService {

    private static final Logger log = LoggerFactory.getLogger(LeaseService.class);

    private final ClusterStateStore store;
    private final TimeSource clock;

    public LeaseService(ClusterStateStore store, TimeSource clock) {
        this.store = store;
        this.clock = clock;
    }

    /**
     * 获取租约。
     *
     * @param ttlMillis 租约有效期（从当前单调时刻起）
     * @return 授予的租约，或当前持有者幂等重复获取时返回既有租约
     * @throws CoordinationException 资源被其他有效持有者持有时抛 HELD_BY_OTHER
     */
    public Lease acquire(String resourceId, String holderId, long ttlMillis) {
        if (ttlMillis <= 0) {
            throw new IllegalArgumentException("ttlMillis 必须为正数: " + ttlMillis);
        }
        long nowMono = clock.monotonicMillis();
        long nowWall = clock.wallClockMillis();

        Lease lease = store.compute(resourceId, s -> {
            if (s.hasValidLease(nowMono)) {
                if (holderId.equals(s.holderId)) {
                    log.info("[ACQUIRE-幂等] resource={} node={} 仍是有效持有者 token={} "
                            + "expiresAtMono={} nowMono={} 剩余={}ms → 返回既有租约",
                            resourceId, holderId, s.fencingToken, s.expiresAtMono, nowMono,
                            s.expiresAtMono - nowMono);
                    return ClusterStateStore.Result.of(s.snapshot(resourceId));
                }
                log.info("[ACQUIRE-拒绝] resource={} node={} 持有者={} token={} 未过期 "
                        + "(expiresAtMono={} > nowMono={}, 剩余={}ms) → HELD_BY_OTHER",
                        resourceId, holderId, s.holderId, s.fencingToken,
                        s.expiresAtMono, nowMono, s.expiresAtMono - nowMono);
                throw new CoordinationException(LeaseRejectReason.HELD_BY_OTHER,
                        "资源 " + resourceId + " 正被 " + s.holderId + " 有效持有，不可抢占");
            }

            long token = s.nextToken;
            s.nextToken++;
            s.hasLease = true;
            s.holderId = holderId;
            s.fencingToken = token;
            s.acquiredAtWall = nowWall;
            s.acquiredAtMono = nowMono;
            s.expiresAtMono = nowMono + ttlMillis;
            s.released = false;
            s.revision = 1;
            s.issuedTokens.add(token);

            log.info("[ACQUIRE-授予] resource={} node={} token={} wall={} mono={} ttl={}ms "
                            + "expiresAtMono={} priorState={} → 全新授予并WAL落盘",
                    resourceId, holderId, token, nowWall, nowMono, ttlMillis,
                    nowMono + ttlMillis, describePrior(s));
            return ClusterStateStore.Result.of(s.snapshot(resourceId),
                    new ClusterStateStore.LeaseGrant(holderId, token, nowWall, nowMono,
                            nowMono + ttlMillis, 1));
        });
        return lease;
    }

    /** 续约：令牌不变，过期时刻顺延。越权/已释放/已过期一律拒绝。 */
    public Lease renew(String resourceId, String holderId, long ttlMillis) {
        if (ttlMillis <= 0) {
            throw new IllegalArgumentException("ttlMillis 必须为正数: " + ttlMillis);
        }
        long nowMono = clock.monotonicMillis();

        return store.compute(resourceId, s -> {
            if (!s.hasLease) {
                log.info("[RENEW-拒绝] resource={} node={} nowMono={} → UNKNOWN_LEASE",
                        resourceId, holderId, nowMono);
                throw new CoordinationException(LeaseRejectReason.UNKNOWN_LEASE,
                        "资源 " + resourceId + " 上不存在租约");
            }
            if (!holderId.equals(s.holderId)) {
                log.info("[RENEW-拒绝] resource={} node={} 实际持有者={} token={} → NOT_HOLDER",
                        resourceId, holderId, s.holderId, s.fencingToken);
                throw new CoordinationException(LeaseRejectReason.NOT_HOLDER,
                        "节点 " + holderId + " 不是资源 " + resourceId + " 的持有者");
            }
            if (s.released) {
                log.info("[RENEW-拒绝] resource={} node={} token={} → ALREADY_RELEASED",
                        resourceId, holderId, s.fencingToken);
                throw new CoordinationException(LeaseRejectReason.ALREADY_RELEASED,
                        "租约 token=" + s.fencingToken + " 已被释放");
            }
            if (s.expiresAtMono <= nowMono) {
                log.info("[RENEW-拒绝] resource={} node={} token={} 已过期 "
                                + "(expiresAtMono={} <= nowMono={}, 超期={}ms) → EXPIRED",
                        resourceId, holderId, s.fencingToken, s.expiresAtMono, nowMono,
                        nowMono - s.expiresAtMono);
                throw new CoordinationException(LeaseRejectReason.EXPIRED,
                        "租约 token=" + s.fencingToken + " 已过期，不可续约");
            }

            int oldRevision = s.revision;
            s.expiresAtMono = nowMono + ttlMillis;
            s.revision = oldRevision + 1;
            log.info("[RENEW-成功] resource={} node={} token={} revision={}→{} nowMono={} "
                            + "ttl={}ms 新expiresAtMono={} → WAL落盘",
                    resourceId, holderId, s.fencingToken, oldRevision, s.revision, nowMono,
                    ttlMillis, s.expiresAtMono);
            return ClusterStateStore.Result.of(s.snapshot(resourceId),
                    new ClusterStateStore.LeaseRenew(s.fencingToken, s.expiresAtMono,
                            s.revision));
        });
    }

    /** 显式释放。重复释放/越权/释放已失效租约均拒绝并给出原因。 */
    public void release(String resourceId, String holderId) {
        long nowMono = clock.monotonicMillis();

        store.compute(resourceId, s -> {
            if (!s.hasLease) {
                log.info("[RELEASE-拒绝] resource={} node={} nowMono={} → UNKNOWN_LEASE",
                        resourceId, holderId, nowMono);
                throw new CoordinationException(LeaseRejectReason.UNKNOWN_LEASE,
                        "资源 " + resourceId + " 上不存在租约");
            }
            if (!holderId.equals(s.holderId)) {
                log.info("[RELEASE-拒绝] resource={} node={} 实际持有者={} token={} → NOT_HOLDER",
                        resourceId, holderId, s.holderId, s.fencingToken);
                throw new CoordinationException(LeaseRejectReason.NOT_HOLDER,
                        "节点 " + holderId + " 不是资源 " + resourceId + " 的持有者");
            }
            if (s.released) {
                log.info("[RELEASE-拒绝] resource={} node={} token={} → ALREADY_RELEASED "
                                + "(重复释放)",
                        resourceId, holderId, s.fencingToken);
                throw new CoordinationException(LeaseRejectReason.ALREADY_RELEASED,
                        "租约 token=" + s.fencingToken + " 已释放，禁止重复释放");
            }
            if (s.expiresAtMono <= nowMono) {
                log.info("[RELEASE-拒绝] resource={} node={} token={} 已过期 "
                                + "(expiresAtMono={} <= nowMono={}) → EXPIRED",
                        resourceId, holderId, s.fencingToken, s.expiresAtMono, nowMono);
                throw new CoordinationException(LeaseRejectReason.EXPIRED,
                        "租约 token=" + s.fencingToken + " 已失效，无需/禁止释放");
            }

            s.released = true;
            s.revision++;
            log.info("[RELEASE-成功] resource={} node={} token={} revision={} nowMono={} "
                            + "剩余={}ms → 资源可被重新获取，WAL落盘",
                    resourceId, holderId, s.fencingToken, s.revision, nowMono,
                    s.expiresAtMono - nowMono);
            return ClusterStateStore.Result.of(null,
                    new ClusterStateStore.LeaseRelease(s.fencingToken, s.revision));
        });
    }

    /**
     * 计划内换主：当前持有者把领导权主动交接给指定继任者。
     * 成功后继任者立即成为有效持有者并拿到严格更大的栅栏令牌，旧令牌被显式作废
     * （进入交接作废台账，此后旧令牌的提交一律拒绝，不做幂等放行）。
     * 整个判定与换牌在资源临界区内原子完成，任何时刻不会出现两个有效持有者。
     *
     * @param ttlMillis 继任者新租约的有效期（从交接生效时刻起）
     * @return 继任者的新租约快照
     * @throws CoordinationException 携带 {@link HandoverRejectReason} 原因码
     */
    public Lease handover(String resourceId, String holderId, String successorId,
                          long ttlMillis) {
        if (ttlMillis <= 0) {
            throw new IllegalArgumentException("ttlMillis 必须为正数: " + ttlMillis);
        }
        long nowMono = clock.monotonicMillis();
        long nowWall = clock.wallClockMillis();

        return store.compute(resourceId, s -> {
            log.info("[HANDOVER-发起] resource={} 发起者={} 继任者={} nowMono={} 当前槽位={}",
                    resourceId, holderId, successorId, nowMono, describePrior(s));
            if (!s.hasLease) {
                log.info("[HANDOVER-拒绝] resource={} 发起者={} nowMono={} → UNKNOWN_LEASE",
                        resourceId, holderId, nowMono);
                throw new CoordinationException(HandoverRejectReason.UNKNOWN_LEASE,
                        "资源 " + resourceId + " 上不存在租约，无法交接");
            }
            if (!holderId.equals(s.holderId)) {
                log.info("[HANDOVER-拒绝] resource={} 发起者={} 实际持有者={} token={} "
                                + "→ NOT_HOLDER（只有当前持有者能发起交接）",
                        resourceId, holderId, s.holderId, s.fencingToken);
                throw new CoordinationException(HandoverRejectReason.NOT_HOLDER,
                        "节点 " + holderId + " 不是资源 " + resourceId + " 的持有者，无权发起交接");
            }
            if (s.released) {
                log.info("[HANDOVER-拒绝] resource={} 发起者={} token={} → ALREADY_RELEASED",
                        resourceId, holderId, s.fencingToken);
                throw new CoordinationException(HandoverRejectReason.ALREADY_RELEASED,
                        "租约 token=" + s.fencingToken + " 已释放，无法交接");
            }
            if (s.expiresAtMono <= nowMono) {
                log.info("[HANDOVER-拒绝] resource={} 发起者={} token={} 已过期 "
                                + "(expiresAtMono={} <= nowMono={}, 超期={}ms) → EXPIRED"
                                + "（只能等租约自然失效后重新获取）",
                        resourceId, holderId, s.fencingToken, s.expiresAtMono, nowMono,
                        nowMono - s.expiresAtMono);
                throw new CoordinationException(HandoverRejectReason.EXPIRED,
                        "租约 token=" + s.fencingToken + " 已过期，无法交接");
            }
            if (successorId == null || successorId.isBlank()
                    || successorId.equals(s.holderId)) {
                log.info("[HANDOVER-拒绝] resource={} 发起者={} 继任者={} → INVALID_SUCCESSOR",
                        resourceId, holderId, successorId);
                throw new CoordinationException(HandoverRejectReason.INVALID_SUCCESSOR,
                        "继任者非法: " + successorId + "（为空或与当前持有者相同）");
            }

            long oldToken = s.fencingToken;
            long newToken = s.nextToken;
            s.nextToken++;
            s.holderId = successorId;
            s.fencingToken = newToken;
            s.acquiredAtWall = nowWall;
            s.acquiredAtMono = nowMono;
            s.expiresAtMono = nowMono + ttlMillis;
            s.released = false;
            int newRevision = s.revision + 1;
            s.revision = newRevision;
            s.issuedTokens.add(newToken);
            s.handoverSupersededTokens.add(oldToken);

            log.info("[HANDOVER-成功] resource={} {}→{} token={}→{} revision={} wall={} "
                            + "mono={} ttl={}ms 新expiresAtMono={} 旧令牌已作废 → WAL落盘，"
                            + "继任者立即生效（无需等旧租约到期）",
                    resourceId, holderId, successorId, oldToken, newToken, newRevision,
                    nowWall, nowMono, ttlMillis, nowMono + ttlMillis);
            return ClusterStateStore.Result.of(s.snapshot(resourceId),
                    new ClusterStateStore.LeaseHandover(oldToken, newToken, nowWall, nowMono,
                            nowMono + ttlMillis, newRevision, successorId));
        });
    }

    /** 查看资源当前租约快照（不判定过期，调用方可见 released 标志与过期时刻）。 */
    public Optional<Lease> inspect(String resourceId) {
        long nowMono = clock.monotonicMillis();
        return store.compute(resourceId, s -> {
            Lease snapshot = s.snapshot(resourceId);
            log.debug("[INSPECT] resource={} nowMono={} snapshot={}", resourceId, nowMono,
                    snapshot);
            return ClusterStateStore.Result.of(Optional.ofNullable(snapshot));
        });
    }

    private static String describePrior(ResourceState s) {
        if (!s.hasLease) {
            return "无历史租约";
        }
        return "holder=" + s.holderId + ", token=" + s.fencingToken + ", released=" + s.released
                + ", expiresAtMono=" + s.expiresAtMono;
    }
}
