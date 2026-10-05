package com.github.h3yow.leasecoordinationjava.coordination.write;

import com.github.h3yow.leasecoordinationjava.coordination.clock.TimeSource;
import com.github.h3yow.leasecoordinationjava.coordination.lease.CoordinationException;
import com.github.h3yow.leasecoordinationjava.coordination.storage.ClusterStateStore;
import com.github.h3yow.leasecoordinationjava.coordination.storage.ResourceState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 有序写入服务（fencing-token 模式）。
 *
 * <p>提交按以下顺序判定，全部在资源的线性化临界区内完成：</p>
 * <ol>
 *   <li><b>交接边界</b>：令牌 {@code <=} 交接边界（已在计划内交接中被取代）→
 *       {@link WriteRejectReason#SUPERSEDED_BY_HANDOVER}，优先于幂等重放，
 *       即使该请求之前成功落过库也绝不放行；</li>
 *   <li><b>幂等重放</b>：幂等键已存在 → 无论令牌是否仍有效，原样返回首次结果，
 *       {@code replay=true}，绝不产生第二次副作用（崩溃前已落盘的提交重放安全）；</li>
 *   <li><b>旧领导 / 过期令牌</b>：令牌从未签发 → {@link WriteRejectReason#UNKNOWN_TOKEN}；
 *       令牌小于当前租约槽的令牌（已产生更新的领导） → {@link WriteRejectReason#STALE_TOKEN}；
 *       令牌对应租约已释放或已过期（基于单调时钟） →
 *       {@link WriteRejectReason#LEASE_EXPIRED}；</li>
 *   <li><b>接受</b>：分配单调副作用序号并 WAL 落盘，外部存储可据此拒绝乱序写入。</li>
 * </ol>
 */
public class WriteService {

    private static final Logger log = LoggerFactory.getLogger(WriteService.class);

    private final ClusterStateStore store;
    private final TimeSource clock;

    public WriteService(ClusterStateStore store, TimeSource clock) {
        this.store = store;
        this.clock = clock;
    }

    /**
     * 查询资源已提交到的最大副作用序号（交接后继任者据此从已知进度继续写）。
     * 无任何提交时返回 0。
     */
    public long committedSequence(String resourceId) {
        return store.compute(resourceId, s -> {
            log.info("[INSPECT-SEQ] resource={} committedSeq={} nowMono={}",
                    resourceId, s.lastSequence, clock.monotonicMillis());
            return ClusterStateStore.Result.of(s.lastSequence);
        });
    }

    public WriteOutcome submit(String resourceId, String nodeId, long fencingToken,
                               String idempotencyKey, String payload) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("idempotencyKey 不能为空");
        }
        long nowMono = clock.monotonicMillis();

        return store.compute(resourceId, s -> {
            // 0) 交接边界优先于一切（含幂等重放）：交接生效后，旧令牌的任何提交一律拒绝，
            //    即使该幂等键之前确实成功落过库，也绝不按"重复提交"放行
            if (fencingToken <= s.handoverBoundaryToken) {
                log.info("[WRITE-拒绝] resource={} node={} key={} token={} 交接边界={} "
                                + "nowMono={} → SUPERSEDED_BY_HANDOVER（令牌已在计划内交接中被取代，"
                                + "重放也不放行）",
                        resourceId, nodeId, idempotencyKey, fencingToken,
                        s.handoverBoundaryToken, nowMono);
                throw new CoordinationException(WriteRejectReason.SUPERSEDED_BY_HANDOVER,
                        "令牌 " + fencingToken + " 已在计划内交接中被取代（交接边界="
                                + s.handoverBoundaryToken + "），拒绝写入");
            }

            // 1) 幂等重放优先：同一请求重放不产生重复副作用，即使发起者已不是领导
            ResourceState.AppliedWrite prior = s.appliedWrites.get(idempotencyKey);
            if (prior != null) {
                log.info("[WRITE-重放] resource={} node={} key={} token={} 命中首次结果 "
                                + "seq={} 首次token={} nowMono={} → replay=true，无新副作用",
                        resourceId, nodeId, idempotencyKey, fencingToken,
                        prior.sequence(), prior.fencingToken(), nowMono);
                return ClusterStateStore.Result.of(new WriteOutcome(
                        true, true, prior.fencingToken(), prior.sequence(),
                        idempotencyKey, "幂等重放：返回首次提交结果 seq=" + prior.sequence()));
            }

            // 2) 令牌合法性：从未签发
            if (!s.issuedTokens.contains(fencingToken)) {
                log.info("[WRITE-拒绝] resource={} node={} key={} token={} nowMono={} "
                                + "已签发令牌={} → UNKNOWN_TOKEN",
                        resourceId, nodeId, idempotencyKey, fencingToken, nowMono,
                        s.issuedTokens);
                throw new CoordinationException(WriteRejectReason.UNKNOWN_TOKEN,
                        "令牌 " + fencingToken + " 从未被签发，拒绝写入");
            }

            // 3) 旧领导令牌：已有更新的栅栏令牌占据租约槽
            if (!s.hasLease || s.fencingToken > fencingToken) {
                long currentToken = s.hasLease ? s.fencingToken : -1L;
                log.info("[WRITE-拒绝] resource={} node={} key={} token={} 当前token={} "
                                + "nowMono={} → STALE_TOKEN（旧领导，已发生领导权变更）",
                        resourceId, nodeId, idempotencyKey, fencingToken, currentToken, nowMono);
                throw new CoordinationException(WriteRejectReason.STALE_TOKEN,
                        "令牌 " + fencingToken + " 已过期：当前领导令牌为 " + currentToken);
            }

            // 4) 槽位令牌等于提交令牌：提交者必须仍是该令牌持有者，且租约未释放/未过期
            if (s.fencingToken != fencingToken || !nodeId.equals(s.holderId) || s.released
                    || s.expiresAtMono <= nowMono) {
                log.info("[WRITE-拒绝] resource={} node={} key={} token={} holder={} "
                                + "released={} expiresAtMono={} nowMono={} → LEASE_EXPIRED",
                        resourceId, nodeId, idempotencyKey, fencingToken, s.holderId,
                        s.released, s.expiresAtMono, nowMono);
                throw new CoordinationException(WriteRejectReason.LEASE_EXPIRED,
                        "令牌 " + fencingToken + " 对应领导权已失效（非持有者/已释放/已过期）"
                                + "，拒绝写入");
            }

            // 5) 接受：序号单调递增，WAL 落盘后才算生效
            long seq = ++s.lastSequence;
            s.appliedWrites.put(idempotencyKey,
                    new ResourceState.AppliedWrite(fencingToken, seq, payload));
            log.info("[WRITE-接受] resource={} node={} key={} token={} seq={} wall={} "
                            + "nowMono={} expiresAtMono={} 剩余={}ms → 顺序生效并WAL落盘",
                    resourceId, nodeId, idempotencyKey, fencingToken, seq,
                    clock.wallClockMillis(), nowMono, s.expiresAtMono,
                    s.expiresAtMono - nowMono);
            return ClusterStateStore.Result.of(new WriteOutcome(
                            true, false, fencingToken, seq, idempotencyKey,
                            "写入已按序接受 seq=" + seq),
                    new ClusterStateStore.WriteApplied(fencingToken, seq, idempotencyKey,
                            payload));
        });
    }
}
