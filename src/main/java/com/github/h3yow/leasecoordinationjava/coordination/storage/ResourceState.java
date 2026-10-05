package com.github.h3yow.leasecoordinationjava.coordination.storage;

import com.github.h3yow.leasecoordinationjava.coordination.lease.Lease;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 单个资源的全部权威状态，仅在 {@link ClusterStateStore} 对该资源的临界区内被访问。
 *
 * <p>包含两部分：</p>
 * <ol>
 *   <li><b>租约槽</b>：当前/最近一次租约。过期只做"惰性判定"——槽位不物理清空，
 *       而是用单调时钟 {@code nowMono} 比较 {@code expiresAtMono}；</li>
 *   <li><b>写入台账</b>：已签发栅栏令牌集合 + 幂等键到已应用写入的映射 + 全局副作用序号。</li>
 * </ol>
 */
public class ResourceState {

    // ---- 租约槽（无租约时 hasLease=false） ----
    public boolean hasLease;
    public String holderId;
    public long fencingToken;
    public long acquiredAtWall;
    public long acquiredAtMono;
    public long expiresAtMono;
    public boolean released;
    public int revision;

    // ---- 栅栏令牌台账 ----
    /** 本资源历史上所有已签发的令牌（重启后从 WAL 恢复），用于识别"旧领导令牌"。 */
    public final Set<Long> issuedTokens = new HashSet<>();
    /** 下一次全新授予使用的令牌，从 1 开始单调递增，只增不减。 */
    public long nextToken = 1L;
    /** 被计划内换主显式作废的令牌：这些令牌的提交一律拒绝，不走幂等重放放行。 */
    public final Set<Long> handoverSupersededTokens = new HashSet<>();

    // ---- 写入台账 ----
    public final Map<String, AppliedWrite> appliedWrites = new HashMap<>();
    public long lastSequence = 0L;

    /** 一条已产生副作用的写入记录（幂等重放时原样返回）。 */
    public record AppliedWrite(long fencingToken, long sequence, String payload) {
    }

    /** 槽位上是否存在一个未释放、且相对 nowMono（单调时钟）尚未过期的租约。 */
    public boolean hasValidLease(long nowMono) {
        return hasLease && !released && expiresAtMono > nowMono;
    }

    /** 生成不可变租约快照；无租约时返回 null。 */
    public Lease snapshot(String resourceId) {
        if (!hasLease) {
            return null;
        }
        return new Lease(resourceId, holderId, fencingToken, acquiredAtWall, acquiredAtMono,
                expiresAtMono, released, revision);
    }
}
