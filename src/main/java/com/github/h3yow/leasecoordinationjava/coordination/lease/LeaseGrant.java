package com.github.h3yow.leasecoordinationjava.coordination.lease;

/**
 * 一次租约授予的不可变凭证。
 *
 * @param resourceId    资源标识
 * @param holderId      持有者（逻辑节点）标识
 * @param fencingToken  本次授予对应的单调递增 fencing 令牌
 * @param acquiredAtMs  授予时间（逻辑时钟，epoch 毫秒）
 * @param expireAtMs    过期时间（epoch 毫秒）
 * @param version       同一资源上的第几次授予（从 1 开始）
 */
public record LeaseGrant(
        String resourceId,
        String holderId,
        long fencingToken,
        long acquiredAtMs,
        long expireAtMs,
        long version
) {

    /**
     * @param nowMs 判定时刻
     * @return 租约在给定时刻是否仍然有效（nowMs &lt; expireAtMs）
     */
    public boolean isValidAt(long nowMs) {
        // 半开区间：恰好等于过期时刻即视为已过期，边界处全局判定一致、无歧义。
        return nowMs < expireAtMs;
    }
}
