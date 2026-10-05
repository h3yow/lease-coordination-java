package com.github.h3yow.leasecoordinationjava.coordination.write;

/** 有序写入被拒绝的可区分原因。 */
public enum WriteRejectReason {
    /** 令牌从未被签发过。 */
    UNKNOWN_TOKEN,
    /** 令牌属于旧领导（小于当前签发的栅栏令牌，或小于已应用令牌）。 */
    STALE_TOKEN,
    /** 令牌对应租约已过期或已释放。 */
    LEASE_EXPIRED,
    /** 令牌已被计划内换主显式作废（交接生效后旧令牌一律拒绝，不做幂等放行）。 */
    HANDOVER_SUPERSEDED
}
