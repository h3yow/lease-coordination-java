package com.github.h3yow.leasecoordinationjava.coordination.write;

/** 有序写入被拒绝的可区分原因。 */
public enum WriteRejectReason {
    /** 令牌从未被签发过。 */
    UNKNOWN_TOKEN,
    /** 令牌属于旧领导（小于当前签发的栅栏令牌，或小于已应用令牌）。 */
    STALE_TOKEN,
    /** 令牌对应租约已过期或已释放。 */
    LEASE_EXPIRED
}
