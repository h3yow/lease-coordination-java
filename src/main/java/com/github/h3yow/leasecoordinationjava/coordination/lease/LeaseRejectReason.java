package com.github.h3yow.leasecoordinationjava.coordination.lease;

/** 租约操作被拒绝的可区分原因。 */
public enum LeaseRejectReason {
    /** 获取时资源仍被其他有效持有者占用。 */
    HELD_BY_OTHER,
    /** 续约/释放者不是当前持有者（越权操作）。 */
    NOT_HOLDER,
    /** 对已过期租约执行续约。 */
    EXPIRED,
    /** 对已释放租约重复释放。 */
    ALREADY_RELEASED,
    /** 资源上从未授予过租约（或已被清理）。 */
    UNKNOWN_LEASE,
    /** 交接目标非法：继任者为空或与当前持有者相同。 */
    INVALID_SUCCESSOR
}
