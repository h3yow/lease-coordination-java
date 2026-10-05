package com.github.h3yow.leasecoordinationjava.coordination.lease;

/** 计划内换主（领导权交接）被拒绝的可区分原因。 */
public enum HandoverRejectReason {
    /** 资源上不存在租约。 */
    UNKNOWN_LEASE,
    /** 发起方不是当前持有者（越权交接）。 */
    NOT_HOLDER,
    /** 租约已被释放，无法交接。 */
    ALREADY_RELEASED,
    /** 租约已过期：只能等他人重新获取，不能交接。 */
    EXPIRED,
    /** 继任者非法（为空或与当前持有者相同）。 */
    INVALID_SUCCESSOR
}
