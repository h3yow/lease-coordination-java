package com.github.h3yow.leasecoordinationjava.coordination.lease;

/**
 * 协调操作被拒绝的可区分原因。
 */
public enum RejectReason {

    /** 目标资源上存在未过期租约，且持有者不是自己。 */
    LEASE_HELD_BY_OTHER,

    /** 续约或释放时携带的租约不属于该资源/持有者（越权操作）。 */
    LEASE_NOT_OWNER,

    /** 续约的租约已过期或已被释放，必须重新获取。 */
    LEASE_EXPIRED,

    /** 对已经释放的租约再次释放（重复释放）。 */
    LEASE_ALREADY_RELEASED,

    /** 写入携带的 fencing 令牌低于资源上已提交过的最高令牌（旧领导提交）。 */
    FENCING_TOKEN_STALE,

    /** 写入携带的令牌在任何一次授予中都不存在（伪造/未知令牌）。 */
    FENCING_TOKEN_UNKNOWN,

    /** 持令牌方在写入时租约已过期，领导权已失效。 */
    FENCING_LEASE_EXPIRED,

    /** 其他未分类的非法参数。 */
    ILLEGAL_ARGUMENT
}
