package com.github.h3yow.leasecoordinationjava.coordination.lease;

/**
 * 租约不可变快照。
 *
 * @param fencingToken 栅栏令牌：每次全新授予递增，续约保持不变
 * @param revision     该租约的修订版本，初次授予为 1，每次续约 +1
 * @param released     是否已被显式释放
 */
public record Lease(
        String resourceId,
        String holderId,
        long fencingToken,
        long acquiredAtWall,
        long acquiredAtMono,
        long expiresAtMono,
        boolean released,
        int revision) {
}
