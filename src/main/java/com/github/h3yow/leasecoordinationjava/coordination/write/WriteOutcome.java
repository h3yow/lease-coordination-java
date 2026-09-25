package com.github.h3yow.leasecoordinationjava.coordination.write;

/**
 * 写入结果。
 *
 * @param accepted       是否真正被接受（产生副作用）
 * @param replay         是否为重放命中（未产生新副作用，返回首次结果）
 * @param appliedToken   实际应用的栅栏令牌
 * @param sequence       副作用序号（首次应用时分配，重放返回原序号）
 * @param idempotencyKey 幂等键
 */
public record WriteOutcome(
        boolean accepted,
        boolean replay,
        long appliedToken,
        long sequence,
        String idempotencyKey,
        String message) {
}
