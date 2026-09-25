package com.github.h3yow.leasecoordinationjava.coordination.write;

/**
 * 有序写入的提交结果。
 *
 * @param resourceId    被写入资源
 * @param requestId     客户端请求标识（用于重放去重）
 * @param fencingToken  本次写入携带的 fencing 令牌
 * @param committedAtMs 提交时间（逻辑时钟）
 * @param replay        true 表示这是重放请求，未产生新的副作用
 */
public record WriteResult(
        String resourceId,
        String requestId,
        long fencingToken,
        long committedAtMs,
        boolean replay
) {
}
