package com.github.h3yow.leasecoordinationjava.coordination.clock;

/**
 * 集群统一使用的逻辑时钟。所有判定（租约过期、令牌顺序）只允许依赖该时钟给出的时间。
 */
public interface LogicalClock {

    /**
     * @return 当前逻辑时间（epoch 毫秒）。实现必须保证单调不回退。
     */
    long nowMillis();
}
