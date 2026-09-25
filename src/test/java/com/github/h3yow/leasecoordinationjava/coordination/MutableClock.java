package com.github.h3yow.leasecoordinationjava.coordination;

import com.github.h3yow.leasecoordinationjava.coordination.clock.LogicalClock;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 测试用可控时钟：可任意推进、回拨（回拨用于验证 GuardedSystemClock 的保护），
 * 本身保持单调语义以模拟“经过保护后”的时间视图。
 */
public final class MutableClock implements LogicalClock {

    private final AtomicLong now;

    public MutableClock(long startMs) {
        this.now = new AtomicLong(startMs);
    }

    @Override
    public long nowMillis() {
        return now.get();
    }

    /** 时间前进；若传入负数也不会让时钟倒退（模拟受保护的单调时间）。 */
    public long advance(long deltaMs) {
        return now.updateAndGet(v -> v + Math.max(0, deltaMs));
    }

    public void setTo(long valueMs) {
        now.set(valueMs);
    }
}
