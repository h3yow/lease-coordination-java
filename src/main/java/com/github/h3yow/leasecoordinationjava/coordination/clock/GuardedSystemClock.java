package com.github.h3yow.leasecoordinationjava.coordination.clock;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * 受保护的系统时钟。
 *
 * <p>保护规则：</p>
 * <ul>
 *   <li><b>单调不回退</b>：内部维护“曾经观测到的最大读数”{@code lastObserved}。
 *       若底层原始时钟发生回拨（NTP 校时、闰秒处理、虚拟机迁移等），返回
 *       {@code lastObserved} 而不是更小的值——保证租约不会因为时钟回拨而“复活”，
 *       也不会把时间戳交给过期判定造成混乱。</li>
 *   <li><b>容忍瞬时抖动</b>：同一毫秒内多次读取得到相同值是允许的（单调非严格递增）；
 *       一旦原始时钟恢复并超过历史最大值，立即重新跟随真实时间。</li>
 * </ul>
 *
 * <p>注意：单调化只在本进程内成立。本实现模拟的是单进程多逻辑节点集群，
 * 所有节点共用同一个时钟实例，因此“同一资源不会出现两个同时有效”的判定有统一时间基准。</p>
 */
public final class GuardedSystemClock implements LogicalClock {

    private final LongSupplier rawClock;
    private final AtomicLong lastObserved;

    /** 生产环境使用真实系统时钟。 */
    public GuardedSystemClock() {
        this(System::currentTimeMillis);
    }

    /** 便于测试：注入可操纵的“原始时钟”，以验证回拨保护。 */
    public GuardedSystemClock(LongSupplier rawClock) {
        this.rawClock = rawClock;
        this.lastObserved = new AtomicLong(rawClock.getAsLong());
    }

    @Override
    public long nowMillis() {
        long raw = rawClock.getAsLong();
        return lastObserved.updateAndGet(prev -> Math.max(prev, raw));
    }
}
