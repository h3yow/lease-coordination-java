package com.github.h3yow.leasecoordinationjava.coordination.clock;

/**
 * 可在测试中精确控制的时钟。
 *
 * <p>维护两条独立时间线：</p>
 * <ul>
 *   <li><b>单调时钟</b> {@code mono}：只增不减，{@link #shiftWallClock} 不影响它，
 *       租约过期判定只看它 —— 因此墙上时钟回拨/抖动永远不会改变租约结论；</li>
 *   <li><b>墙上时钟</b> {@code wall}：可以任意拨动（包括向前回拨、瞬时正负抖动），
 *       仅用于日志中的人类可读时间戳。</li>
 * </ul>
 */
public class SimulatedClock implements TimeSource {

    /** 墙上时钟初始锚点：2023-11-14T22:13:20Z，仅为了日志可读。 */
    private static final long WALL_ANCHOR = 1_700_000_000_000L;

    private volatile long mono;
    private volatile long wall;

    public SimulatedClock() {
        this.mono = 0L;
        this.wall = WALL_ANCHOR;
    }

    @Override
    public long wallClockMillis() {
        return wall;
    }

    @Override
    public long monotonicMillis() {
        return mono;
    }

    /** 让真实流逝的时间前进 delta 毫秒，单调时钟与墙上时钟同步前进（delta 必须非负）。 */
    public void advance(long deltaMillis) {
        if (deltaMillis < 0) {
            throw new IllegalArgumentException("单调时钟只能前进，delta=" + deltaMillis);
        }
        this.mono += deltaMillis;
        this.wall += deltaMillis;
    }

    /**
     * 仅拨动墙上时钟：delta 可为负（NTP 回拨）也可为小的正负值（瞬时抖动）。
     * 单调时钟完全不受影响，保证租约判定不被时钟异常干扰。
     */
    public void shiftWallClock(long deltaMillis) {
        this.wall += deltaMillis;
    }
}
