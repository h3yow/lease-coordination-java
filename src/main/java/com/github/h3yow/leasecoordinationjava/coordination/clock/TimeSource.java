package com.github.h3yow.leasecoordinationjava.coordination.clock;

/**
 * 时间源：区分墙上时钟（可能回拨/抖动）与单调时钟（只增不减）。
 * 租约判定一律基于单调时钟，墙上时钟仅用于日志展示。
 */
public interface TimeSource {

    /** 墙上时钟（毫秒），可能因 NTP 回拨或人为调整而倒退。 */
    long wallClockMillis();

    /** 单调时钟（毫秒），保证不回退，是租约过期判定的唯一依据。 */
    long monotonicMillis();
}
