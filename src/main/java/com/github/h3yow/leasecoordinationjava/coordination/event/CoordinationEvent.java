package com.github.h3yow.leasecoordinationjava.coordination.event;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * 持久化到协调日志中的事件（事件溯源）。崩溃恢复时重放这些事件重建全部状态。
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = CoordinationEvent.LeaseAcquired.class, name = "LEASE_ACQUIRED"),
        @JsonSubTypes.Type(value = CoordinationEvent.LeaseRenewed.class, name = "LEASE_RENEWED"),
        @JsonSubTypes.Type(value = CoordinationEvent.LeaseReleased.class, name = "LEASE_RELEASED"),
        @JsonSubTypes.Type(value = CoordinationEvent.WriteCommitted.class, name = "WRITE_COMMITTED")
})
public sealed interface CoordinationEvent
        permits CoordinationEvent.LeaseAcquired,
                CoordinationEvent.LeaseRenewed,
                CoordinationEvent.LeaseReleased,
                CoordinationEvent.WriteCommitted {

    /** 事件发生的逻辑时间。 */
    long atMs();

    record LeaseAcquired(String resourceId,
                         String holderId,
                         long fencingToken,
                         long acquiredAtMs,
                         long expireAtMs,
                         long version) implements CoordinationEvent {
        @Override public long atMs() { return acquiredAtMs; }
    }

    record LeaseRenewed(String resourceId,
                        String holderId,
                        long fencingToken,
                        long renewedAtMs,
                        long newExpireAtMs) implements CoordinationEvent {
        @Override public long atMs() { return renewedAtMs; }
    }

    record LeaseReleased(String resourceId,
                         String holderId,
                         long fencingToken,
                         long releasedAtMs) implements CoordinationEvent {
        @Override public long atMs() { return releasedAtMs; }
    }

    record WriteCommitted(String resourceId,
                          String holderId,
                          String requestId,
                          long fencingToken,
                          long committedAtMs,
                          long payloadRef) implements CoordinationEvent {
        @Override public long atMs() { return committedAtMs; }
    }
}
