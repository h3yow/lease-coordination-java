package com.github.h3yow.leasecoordinationjava.coordination.lease;

/**
 * 协调操作被拒绝时抛出的异常，携带可区分的 {@link RejectReason}。
 */
public class CoordinationException extends RuntimeException {

    private final RejectReason reason;

    public CoordinationException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason getReason() {
        return reason;
    }
}
