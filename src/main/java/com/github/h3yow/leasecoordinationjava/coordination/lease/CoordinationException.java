package com.github.h3yow.leasecoordinationjava.coordination.lease;

/** 协调操作被拒绝时抛出，携带可区分的原因码。 */
public class CoordinationException extends RuntimeException {

    private final Enum<?> reason;

    public CoordinationException(Enum<?> reason, String message) {
        super(message);
        this.reason = reason;
    }

    public Enum<?> reason() {
        return reason;
    }
}
