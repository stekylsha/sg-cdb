package com.strangegizmo.cdb;

public class CdbException extends RuntimeException {

    public CdbException() {
        super();
    }

    public CdbException(String message) {
        super(message);
    }

    public CdbException(String message, Throwable cause) {
        super(message, cause);
    }

    public CdbException(Throwable cause) {
        super(cause);
    }

    public CdbException(String message, Throwable cause,
            boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}