package com.strangegizmo.cdb.exception;

/**
 * Exception specific for state problems.  Things like trying to call {@code next()}
 * when there is no next; calling methods on a CDB that has been closed; and
 * other horrific acts.
 */
public class CdbStateException extends RuntimeException {

    public CdbStateException() {
        super();
    }

    public CdbStateException(String message) {
        super(message);
    }

    public CdbStateException(String message, Throwable cause) {
        super(message, cause);
    }

    public CdbStateException(Throwable cause) {
        super(cause);
    }

    public CdbStateException(String message, Throwable cause,
            boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}