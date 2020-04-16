package com.strangegizmo.cdb.exception;

/**
 * Exception specific to incorrectly formatted CDB and CDB dump files.
 */
public class CdbFormatException extends RuntimeException {

    public CdbFormatException() {
        super();
    }

    public CdbFormatException(String message) {
        super(message);
    }

    public CdbFormatException(String message, Throwable cause) {
        super(message, cause);
    }

    public CdbFormatException(Throwable cause) {
        super(cause);
    }

    public CdbFormatException(String message, Throwable cause,
            boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}