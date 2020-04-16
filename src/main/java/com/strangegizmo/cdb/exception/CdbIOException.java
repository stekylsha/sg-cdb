package com.strangegizmo.cdb.exception;

/**
 * Exception specific for reading and/or writing the CDB or CDB dump files.
 */
public class CdbIOException extends RuntimeException {

    public CdbIOException() {
        super();
    }

    public CdbIOException(String message) {
        super(message);
    }

    public CdbIOException(String message, Throwable cause) {
        super(message, cause);
    }

    public CdbIOException(Throwable cause) {
        super(cause);
    }

    public CdbIOException(String message, Throwable cause,
            boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}