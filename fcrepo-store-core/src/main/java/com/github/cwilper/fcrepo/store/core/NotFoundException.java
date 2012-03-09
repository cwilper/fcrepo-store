package com.github.cwilper.fcrepo.store.core;

/**
 * Signals an object or managed content stream does not exist as expected.
 */
public class NotFoundException extends StoreException {
    /**
     * Creates an instance with a message.
     *
     * @param message the message.
     */
    public NotFoundException(String message) {
        super(message);
    }

    /**
     * Creates an instance with a message and cause.
     *
     * @param message the message.
     * @param cause the cause.
     */
    public NotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
