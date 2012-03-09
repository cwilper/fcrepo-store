package com.github.cwilper.fcrepo.store.core;

/**
 * Signals a failure to add an object because one with the same pid already
 * exists.
 */
public class ExistsException extends StoreException {
    /**
     * Creates an instance with a message.
     *
     * @param message the message.
     */
    public ExistsException(String message) {
        super(message);
    }

    /**
     * Creates an instance with a message and cause.
     *
     * @param message the message.
     * @param cause the cause.
     */
    public ExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
