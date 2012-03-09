package com.github.cwilper.fcrepo.store.core;

/**
 * Signals an error while working with a store.
 */
public class StoreException extends RuntimeException {
    /**
     * Creates an instance with a message.
     *
     * @param message the message.
     */
    public StoreException(String message) {
        super(message);
    }

    /**
     * Creates an instance with a message and cause.
     *
     * @param message the message.
     * @param cause the cause.
     */
    public StoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
