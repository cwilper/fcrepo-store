package com.github.cwilper.fcrepo.store.util.commands;

import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.store.core.FedoraStoreSession;

/**
 * Provides {@link ThreadLocal} data relevant to the currently-executing
 * {@link Command}.
 */
public final class CommandContext {
    private static final ThreadLocal<FedoraStoreSession> tSource =
            new ThreadLocal<FedoraStoreSession>();

    private static final ThreadLocal<FedoraStoreSession> tDestination =
            new ThreadLocal<FedoraStoreSession>();

    private static final ThreadLocal<FedoraObject> tObject =
            new ThreadLocal<FedoraObject>();
    
    private CommandContext() { }

    public static void setSource(FedoraStoreSession source) {
        tSource.set(source);
    }

    public static FedoraStoreSession getSource() {
        return tSource.get();
    }

    public static void setDestination(FedoraStoreSession destination) {
        tDestination.set(destination);
    }

    public static FedoraStoreSession getDestination() {
        return tDestination.get();
    }

    public static void setObject(FedoraObject object) {
        tObject.set(object);
    }

    public static FedoraObject getObject() {
        return tObject.get();
    }
}
