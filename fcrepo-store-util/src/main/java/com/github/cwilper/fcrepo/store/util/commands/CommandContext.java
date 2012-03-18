package com.github.cwilper.fcrepo.store.util.commands;

import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.store.core.FedoraStore;

/**
 * Provides {@link ThreadLocal} data relevant to the currently-executing
 * {@link Command}.
 */
public final class CommandContext {
    private static final ThreadLocal<FedoraStore> tSource =
            new ThreadLocal<FedoraStore>();

    private static final ThreadLocal<FedoraStore> tDestination =
            new ThreadLocal<FedoraStore>();

    private static final ThreadLocal<FedoraObject> tObject =
            new ThreadLocal<FedoraObject>();
    
    private CommandContext() { }

    public static void setSource(FedoraStore source) {
        tSource.set(source);
    }

    public static FedoraStore getSource() {
        return tSource.get();
    }

    public static void setDestination(FedoraStore destination) {
        tDestination.set(destination);
    }

    public static FedoraStore getDestination() {
        return tDestination.get();
    }

    public static void setObject(FedoraObject object) {
        tObject.set(object);
    }

    public static FedoraObject getObject() {
        return tObject.get();
    }
}
