package com.github.cwilper.fcrepo.store.core;

/**
 * Provides {@link FedoraStoreSession}s.
 */
public interface FedoraStore {
    /**
     * Gets a session.
     *
     * @return the session, never <code>null</code>.
     * @throws StoreException if anything goes wrong.
     */
    public FedoraStoreSession getSession();
}
