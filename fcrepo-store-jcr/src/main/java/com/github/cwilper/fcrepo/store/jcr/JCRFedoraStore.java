package com.github.cwilper.fcrepo.store.jcr;

import com.github.cwilper.fcrepo.dto.core.io.DTOReader;
import com.github.cwilper.fcrepo.dto.core.io.DTOWriter;
import com.github.cwilper.fcrepo.store.core.FedoraStore;
import com.github.cwilper.fcrepo.store.core.FedoraStoreSession;
import com.github.cwilper.fcrepo.store.core.StoreException;

import javax.jcr.Credentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;

/**
 * JCR-based implementation of {@link FedoraStore}.
 */
public class JCRFedoraStore implements FedoraStore {
    private final Repository repository;
    private final Credentials credentials;
    private final DTOReader readerFactory;
    private final DTOWriter writerFactory;

    public JCRFedoraStore(Repository repository, Credentials credentials,
            DTOReader readerFactory, DTOWriter writerFactory) {
        if (repository == null || credentials == null || readerFactory == null
                || writerFactory == null)
            throw new NullPointerException();
        this.repository = repository;
        this.credentials = credentials;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
    }

    @Override
    public FedoraStoreSession getSession() {
        try {
            return new JCRFedoraStoreSession(repository.login(credentials),
                    readerFactory, writerFactory);
        } catch (RepositoryException e) {
            throw new StoreException("Error getting JCR session", e);
        }
    }
}
