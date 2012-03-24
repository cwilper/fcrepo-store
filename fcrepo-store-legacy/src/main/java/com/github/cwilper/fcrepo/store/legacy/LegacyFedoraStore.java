package com.github.cwilper.fcrepo.store.legacy;

import com.github.cwilper.fcrepo.dto.core.io.DTOReader;
import com.github.cwilper.fcrepo.dto.core.io.DTOWriter;
import com.github.cwilper.fcrepo.store.core.FedoraStore;
import com.github.cwilper.fcrepo.store.core.FedoraStoreSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Legacy {@link FedoraStore} implementation compatible with pre-Akubra
 * versions of Fedora.
 */
public class LegacyFedoraStore implements FedoraStore {
    private static final Logger logger = LoggerFactory.getLogger(
            LegacyFedoraStore.class);

    private final FileStore objectStore;
    private final FileStore contentStore;
    private final DTOReader readerFactory;
    private final DTOWriter writerFactory;

    /**
     * Creates an instance. Upon construction, the object and content
     * path registries will be built for the first time if they're empty.
     *
     * @param objectStore the file store to use for Fedora objects.
     * @param contentStore the file store to use for managed content.
     * @param readerFactory the factory to use for deserializing.
     * @param writerFactory the factory to use for serializing.
     * @throws NullPointerException if any argument is null.
     */
    public LegacyFedoraStore(FileStore objectStore, FileStore contentStore,
            DTOReader readerFactory, DTOWriter writerFactory) {
        if (objectStore == null || contentStore == null
                || readerFactory == null || writerFactory == null) {
            throw new NullPointerException();
        }
        this.objectStore = objectStore;
        this.contentStore = contentStore;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        populateRegistryIfEmpty(objectStore, "Object");
        populateRegistryIfEmpty(contentStore, "Content");
    }

    private void populateRegistryIfEmpty(FileStore fileStore, String name) {
        if (fileStore.getPathCount() == 0) {
            logger.info("Populating {} Store Path Registry.", name);
            fileStore.populateRegistry();
        }
    }
    
    @Override
    public FedoraStoreSession getSession() {
        return new LegacyFedoraStoreSession(objectStore, contentStore,
                readerFactory, writerFactory);
    }
}
