package com.github.cwilper.fcrepo.store.akubra;

import com.github.cwilper.fcrepo.dto.core.io.DTOReader;
import com.github.cwilper.fcrepo.dto.core.io.DTOWriter;
import com.github.cwilper.fcrepo.store.core.FedoraStore;
import com.github.cwilper.fcrepo.store.core.FedoraStoreSession;
import org.akubraproject.BlobStore;

/**
 * Akubra-based {@link FedoraStore} implementation.
 * <p>
 * This implementation is designed to work with two distinct
 * {@link BlobStore}s, one for
 * {@link com.github.cwilper.fcrepo.dto.core.FedoraObject}s and one for managed
 * datastream content.
 * <p>
 * <h2>Compatibility</h2>
 * This implementation is compatible with storage managed by Fedora's <code>
 * <a href="http://fedora-commons.org/documentation/3.2/javadocs/fedora/server/storage/lowlevel/akubra/AkubraLowlevelStorage.html">AkubraLowlevelStorage</a>
 * </code> module, available since Fedora 3.2. Accordingly, the configured blob
 * stores <strong>MUST</strong>:
 * <ul>
 *   <li> be <em>non-transactional</em></li>
 *   <li> be able to accept <code>info:fedora/</code> URIs as blob ids.
 * </ul>
 * <p>
 * <h2>Atomicity</h2>
 * This implementation <em>does not</em> attempt "safe" overwrites via rename.
 * If atomic writes are needed, they must be provided by the configured blob
 * stores.
 * <p>
 * <h2>Blob Ids</h2>
 * <ul>
 *   <li>When storing/retrieving serialized Fedora objects, the blob ids used
 *       will be of the form <code>info:fedora/pid</code>.</li>
 *   <li>When storing/retrieving managed datastream content, the blob ids used
 *       will be of the form <code>info:fedora/pid/dsId/dsVersionId</code>.
 *       </li>
 * </ul>
 */
public class AkubraFedoraStore implements FedoraStore {
    private final BlobStore objectStore;
    private final BlobStore contentStore;
    private final DTOReader readerFactory;
    private final DTOWriter writerFactory;

    /**
     * Creates an instance.
     *
     * @param objectStore the blob store to use for Fedora objects.
     * @param contentStore the blob store to use for managed content.
     * @param readerFactory the factory to use for deserializing.
     * @param writerFactory the factory to use for serializing.
     * @throws NullPointerException if any argument is null.
     */
    public AkubraFedoraStore(BlobStore objectStore, BlobStore contentStore,
            DTOReader readerFactory, DTOWriter writerFactory) {
        if (objectStore == null || contentStore == null
                || readerFactory == null || writerFactory == null) {
            throw new NullPointerException();
        }
        this.objectStore = objectStore;
        this.contentStore = contentStore;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
    }

    @Override
    public FedoraStoreSession getSession() {
        return new AkubraFedoraStoreSession(objectStore, contentStore,
                readerFactory, writerFactory);
    }
}
