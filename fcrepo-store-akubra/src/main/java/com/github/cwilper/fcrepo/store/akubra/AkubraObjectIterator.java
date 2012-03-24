package com.github.cwilper.fcrepo.store.akubra;

import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.dto.core.io.DTOReader;
import com.google.common.collect.AbstractIterator;
import org.akubraproject.BlobStoreConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

/**
 * An iterator of {@link FedoraObject}s in an Akubra blob store.
 */
class AkubraObjectIterator
        extends AbstractIterator<FedoraObject> {
    private static final Logger logger =
            LoggerFactory.getLogger(AkubraObjectIterator.class);

    private final BlobStoreConnection connection;
    private final Iterator<URI> ids;
    private final DTOReader readerFactory;

    /**
     * Creates an instance.
     *
     * @param connection the connection.
     * @param ids the blob ids of the Fedora objects to iterate.
     * @param readerFactory the factory to use to deserialize the objects.
     */
    public AkubraObjectIterator(BlobStoreConnection connection,
            Iterator<URI> ids, DTOReader readerFactory) {
        this.connection = connection;
        this.ids = ids;
        this.readerFactory = readerFactory;
    }

    @Override
    protected FedoraObject computeNext() {
        while (ids.hasNext()) {
            URI id = ids.next();
            try {
                return Util.readObject(readerFactory,
                        connection.getBlob(id, null));
            } catch (IOException e) {
                logger.warn(Constants.ERR_PARSING_OBJ + ": " + id, e);
            }
        }
        return endOfData();
    }
}

