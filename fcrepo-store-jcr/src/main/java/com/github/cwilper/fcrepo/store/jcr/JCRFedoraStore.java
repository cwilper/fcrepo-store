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
 * <p>
 * In order to scale effectively to millions of objects, this implementation
 * stores serialized objects and managed datastream content within a
 * multi-level directory (<code>nt:folder</code>) structure based on the pid
 * of the object. The first four lowercase hex digits of the md5 digest of the
 * pid are used to compose the top and mid-level folder paths, and the pid
 * with an underscore instead of a colon is used as the final folder path
 * element, as follows (for object <code>test:o1</code>):
 * <ul>
 *   <li> <code>/08/cf/test_o1</code></li>
 * </ul>
 * <p>
 * The serialized object is saved within an <code>nt:file</code> sub-node
 * named <code>object</code>, which has an <code>nt:resource</code> sub-node
 * named <code>jcr:content</code>, whose <code>jcr:data</code> property holds
 * the binary value. So the serialized object <code>test:o1</code> would be
 * found at:
 * <ul>
 *   <li> <code>/08/cf/test_o1/object/jcr:content[jcr:data]</code></li>
 * </ul>
 * <p>
 * In addition, managed datastream content resides beneath a sub-folder of the
 * object named using the datastream id. Within that folder, the content is
 * stored in an <code>nt:file</code> sub-node named using the datastream
 * version id, which has an <code>nt:resource</code> sub-node named
 * <code>jcr:content</code>, whose <code>jcr:data</code> property holds
 * the binary value. So the content of <code>test:o1</code>'s managed
 * datastream with id <code>DS1</code> and version id <code>DS1.0</code>
 * would be found at:
 * <p>
 * <ul>
 *   <li> <code>/08/cf/test_o1/DS1/DS1.0/jcr:content[jcr:data]</code></li>
 * </ul>
 */
public class JCRFedoraStore implements FedoraStore {
    private final Repository repository;
    private final Credentials credentials;
    private final DTOReader readerFactory;
    private final DTOWriter writerFactory;

    /**
     * Creates an instance.
     *
     * @param repository the repository to use.
     * @param credentials the credentials to use when opening sessions.
     * @param readerFactory the factory to use when objects need to be read.
     * @param writerFactory the factory to use when objects need to be written.
     * @throws NullPointerException if any argument is null.
     */
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
