package com.github.cwilper.fcrepo.store.akubra;

import com.github.cwilper.fcrepo.dto.core.ControlGroup;
import com.github.cwilper.fcrepo.dto.core.Datastream;
import com.github.cwilper.fcrepo.dto.core.DatastreamVersion;
import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.dto.core.io.DTOReader;
import com.github.cwilper.fcrepo.dto.core.io.DTOWriter;
import com.github.cwilper.fcrepo.store.core.ExistsException;
import com.github.cwilper.fcrepo.store.core.FedoraStore;
import com.github.cwilper.fcrepo.store.core.NotFoundException;
import com.github.cwilper.fcrepo.store.core.StoreException;
import org.akubraproject.Blob;
import org.akubraproject.BlobStore;
import org.akubraproject.BlobStoreConnection;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Iterator;

/**
 * Akubra-based {@link FedoraStore} implementation.
 * <p>
 * This implementation is designed to work with two distinct
 * {@link BlobStore}s, one for {@link FedoraObject}s and one for managed
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
    private static final Logger logger =
            LoggerFactory.getLogger(AkubraFedoraStore.class);

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
    public void addObject(FedoraObject object) {
        if (object == null) throw new NullPointerException();
        if (object.pid() == null) throw new IllegalArgumentException();
        BlobStoreConnection connection = Util.getConnection(objectStore);
        try {
            Blob blob = Util.getBlob(connection, object.pid());
            if (blob.exists()) throw new ExistsException(object.pid());
            Util.writeObject(writerFactory, object, blob);
        } catch (IOException e) {
            throw new StoreException(Constants.ERR_ADDING_OBJ, e);
        } finally {
            connection.close();
        }
    }

    @Override
    public FedoraObject getObject(String pid) {
        if (pid == null) throw new NullPointerException();
        BlobStoreConnection connection = Util.getConnection(objectStore);
        try {
            Blob blob = Util.getBlob(connection, pid);
            if (!blob.exists()) throw new NotFoundException(
                    Constants.ERR_NOTFOUND_OBJ_IN_STORAGE + ": " + pid);
            return Util.readObject(readerFactory, blob);
        } catch (IOException e) {
            throw new StoreException(Constants.ERR_GETTING_OBJ, e);
        } finally {
            connection.close();
        }
    }

    @Override
    public void updateObject(FedoraObject object) {
        if (object == null) throw new NullPointerException();
        if (object.pid() == null) throw new IllegalArgumentException();
        BlobStoreConnection connection = Util.getConnection(objectStore);
        try {
            Blob blob = Util.getBlob(connection, object.pid());
            if (!blob.exists()) throw new NotFoundException(
                    Constants.ERR_NOTFOUND_OBJ_IN_STORAGE + ": "
                    + object.pid());
            deleteOldManagedContent(
                    Util.readObject(readerFactory, blob), object);
            Util.writeObject(writerFactory, object, blob);
        } catch (IOException e) {
            throw new StoreException(Constants.ERR_UPDATING_OBJ, e);
        } finally {
            connection.close();
        }
    }

    @Override
    public void deleteObject(String pid) {
        if (pid == null) throw new NullPointerException();
        BlobStoreConnection connection = Util.getConnection(objectStore);
        try {
            Blob blob = Util.getBlob(connection, pid);
            if (!blob.exists()) throw new NotFoundException(
                    Constants.ERR_NOTFOUND_OBJ_IN_STORAGE + ": " + pid);
            deleteOldManagedContent(
                    Util.readObject(readerFactory, blob), null);
            blob.delete();
        } catch (IOException e) {
            throw new StoreException(Constants.ERR_DELETING_OBJ, e);
        } finally {
            connection.close();
        }
    }

    @Override
    public InputStream getContent(String pid, String datastreamId,
            String datastreamVersionId) {
        Blob blob = getContentBlob(
                pid, datastreamId, datastreamVersionId, true);
        boolean success = false;
        try {
            InputStream inputStream = new ConnectionClosingInputStream(
                    blob.getConnection(), blob.openInputStream());
            success = true;
            return inputStream;
        } catch (IOException e) {
            throw new StoreException(Constants.ERR_GETTING_CONT, e);
        } finally {
            if (!success) blob.getConnection().close();
        }
    }

    @Override
    public long getContentLength(String pid, String datastreamId,
            String datastreamVersionId) {
        Blob blob = getContentBlob(
                pid, datastreamId, datastreamVersionId, true);
        try {
            return blob.getSize();
        } catch (IOException e) {
            throw new StoreException(Constants.ERR_GETTING_CONT_LEN, e);
        } finally {
            blob.getConnection().close();
        }
    }

    @Override
    public void setContent(String pid, String datastreamId,
            String datastreamVersionId, InputStream inputStream) {
        if (inputStream == null) throw new NullPointerException();
        Blob blob = getContentBlob(
                pid, datastreamId, datastreamVersionId, false);
        OutputStream outputStream = null;
        boolean success = false;
        try {
            outputStream = blob.openOutputStream(-1, true);
            IOUtils.copyLarge(inputStream, outputStream);
            inputStream.close();
            outputStream.close();
            success = true;
        } catch (IOException e) {
            throw new StoreException(Constants.ERR_SETTING_CONT, e);
        } finally {
            if (!success) {
                Util.closeOrWarn(inputStream);
                Util.closeOrWarn(outputStream);
            }
        }
    }

    @Override
    public Iterator<FedoraObject> iterator() {
        BlobStoreConnection connection = Util.getConnection(objectStore);
        boolean success = false;
        try {
            Iterator<URI> ids = connection.listBlobIds(null);
            success = true;
            return new ConnectionClosingObjectIterator(connection, ids,
                    readerFactory);
        } catch (IOException e) {
            throw new StoreException(Constants.ERR_LISTING_OBJS, e);
        } finally {
            if (!success) {
                connection.close();
            }
        }
    }

    private Blob getContentBlob(String pid, String datastreamId,
            String datastreamVersionId, boolean mustExist) {
        if (pid == null  || datastreamId == null ||
                datastreamVersionId == null) throw new NullPointerException();
        FedoraObject object = getObject(pid);
        if (!Util.hasManagedDatastreamVersion(object, datastreamId,
                datastreamVersionId)) {
            throw new NotFoundException(Constants.ERR_NOTFOUND_DS_IN_OBJ + " "
                    + Util.getDetails(pid, datastreamId, datastreamVersionId));
        }
        BlobStoreConnection connection = Util.getConnection(contentStore);
        boolean success = false;
        try {
            Blob blob = Util.getBlob(connection, pid, datastreamId,
                    datastreamVersionId);
            if (mustExist && !blob.exists()) {
                throw new NotFoundException(
                        Constants.ERR_NOTFOUND_DS_IN_STORAGE + " "
                        + Util.getDetails(pid, datastreamId,
                        datastreamVersionId));
            }
            success = true;
            return blob;
        } catch (IOException e) {
            throw new StoreException(Constants.ERR_GETTING_CONT, e);
        } finally {
            if (!success) connection.close();
        }
    }

    // just log a warning message in the event of failure
    private void deleteContent(String pid, String datastreamId,
            String datastreamVersionId) {
        BlobStoreConnection connection = Util.getConnection(contentStore);
        try {
            Util.getBlob(connection, pid, datastreamId, datastreamVersionId)
                    .delete();
        } catch (Exception e) {
            logger.warn(Constants.ERR_DELETING_CONT + " " + Util.getDetails(
                    pid, datastreamId, datastreamVersionId), e);
        } finally {
            connection.close();
        }
    }

    // if newObject is null, all managed content will be deleted
    private void deleteOldManagedContent(FedoraObject oldObject,
            FedoraObject newObject) {
        for (Datastream datastream : oldObject.datastreams().values()) {
            if (datastream.controlGroup() == ControlGroup.MANAGED) {
                String datastreamId = datastream.id();
                for (DatastreamVersion datastreamVersion : datastream.versions()) {
                    String datastreamVersionId = datastreamVersion.id();
                    if (newObject == null ||
                            !Util.hasManagedDatastreamVersion(
                            newObject, datastreamId, datastreamVersionId)) {
                        deleteContent(oldObject.pid(), datastreamId,
                            datastreamVersionId);
                    }
                }
            }
        }
    }
}
