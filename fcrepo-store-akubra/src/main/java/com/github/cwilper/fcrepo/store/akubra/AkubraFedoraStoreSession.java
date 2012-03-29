package com.github.cwilper.fcrepo.store.akubra;

import com.github.cwilper.fcrepo.dto.core.ControlGroup;
import com.github.cwilper.fcrepo.dto.core.Datastream;
import com.github.cwilper.fcrepo.dto.core.DatastreamVersion;
import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.dto.core.io.DTOReader;
import com.github.cwilper.fcrepo.dto.core.io.DTOWriter;
import com.github.cwilper.fcrepo.store.core.ExistsException;
import com.github.cwilper.fcrepo.store.core.FedoraStoreSession;
import com.github.cwilper.fcrepo.store.core.NotFoundException;
import com.github.cwilper.fcrepo.store.core.StoreException;
import org.akubraproject.Blob;
import org.akubraproject.BlobStore;
import org.akubraproject.BlobStoreConnection;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.XAResource;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

/**
 * Akubra-based {@link FedoraStoreSession} implementation.
 */
class AkubraFedoraStoreSession implements FedoraStoreSession {
    private static final Logger logger =
            LoggerFactory.getLogger(AkubraFedoraStoreSession.class);

    private final BlobStore objectStore;
    private final BlobStore contentStore;
    private final DTOReader readerFactory;
    private final DTOWriter writerFactory;

    private BlobStoreConnection objectStoreConnection;
    private BlobStoreConnection contentStoreConnection;

    private boolean closed;

    AkubraFedoraStoreSession(BlobStore objectStore, BlobStore contentStore,
            DTOReader readerFactory, DTOWriter writerFactory) {
        if (objectStore == null || contentStore == null
                || readerFactory == null || writerFactory == null) {
            throw new NullPointerException();
        }
        this.objectStore = objectStore;
        this.contentStore = contentStore;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.closed = false;
    }

    @Override
    public XAResource getXAResource() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addObject(FedoraObject object) {
        ensureNotClosed();
        if (object == null) throw new NullPointerException();
        if (object.pid() == null) throw new IllegalArgumentException();
        try {
            Blob blob = Util.getBlob(getObjectStoreConnection(), object.pid());
            if (blob.exists()) throw new ExistsException(object.pid());
            Util.writeObject(writerFactory, object, blob);
        } catch (IOException e) {
            throw new StoreException(Constants.ERR_ADDING_OBJ, e);
        }
    }

    @Override
    public FedoraObject getObject(String pid) {
        ensureNotClosed();
        if (pid == null) throw new NullPointerException();
        try {
            Blob blob = Util.getBlob(getObjectStoreConnection(), pid);
            if (!blob.exists()) throw new NotFoundException(
                    Constants.ERR_NOTFOUND_OBJ_IN_STORAGE + ": " + pid);
            return Util.readObject(readerFactory, blob);
        } catch (IOException e) {
            throw new StoreException(Constants.ERR_GETTING_OBJ, e);
        }
    }

    @Override
    public void updateObject(FedoraObject object) {
        ensureNotClosed();
        if (object == null) throw new NullPointerException();
        if (object.pid() == null) throw new IllegalArgumentException();
        try {
            Blob blob = Util.getBlob(getObjectStoreConnection(), object.pid());
            if (!blob.exists()) throw new NotFoundException(
                    Constants.ERR_NOTFOUND_OBJ_IN_STORAGE + ": "
                    + object.pid());
            deleteOldManagedContent(
                    Util.readObject(readerFactory, blob), object);
            Util.writeObject(writerFactory, object, blob);
        } catch (IOException e) {
            throw new StoreException(Constants.ERR_UPDATING_OBJ, e);
        }
    }

    @Override
    public void deleteObject(String pid) {
        ensureNotClosed();
        if (pid == null) throw new NullPointerException();
        try {
            Blob blob = Util.getBlob(getObjectStoreConnection(), pid);
            if (!blob.exists()) throw new NotFoundException(
                    Constants.ERR_NOTFOUND_OBJ_IN_STORAGE + ": " + pid);
            deleteOldManagedContent(
                    Util.readObject(readerFactory, blob), null);
            blob.delete();
        } catch (IOException e) {
            throw new StoreException(Constants.ERR_DELETING_OBJ, e);
        }
    }

    @Override
    public InputStream getContent(String pid, String datastreamId,
            String datastreamVersionId) {
        ensureNotClosed();
        Blob blob = getContentBlob(
                pid, datastreamId, datastreamVersionId, true);
        boolean success = false;
        try {
            return blob.openInputStream();
        } catch (IOException e) {
            throw new StoreException(Constants.ERR_GETTING_CONT, e);
        }
    }

    @Override
    public long getContentLength(String pid, String datastreamId,
            String datastreamVersionId) {
        ensureNotClosed();
        Blob blob = getContentBlob(
                pid, datastreamId, datastreamVersionId, true);
        try {
            return blob.getSize();
        } catch (IOException e) {
            throw new StoreException(Constants.ERR_GETTING_CONT_LEN, e);
        }
    }

    @Override
    public void setContent(String pid, String datastreamId,
            String datastreamVersionId, InputStream inputStream) {
        ensureNotClosed();
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
    public void close() {
        if (!closed) {
            try {
                if (objectStoreConnection != null) {
                    objectStoreConnection.close();
                }
                if (objectStoreConnection != null) {
                    objectStoreConnection.close();
                }
            } finally {
                closed = true;
            }
        }
    }

    @Override
    public Iterator<FedoraObject> iterator() {
        ensureNotClosed();
        BlobStoreConnection connection = getObjectStoreConnection();
        try {
            return new AkubraObjectIterator(connection,
                    connection.listBlobIds(null),
                    readerFactory);
        } catch (IOException e) {
            throw new StoreException(Constants.ERR_LISTING_OBJS, e);
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
        try {
            Blob blob = Util.getBlob(getContentStoreConnection(),
                    pid, datastreamId, datastreamVersionId);
            if (mustExist && !blob.exists()) {
                throw new NotFoundException(
                        Constants.ERR_NOTFOUND_DS_IN_STORAGE + " "
                        + Util.getDetails(pid, datastreamId,
                        datastreamVersionId));
            }
            return blob;
        } catch (IOException e) {
            throw new StoreException(Constants.ERR_GETTING_CONT, e);
        }
    }

    // just log a warning message in the event of failure
    private void deleteContent(String pid, String datastreamId,
            String datastreamVersionId) {
        try {
            Util.getBlob(getContentStoreConnection(), pid, datastreamId,
                    datastreamVersionId).delete();
        } catch (Exception e) {
            logger.warn(Constants.ERR_DELETING_CONT + " " + Util.getDetails(
                    pid, datastreamId, datastreamVersionId), e);
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

    private void ensureNotClosed() {
        if (closed) throw new IllegalStateException("Session is closed");
    }

    private BlobStoreConnection getObjectStoreConnection() {
        if (objectStoreConnection == null) {
            objectStoreConnection = Util.getConnection(objectStore);
        }
        return objectStoreConnection;
    }

    private BlobStoreConnection getContentStoreConnection() {
        if (contentStoreConnection == null) {
            contentStoreConnection = Util.getConnection(contentStore);
        }
        return contentStoreConnection;
    }
}
