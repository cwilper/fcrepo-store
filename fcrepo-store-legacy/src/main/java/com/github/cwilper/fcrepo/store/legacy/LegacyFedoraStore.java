package com.github.cwilper.fcrepo.store.legacy;

import com.github.cwilper.fcrepo.dto.core.ControlGroup;
import com.github.cwilper.fcrepo.dto.core.Datastream;
import com.github.cwilper.fcrepo.dto.core.DatastreamVersion;
import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.dto.core.io.DTOReader;
import com.github.cwilper.fcrepo.dto.core.io.DTOWriter;
import com.github.cwilper.fcrepo.store.core.impl.CommonConstants;
import com.github.cwilper.fcrepo.store.core.ExistsException;
import com.github.cwilper.fcrepo.store.core.FedoraStore;
import com.github.cwilper.fcrepo.store.core.NotFoundException;
import com.github.cwilper.fcrepo.store.core.StoreException;
import com.google.common.collect.AbstractIterator;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

/**
 * Legacy {@link FedoraStore} implementation compatible with pre-Akubra
 * versions of Fedora.
 */
public class LegacyFedoraStore implements FedoraStore {
    private static final Logger logger =
            LoggerFactory.getLogger(LegacyFedoraStore.class);

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
    public void addObject(FedoraObject object) {
        if (object == null) throw new NullPointerException();
        if (object.pid() == null) throw new IllegalArgumentException();
        String path = objectStore.getPath(object.pid());
        if (path != null) throw new ExistsException(object.pid());
        path = objectStore.generatePath(object.pid());
        objectStore.setPath(object.pid(), path);
        boolean success = false;
        try {
            Util.writeObject(writerFactory, object,
                    objectStore.getFileOutputStream(path));
            success = true;
        } catch (IOException e) {
            throw new StoreException(CommonConstants.ERR_ADDING_OBJ, e);
        } finally {
            if (!success) {
                objectStore.setPath(object.pid(), null);
            }
        }
    }

    @Override
    public FedoraObject getObject(String pid) {
        if (pid == null) throw new NullPointerException();
        try {
            String path = objectStore.getPath(pid);
            if (path == null) throw new NotFoundException(
                    CommonConstants.ERR_NOTFOUND_OBJ_IN_STORAGE + ": " + pid);
            return Util.readObject(readerFactory,
                    objectStore.getFileInputStream(path));
        } catch (IOException e) {
            throw new StoreException(CommonConstants.ERR_GETTING_OBJ, e);
        }
    }

    @Override
    public void updateObject(FedoraObject object) {
        if (object == null) throw new NullPointerException();
        if (object.pid() == null) throw new IllegalArgumentException();
        try {
            String path = objectStore.getPath(object.pid());
            if (path == null) throw new NotFoundException(
                    CommonConstants.ERR_NOTFOUND_OBJ_IN_STORAGE + ": "
                    + object.pid());
            deleteOldManagedContent(Util.readObject(readerFactory,
                    objectStore.getFileInputStream(path)), object);
            Util.writeObject(writerFactory, object,
                    objectStore.getFileOutputStream(path));
        } catch (IOException e) {
            throw new StoreException(CommonConstants.ERR_UPDATING_OBJ, e);
        }
    }

    @Override
    public void deleteObject(String pid) {
        if (pid == null) throw new NullPointerException();
        try {
            String path = objectStore.getPath(pid);
            if (path == null) throw new NotFoundException(
                    CommonConstants.ERR_NOTFOUND_OBJ_IN_STORAGE + ": " + pid);
            deleteOldManagedContent(
                    Util.readObject(readerFactory,
                            objectStore.getFileInputStream(path)), null);
            objectStore.deleteFile(path);
            objectStore.setPath(pid, null);
        } catch (IOException e) {
            throw new StoreException(CommonConstants.ERR_DELETING_OBJ, e);
        }
    }

    @Override
    public InputStream getContent(String pid, String datastreamId,
            String datastreamVersionId) {
        String path = getContentPath(
                pid, datastreamId, datastreamVersionId, true);
        return contentStore.getFileInputStream(path);
    }

    @Override
    public long getContentLength(String pid, String datastreamId,
            String datastreamVersionId) {
        String path = getContentPath(
                pid, datastreamId, datastreamVersionId, true);
        return contentStore.getFileSize(path);
    }

    @Override
    public void setContent(String pid, String datastreamId,
            String datastreamVersionId, InputStream inputStream) {
        if (inputStream == null) throw new NullPointerException();
        OutputStream outputStream = null;
        boolean success = false;
        try {
            String path = getContentPath(
                    pid, datastreamId, datastreamVersionId, false);
            outputStream = contentStore.getFileOutputStream(path);
            IOUtils.copyLarge(inputStream, outputStream);
            inputStream.close();
            outputStream.close();
            success = true;
        } catch (IOException e) {
            throw new StoreException(CommonConstants.ERR_SETTING_CONT, e);
        } finally {
            if (!success) {
                Util.closeOrWarn(inputStream);
                Util.closeOrWarn(outputStream);
            }
        }
    }

    @Override
    public Iterator<FedoraObject> iterator() {
        final Iterator<String> paths = objectStore.iterator();
        return new AbstractIterator<FedoraObject>() {
            @Override
            protected FedoraObject computeNext() {
                while (paths.hasNext()) {
                    String path = paths.next();
                    try {
                        return Util.readObject(readerFactory,
                                objectStore.getFileInputStream(path));
                    } catch (IOException e) {
                        logger.warn(CommonConstants.ERR_PARSING_OBJ + ": "
                                + path, e);
                    }
                }
                return endOfData();
            }
        };
    }

    private String getContentPath(String pid, String datastreamId,
            String datastreamVersionId, boolean mustExist) {
        if (pid == null  || datastreamId == null ||
                datastreamVersionId == null) throw new NullPointerException();
        FedoraObject object = getObject(pid);
        if (!Util.hasManagedDatastreamVersion(object, datastreamId,
                datastreamVersionId)) {
            throw new NotFoundException(CommonConstants.ERR_NOTFOUND_DS_IN_OBJ + " "
                    + Util.getDetails(pid, datastreamId, datastreamVersionId));
        }
        String id = Util.getId(pid, datastreamId, datastreamVersionId);
        String path = contentStore.getPath(id);
        if (path == null) {
            if (mustExist) {
                throw new NotFoundException(
                        CommonConstants.ERR_NOTFOUND_DS_IN_STORAGE + " "
                        + Util.getDetails(pid, datastreamId,
                        datastreamVersionId));
            } else {
                path = contentStore.generatePath(id);
                contentStore.setPath(id, path);
            }
        }
        return path;
    }

    // just log a warning message in the event of failure
    private void deleteContent(String pid, String datastreamId,
            String datastreamVersionId) {
        String id = Util.getId(pid, datastreamId, datastreamVersionId);
        String path = contentStore.getPath(id);
        if (path == null) {
            logger.warn(CommonConstants.ERR_DELETING_CONT + " " + Util.getDetails(
                    pid, datastreamId, datastreamVersionId)
                    + ": No such datastream in registry");
        }
        try {
            contentStore.deleteFile(path);
            contentStore.setPath(id, null);
        } catch (Exception e) {
            logger.warn(CommonConstants.ERR_DELETING_CONT + " " + Util.getDetails(
                    pid, datastreamId, datastreamVersionId), e);
        }
    }

    // if newObject is null, all managed content will be deleted
    private void deleteOldManagedContent(FedoraObject oldObject,
            FedoraObject newObject) {
        for (Datastream datastream : oldObject.datastreams().values()) {
            if (datastream.controlGroup() == ControlGroup.MANAGED) {
                String datastreamId = datastream.id();
                for (DatastreamVersion datastreamVersion :
                        datastream.versions()) {
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
