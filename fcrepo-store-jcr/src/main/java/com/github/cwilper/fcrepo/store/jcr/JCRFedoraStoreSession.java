package com.github.cwilper.fcrepo.store.jcr;

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
import com.github.cwilper.fcrepo.store.core.impl.CommonConstants;
import com.google.common.collect.AbstractIterator;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.transaction.xa.XAResource;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * JCR-based {@link FedoraStoreSession} implementation. Supports transactions
 * if the underlying JCR Session is also an <code>XAResource</code>.
 */
class JCRFedoraStoreSession implements FedoraStoreSession {
    private static final Logger logger =
            LoggerFactory.getLogger(JCRFedoraStoreSession.class);

    private final Session session;
    private final DTOReader readerFactory;
    private final DTOWriter writerFactory;

    private boolean closed;

    JCRFedoraStoreSession(Session session, DTOReader readerFactory,
            DTOWriter writerFactory) {
        if (session == null || readerFactory == null || writerFactory == null)
            throw new NullPointerException();
        this.session = session;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.closed = false;
    }

    @Override
    public XAResource getXAResource() {
        if (session instanceof XAResource) {
            return (XAResource) session;
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void addObject(FedoraObject object) {
        ensureNotClosed();
        if (object == null) throw new NullPointerException();
        if (object.pid() == null) throw new IllegalArgumentException();
        try {
            String objectPath = getObjectPath(object.pid());
            if (session.nodeExists(objectPath)) {
                throw new ExistsException("Object already exists: "
                        + object.pid());
            }
            Node folder = mkdirs(objectPath);
            addFile(folder, "object", getBinaryValue(object));
            session.save();
        } catch (RepositoryException e) {
            throw new StoreException("Error adding object", e);
        }
    }

    @Override
    public FedoraObject getObject(String pid) {
        ensureNotClosed();
        if (pid == null) throw new NullPointerException();
        try {
            String objectPath = getObjectPath(pid);
            if (session.nodeExists(objectPath)) {
                Node content = session.getNode(objectPath +
                        "/object/jcr:content");
                Binary binary = content.getProperty("jcr:data").getBinary();
                DTOReader reader = readerFactory.getInstance();
                try {
                    return reader.readObject(binary.getStream());
                } catch (IOException e) {
                    throw new StoreException("Error reading object", e);
                } finally {
                    reader.close();
                }
            } else {
                throw new NotFoundException("No such object: " + pid);
            }
        } catch (RepositoryException e) {
            throw new StoreException("Error getting object", e);
        }
        
    }

    @Override
    public void updateObject(FedoraObject object) {
        ensureNotClosed();
        if (object == null) throw new NullPointerException();
        if (object.pid() == null) throw new IllegalArgumentException();
        try {
            String objectPath = getObjectPath(object.pid());
            if (session.nodeExists(objectPath)) {
                deleteOldManagedContent(getObject(object.pid()), object);
                Node content = session.getNode(objectPath +
                        "/object/jcr:content");
                content.getProperty("jcr:data").setValue(
                        getBinaryValue(object));
                session.save();
            } else {
                throw new NotFoundException("No such object: " + object.pid());
            }
        } catch (RepositoryException e) {
            throw new StoreException("Error getting object", e);
        }
    }

    @Override
    public void deleteObject(String pid) {
        ensureNotClosed();
        if (pid == null) throw new NullPointerException();
        try {
            session.removeItem(getObjectPath(pid));
            session.save();
        } catch (PathNotFoundException e) {
            throw new NotFoundException(
                    CommonConstants.ERR_NOTFOUND_OBJ_IN_STORAGE + ": " + pid);
        } catch (RepositoryException e) {
            throw new StoreException(CommonConstants.ERR_DELETING_OBJ, e);
        }
    }

    @Override
    public InputStream getContent(String pid, String datastreamId,
            String datastreamVersionId) {
        ensureNotClosed();
        try {
            return getContentNode(pid, datastreamId, datastreamVersionId)
                    .getProperty("jcr:data").getBinary().getStream();
        } catch (RepositoryException e) {
            throw new StoreException("Error getting content for " +
                    Util.getDetails(pid, datastreamId, datastreamVersionId));
        }
    }

    @Override
    public long getContentLength(String pid, String datastreamId,
            String datastreamVersionId) {
        ensureNotClosed();
        try {
            return getContentNode(pid, datastreamId, datastreamVersionId)
                    .getProperty("jcr:data").getBinary().getSize();
        } catch (RepositoryException e) {
            throw new StoreException("Error getting content for " +
                    Util.getDetails(pid, datastreamId, datastreamVersionId));
        }
    }

    @Override
    public void setContent(String pid, String datastreamId,
            String datastreamVersionId, InputStream inputStream) {
        ensureNotClosed();
        if (pid == null || datastreamId == null ||
                datastreamVersionId == null || inputStream == null)
            throw new NullPointerException();
        boolean success = false;
        try {
            FedoraObject object = getObject(pid);
            if (!Util.hasManagedDatastreamVersion(object, datastreamId,
                    datastreamVersionId)) {
                throw new NotFoundException(
                        CommonConstants.ERR_NOTFOUND_DS_IN_OBJ + " "
                        + Util.getDetails(pid, datastreamId,
                        datastreamVersionId));
            }
            String dsPath = getObjectPath(pid) + "/" + datastreamId;
            Node dsNode;
            if (session.nodeExists(dsPath)) {
                dsNode = session.getNode(dsPath);
            } else {
                dsNode = mkdirs(dsPath);
            }
            String dsvPath = dsPath + "/" + datastreamVersionId;
            Binary value = session.getValueFactory().createBinary(inputStream);
            if (session.nodeExists(dsvPath)) {
                Node content = session.getNode(dsvPath + "/jcr:content");
                content.getProperty("jcr:data").setValue(value);
            } else {
                addFile(dsNode, datastreamVersionId, value);
            }
            session.save();
            inputStream.close();
            success = true;
        } catch (IOException e) {
            throw new StoreException(CommonConstants.ERR_SETTING_CONT, e);
        } catch (RepositoryException e) {
            throw new StoreException(CommonConstants.ERR_SETTING_CONT, e);
        } finally {
            if (!success) {
                Util.closeOrWarn(inputStream);
            }
        }
    }

    @Override
    public void close() {
        if (!closed) {
            session.logout();
            closed = true;
        }
    }

    @Override
    public Iterator<FedoraObject> iterator() {
        ensureNotClosed();
        try {
            final Iterator<Node> objectNodes =
                    new ObjectNodeIterator(session.getRootNode());
            return new AbstractIterator<FedoraObject>() {
                @Override
                protected FedoraObject computeNext() {
                    while (objectNodes.hasNext()) {
                        Node node = objectNodes.next();
                        try {
                            Node content = node.getNode("object")
                                    .getNode("jcr:content");
                            Binary binary = content.getProperty("jcr:data")
                                    .getBinary();
                            return Util.readObject(readerFactory,
                                    binary.getStream());
                        } catch (IOException e) {
                            logger.warn(CommonConstants.ERR_PARSING_OBJ + " "
                                    + node);
                        } catch (RepositoryException e) {
                            logger.warn("Error opening object node; ignoring "
                                    + node);
                        }
                    }
                    return endOfData();
                }
            };
        } catch (RepositoryException e) {
            throw new StoreException("Error iterating top-level directories",
                    e);
        }
    }

    private Node getContentNode(String pid, String dsId, String dsvId) 
            throws RepositoryException {
        if (pid == null || dsId == null || dsvId == null) {
            throw new NullPointerException();
        }
        try {
            return session.getNode(getObjectPath(pid) + "/" + dsId + "/" +
                    dsvId + "/jcr:content");
        } catch (PathNotFoundException e) {
            throw new NotFoundException("Datastream content not found " +
                    Util.getDetails(pid, dsId, dsvId));
        }
    }

    private String getObjectPath(String pid) {
        String hex = DigestUtils.md5Hex(pid);
        return "/" + hex.charAt(0) + hex.charAt(1) + "/" + hex.charAt(2) +
                hex.charAt(3) + "/" + pid.replaceFirst(":", "_");
    }

    private Binary getBinaryValue(FedoraObject object) {
        DTOWriter writer = writerFactory.getInstance();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            writer.writeObject(object, out);
            out.close();
            return session.getValueFactory().createBinary(
                    new ByteArrayInputStream(out.toByteArray()));
        } catch (RepositoryException e) {
            throw new StoreException("Error getting binary value", e);
        } catch (IOException e) {
            throw new StoreException("Error serializing object", e);
        } finally {
            writer.close();
        }
    }

    private void addFile(Node parent, String name, Binary value)
            throws RepositoryException {
        Node file = parent.addNode(name, "nt:file");
        Node resource = file.addNode("jcr:content", "nt:resource");
        resource.setProperty("jcr:mimeType", "application/octet-stream");
        resource.setProperty("jcr:data", value);
    }

    private Node mkdirs(String path) throws RepositoryException {
        try {
            Node node = session.getRootNode();
            for (String childName : path.split("/")) {
                if (childName.length() > 0) {
                    if (!node.hasNode(childName)) {
                        node = node.addNode(childName, "nt:folder");
                    } else {
                        node = node.getNode(childName);
                    }
                }
            }
            return node;
        } catch (RepositoryException e) {
            throw new StoreException("Error creating folder: ", e);
        }
    }

    // just log a warning message in the event of failure
    private void deleteContent(String pid, String dsId, String dsvId) {
        String path = getObjectPath(pid) + "/" + dsId + "/" + dsvId;
        try {
            session.removeItem(path);
        } catch (Exception e) {
            logger.warn(CommonConstants.ERR_DELETING_CONT
                    + " " + Util.getDetails(pid, dsId, dsvId), e);
        }
    }


    // if newObject is null, all managed content will be deleted
    private void deleteOldManagedContent(FedoraObject oldObject,
            FedoraObject newObject) {
        for (Datastream ds : oldObject.datastreams().values()) {
            if (ds.controlGroup() == ControlGroup.MANAGED) {
                String dsId = ds.id();
                for (DatastreamVersion dsv : ds.versions()) {
                    String dsvId = dsv.id();
                    if (newObject == null ||
                            !Util.hasManagedDatastreamVersion(
                                    newObject, dsId, dsvId)) {
                        deleteContent(oldObject.pid(), dsId, dsvId);
                    }
                }
            }
        }
    }

    private void ensureNotClosed() {
        if (closed) throw new IllegalStateException("Session is closed");
    }
    
    boolean closed() {
        return closed;
    }
}
