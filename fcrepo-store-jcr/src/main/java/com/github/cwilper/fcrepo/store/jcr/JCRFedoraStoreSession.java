package com.github.cwilper.fcrepo.store.jcr;

import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.dto.core.io.DTOReader;
import com.github.cwilper.fcrepo.dto.core.io.DTOWriter;
import com.github.cwilper.fcrepo.store.core.ExistsException;
import com.github.cwilper.fcrepo.store.core.FedoraStoreSession;
import com.github.cwilper.fcrepo.store.core.NotFoundException;
import com.github.cwilper.fcrepo.store.core.StoreException;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * JCR-based {@link com.github.cwilper.fcrepo.store.core.FedoraStoreSession} implementation.
 */
public class JCRFedoraStoreSession implements FedoraStoreSession {
    private static final Logger logger =
            LoggerFactory.getLogger(JCRFedoraStoreSession.class);

    private final Session session;
    private final DTOReader readerFactory;
    private final DTOWriter writerFactory;

    private boolean closed;

    public JCRFedoraStoreSession(Session session, DTOReader readerFactory,
            DTOWriter writerFactory) {
        if (session == null || readerFactory == null || writerFactory == null)
            throw new NullPointerException();
        this.session = session;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.closed = false;
    }

    @Override
    public void addObject(FedoraObject object) {
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

    @Override
    public FedoraObject getObject(String pid) {
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
    }

    @Override
    public void deleteObject(String pid) {
    }

    @Override
    public InputStream getContent(String pid, String datastreamId,
            String datastreamVersionId) {
        return null;
    }

    @Override
    public long getContentLength(String pid, String datastreamId,
            String datastreamVersionId) {
        return 0;
    }

    @Override
    public void setContent(String pid, String datastreamId,
            String datastreamVersionId, InputStream inputStream) {
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
        return null;
    }
}
