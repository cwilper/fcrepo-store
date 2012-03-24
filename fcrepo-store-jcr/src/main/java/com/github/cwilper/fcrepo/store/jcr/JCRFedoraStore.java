package com.github.cwilper.fcrepo.store.jcr;

import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.dto.core.io.DTOReader;
import com.github.cwilper.fcrepo.dto.core.io.DTOWriter;
import com.github.cwilper.fcrepo.store.core.ExistsException;
import com.github.cwilper.fcrepo.store.core.FedoraStore;
import com.github.cwilper.fcrepo.store.core.StoreException;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Binary;
import javax.jcr.Credentials;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * JCR-based {@link FedoraStore} implementation.
 */
public class JCRFedoraStore implements FedoraStore {
    private static final Logger logger =
            LoggerFactory.getLogger(JCRFedoraStore.class);

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
    public void addObject(FedoraObject object) {
        if (object == null) throw new NullPointerException();
        if (object.pid() == null) throw new IllegalArgumentException();
        Session session = Util.getSession(repository, credentials);
        try {
            String objectPath = getObjectPath(object.pid());
            if (session.nodeExists(objectPath)) {
                throw new ExistsException("Object already exists: "
                        + object.pid());
            }
            Node folder = mkdirs(session, objectPath);
            addFile(folder, "object", getBinaryValue(session, object));
            session.save();
        } catch (RepositoryException e) {
            throw new StoreException("Error adding object", e);
        } finally {
            session.logout();
        }
    }
    
    private String getObjectPath(String pid) {
        String hex = DigestUtils.md5Hex(pid);
        return "/" + hex.charAt(0) + hex.charAt(1) + "/" + hex.charAt(2) +
                hex.charAt(3) + "/" + pid.replaceFirst(":", "_");
    }
    
    private Binary getBinaryValue(Session session, FedoraObject object) {
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
    
    private Node mkdirs(Session session, String path)
            throws RepositoryException {
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
        return null;
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
    public Iterator<FedoraObject> iterator() {
        return null;
    }
}
