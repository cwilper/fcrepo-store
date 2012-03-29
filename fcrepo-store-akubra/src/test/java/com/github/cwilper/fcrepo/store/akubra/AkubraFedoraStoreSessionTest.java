package com.github.cwilper.fcrepo.store.akubra;

import com.github.cwilper.fcrepo.dto.core.ControlGroup;
import com.github.cwilper.fcrepo.dto.core.Datastream;
import com.github.cwilper.fcrepo.dto.core.DatastreamVersion;
import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.dto.core.io.DTOReader;
import com.github.cwilper.fcrepo.dto.core.io.DTOWriter;
import com.github.cwilper.fcrepo.dto.foxml.FOXMLReader;
import com.github.cwilper.fcrepo.dto.foxml.FOXMLWriter;
import com.github.cwilper.fcrepo.store.core.ExistsException;
import com.github.cwilper.fcrepo.store.core.NotFoundException;
import org.akubraproject.Blob;
import org.akubraproject.BlobStore;
import org.akubraproject.BlobStoreConnection;
import org.akubraproject.mem.MemBlobStore;
import org.apache.commons.io.IOUtils;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

/**
 * Unit tests for {@link AkubraFedoraStoreSession}.
 */
public class AkubraFedoraStoreSessionTest {
    private static final String EXISTING_PID = "test:existing";
    private static final String EXISTING_URI = "info:fedora/test:existing";
    private static final String DS1V0_URI = EXISTING_URI + "/DS1/DS1.0";
    private static final String DS2V0_URI = EXISTING_URI + "/DS2/DS2.0";

    private AkubraFedoraStoreSession fedoraSession;
    private BlobStore testObjectStore;
    private BlobStore testContentStore;
    
    @Before
    public void setUp() {
        testObjectStore = new MemBlobStore(URI.create("urn:objects"));
        testContentStore = new MemBlobStore(URI.create("urn:content"));
        fedoraSession = new AkubraFedoraStoreSession(testObjectStore,
                testContentStore, new FOXMLReader(), new FOXMLWriter());
    }
    
    @After
    public void tearDown() {
        fedoraSession.close();
    }
    
    @Test (expected=NullPointerException.class)
    public void initWithNullObjectStore() {
        new AkubraFedoraStoreSession(null,
                EasyMock.createMock(BlobStore.class),
                EasyMock.createMock(DTOReader.class),
                EasyMock.createMock(DTOWriter.class));
    }

    @Test (expected=NullPointerException.class)
    public void initWithNullContentStore() {
        new AkubraFedoraStoreSession(EasyMock.createMock(BlobStore.class),
                null,
                EasyMock.createMock(DTOReader.class),
                EasyMock.createMock(DTOWriter.class));
    }

    @Test (expected=NullPointerException.class)
    public void initWithNullReaderFactory() {
        new AkubraFedoraStoreSession(EasyMock.createMock(BlobStore.class),
                EasyMock.createMock(BlobStore.class),
                null,
                EasyMock.createMock(DTOWriter.class));
    }

    @Test (expected=NullPointerException.class)
    public void initWithNullWriterFactory() {
        new AkubraFedoraStoreSession(EasyMock.createMock(BlobStore.class),
                EasyMock.createMock(BlobStore.class),
                EasyMock.createMock(DTOReader.class),
                null);
    }

    @Test (expected=NullPointerException.class)
    public void addObjectNull() {
        fedoraSession.addObject(null);
    }

    @Test (expected=IllegalArgumentException.class)
    public void addObjectNoPid() {
        fedoraSession.addObject(new FedoraObject());
    }

    @Test (expected=ExistsException.class)
    public void addObjectExisting() throws Exception {
        BlobStoreConnection connection = null;
        try {
            connection = testObjectStore.openConnection(null, null);
            Blob blob = connection.getBlob(URI.create(EXISTING_URI), null);
            OutputStream out = blob.openOutputStream(1, false);
            out.write(0);
            out.close();
            Assert.assertTrue(blob.exists());
            fedoraSession.addObject(new FedoraObject().pid(EXISTING_PID));
        } finally {
            if (connection != null) connection.close();
        }
    }

    @Test
    public void addObjectNew() throws Exception {
        fedoraSession.addObject(new FedoraObject().pid("test:new-object"));
        BlobStoreConnection connection = null;
        try {
            connection = testObjectStore.openConnection(null, null);
            Blob blob = connection.getBlob(
                    URI.create("info:fedora/test:new-object"), null);
            Assert.assertTrue(blob.exists());
        } finally {
            if (connection != null) connection.close();
        }
    }
    
    @Test (expected=IllegalStateException.class)
    public void addObjectAfterClose() throws Exception {
        fedoraSession.close();
        fedoraSession.addObject(new FedoraObject().pid("test:new-object"));
    }

    @Test (expected=NullPointerException.class)
    public void getObjectNullPid() {
        fedoraSession.getObject(null);
    }

    @Test (expected=NotFoundException.class)
    public void getObjectNonExisting() {
        fedoraSession.getObject("test:non-existing");
    }

    @Test
    public void getObjectExistingTwice() {
        FedoraObject object = new FedoraObject().pid(EXISTING_PID);
        fedoraSession.addObject(object);
        Assert.assertEquals(object,
                fedoraSession.getObject(EXISTING_PID));
        Assert.assertEquals(object,
                fedoraSession.getObject(EXISTING_PID));
    }

    @Test (expected=IllegalStateException.class)
    public void getObjectAfterClose() throws Exception {
        fedoraSession.close();
        fedoraSession.getObject("test:non-existing");
    }
    
    @Test (expected=NullPointerException.class)
    public void updateObjectNull() {
        fedoraSession.updateObject(null);
    }
    
    @Test (expected=IllegalArgumentException.class)
    public void updateObjectNoPid() {
        fedoraSession.updateObject(new FedoraObject());
    }

    @Test (expected=NotFoundException.class)
    public void updateObjectNonExisting() {
        fedoraSession.updateObject(
                new FedoraObject().pid("test:non-existing"));
    }

    @Test
    public void updateObjectLabel() {
        FedoraObject object = new FedoraObject().pid(EXISTING_PID);
        fedoraSession.addObject(object);
        Assert.assertNull(fedoraSession.getObject(EXISTING_PID).label());
        fedoraSession.updateObject(
                new FedoraObject().pid(EXISTING_PID).label("label"));
        Assert.assertEquals("label",
                fedoraSession.getObject(EXISTING_PID).label());
    }

    @Test
    public void updateObjectDropExistingContent() throws Exception {
        Assert.assertTrue(blobExists(testContentStore, DS1V0_URI, true));
        Assert.assertTrue(blobExists(testContentStore, DS2V0_URI, true));
        addObjectWithDS1andDS2();
        updateObjectDropDS1();
        Assert.assertFalse(blobExists(testContentStore, DS1V0_URI, false));
        Assert.assertTrue(blobExists(testContentStore, DS2V0_URI, false));
    }

    @Test
    public void updateObjectDropNonExistingContent() throws Exception {
        Assert.assertFalse(blobExists(testContentStore, DS1V0_URI, false));
        Assert.assertTrue(blobExists(testContentStore, DS2V0_URI, true));
        addObjectWithDS1andDS2();
        updateObjectDropDS1();
        Assert.assertFalse(blobExists(testContentStore, DS1V0_URI, false));
        Assert.assertTrue(blobExists(testContentStore, DS2V0_URI, false));
    }

    @Test (expected=IllegalStateException.class)
    public void updateObjectAfterClose() throws Exception {
        fedoraSession.close();
        updateObjectDropDS1();
    }

    @Test (expected=NullPointerException.class)
    public void deleteObjectNullPid() {
        fedoraSession.deleteObject(null);
    }

    @Test (expected=NotFoundException.class)
    public void deleteObjectNonExisting() {
        fedoraSession.deleteObject("test:non-existing");
    }

    @Test
    public void deleteObjectWithContent() throws Exception {
        Assert.assertTrue(blobExists(testContentStore, DS1V0_URI, true));
        Assert.assertTrue(blobExists(testContentStore, DS2V0_URI, true));
        addObjectWithDS1andDS2();
        fedoraSession.deleteObject(EXISTING_PID);
        Assert.assertFalse(blobExists(testContentStore, DS1V0_URI, false));
        Assert.assertFalse(blobExists(testContentStore, DS2V0_URI, false));
    }

    @Test (expected=IllegalStateException.class)
    public void deleteObjectAfterClose() throws Exception {
        addObjectWithDS1andDS2();
        fedoraSession.close();
        fedoraSession.deleteObject(EXISTING_PID);
    }

    @Test
    public void listObjectsEmpty() {
        Assert.assertEquals(0, listObjects().size());
    }

    @Test
    public void listObjectsAfterAdd() {
        FedoraObject o1 = new FedoraObject().pid("test:o1");
        FedoraObject o2 = new FedoraObject().pid("test:o2");
        fedoraSession.addObject(o1);
        fedoraSession.addObject(o2);
        Set<FedoraObject> set = listObjects();
        Assert.assertTrue(set.contains(o1));
        Assert.assertTrue(set.contains(o2));
        Assert.assertEquals(2, set.size());
    }

    @Test
    public void listObjectsAfterDelete() {
        FedoraObject o1 = new FedoraObject().pid("test:o1");
        FedoraObject o2 = new FedoraObject().pid("test:o2");
        fedoraSession.addObject(o1);
        fedoraSession.addObject(o2);
        fedoraSession.deleteObject("test:o2");
        Set<FedoraObject> set = listObjects();
        Assert.assertTrue(set.contains(o1));
        Assert.assertEquals(1, set.size());
    }

    @Test (expected=IllegalStateException.class)
    public void listObjectsAfterClose() {
        fedoraSession.close();
        listObjects();
    }

    @Test (expected=NullPointerException.class)
    public void getContentNullPid() {
        fedoraSession.getContent(null, "DS1", "DS1.0");
    }

    @Test (expected=NullPointerException.class)
    public void getContentNullDatastreamId() {
        fedoraSession.getContent(EXISTING_PID, null, "DS1.0");
    }

    @Test (expected=NullPointerException.class)
    public void getContentNullDatastreamVersionId() {
        fedoraSession.getContent(EXISTING_PID, "DS1", null);
    }

    @Test (expected=NotFoundException.class)
    public void getContentObjectNotFound() {
        fedoraSession.getContent(EXISTING_PID, "DS1", "DS1.0");
    }

    @Test (expected=NotFoundException.class)
    public void getContentDatastreamNotFound() {
        fedoraSession.addObject(new FedoraObject().pid(EXISTING_PID));
        fedoraSession.getContent(EXISTING_PID, "DS1", "DS1.0");
    }

    @Test (expected=NotFoundException.class)
    public void getContentDatastreamExistsContentNotFound() {
        addObjectWithDS1(true);
        fedoraSession.getContent(EXISTING_PID, "DS1", "DS1.0");
    }

    @Test
    public void getContentDatastreamExistsContentExistsTwice() throws Exception {
        addObjectWithDS1(true);
        fedoraSession.setContent(EXISTING_PID, "DS1", "DS1.0",
                IOUtils.toInputStream("value"));
        Assert.assertEquals("value", IOUtils.toString(
                fedoraSession.getContent(EXISTING_PID, "DS1", "DS1.0")));
        Assert.assertEquals("value", IOUtils.toString(
                fedoraSession.getContent(EXISTING_PID, "DS1", "DS1.0")));
    }

    @Test (expected=IllegalStateException.class)
    public void getContentAfterClose() {
        fedoraSession.close();
        fedoraSession.getContent(EXISTING_PID, "DS1", "DS1.0");
    }

    @Test (expected=NullPointerException.class)
    public void getContentLengthNullPid() {
        fedoraSession.getContentLength(null, "DS1", "DS1.0");
    }

    @Test (expected=NullPointerException.class)
    public void getContentLengthNullDatastreamId() {
        fedoraSession.getContentLength(EXISTING_PID, null, "DS1.0");
    }

    @Test (expected=NullPointerException.class)
    public void getContentLengthNullDatastreamVersionId() {
        fedoraSession.getContentLength(EXISTING_PID, "DS1", null);
    }

    @Test (expected=NotFoundException.class)
    public void getContentLengthObjectNotFound() {
        fedoraSession.getContentLength(EXISTING_PID, "DS1", "DS1.0");
    }

    @Test (expected=NotFoundException.class)
    public void getContentLengthDatastreamNotFound() {
        fedoraSession.addObject(new FedoraObject().pid(EXISTING_PID));
        fedoraSession.getContentLength(EXISTING_PID, "DS1", "DS1.0");
    }

    @Test (expected=NotFoundException.class)
    public void getContentLengthDatastreamExistsContentNotFound() {
        addObjectWithDS1(true);
        fedoraSession.getContentLength(EXISTING_PID, "DS1", "DS1.0");
    }

    @Test
    public void getContentLengthDatastreamExistsContentFoundTwice() throws Exception {
        addObjectWithDS1(true);
        fedoraSession.setContent(EXISTING_PID, "DS1", "DS1.0",
                IOUtils.toInputStream("value"));
        Assert.assertEquals(5L, fedoraSession.getContentLength(EXISTING_PID,
                "DS1", "DS1.0"));
        Assert.assertEquals(5L, fedoraSession.getContentLength(EXISTING_PID,
                "DS1", "DS1.0"));
    }

    @Test (expected=IllegalStateException.class)
    public void getContentLengthAfterClose() {
        fedoraSession.close();
        fedoraSession.getContentLength(EXISTING_PID, "DS1", "DS1.0");
    }

    @Test (expected=NullPointerException.class)
    public void setContentNullPid() {
        fedoraSession.setContent(null, "DS1", "DS1.0",
                IOUtils.toInputStream("value"));
    }

    @Test (expected=NullPointerException.class)
    public void setContentNullDatastreamId() {
        fedoraSession.setContent(EXISTING_PID, null, "DS1.0",
                IOUtils.toInputStream("value"));
    }

    @Test (expected=NullPointerException.class)
    public void setContentNullDatastreamVersionId() {
        fedoraSession.setContent(EXISTING_PID, "DS1", null,
                IOUtils.toInputStream("value"));
    }

    @Test (expected=NullPointerException.class)
    public void setContentNullInputStream() {
        fedoraSession.setContent(EXISTING_PID, null, "DS1.0", null);
    }

    @Test (expected=NotFoundException.class)
    public void setContentObjectNotFound() {
        fedoraSession.setContent(EXISTING_PID, "DS1", "DS1.0",
                IOUtils.toInputStream("value"));
    }

    @Test (expected=NotFoundException.class)
    public void setContentDatastreamNotFound() {
        fedoraSession.addObject(new FedoraObject().pid(EXISTING_PID));
        fedoraSession.setContent(EXISTING_PID, "DS1", "DS1.0",
                IOUtils.toInputStream("value"));
    }

    @Test (expected=NotFoundException.class)
    public void setContentDatastreamNotManaged() {
        addObjectWithDS1(false);
        fedoraSession.setContent(EXISTING_PID, "DS1", "DS1.0",
                IOUtils.toInputStream("value"));
    }

    @Test
    public void setContentDatastreamManagedTwice() throws Exception {
        addObjectWithDS1(true);
        fedoraSession.setContent(EXISTING_PID, "DS1", "DS1.0",
                IOUtils.toInputStream("value1"));
        Assert.assertEquals("value1", IOUtils.toString(
                fedoraSession.getContent(EXISTING_PID, "DS1", "DS1.0")));
        fedoraSession.setContent(EXISTING_PID, "DS1", "DS1.0",
                IOUtils.toInputStream("value2"));
        Assert.assertEquals("value2", IOUtils.toString(
                fedoraSession.getContent(EXISTING_PID, "DS1", "DS1.0")));
    }

    @Test (expected=IllegalStateException.class)
    public void setContentAfterClose() throws Exception {
        fedoraSession.close();
        fedoraSession.setContent(EXISTING_PID, "DS1", "DS1.0",
                IOUtils.toInputStream("value1"));
    }
    
    @Test (expected=UnsupportedOperationException.class)
    public void getXAResource() {
        fedoraSession.getXAResource();
    }

    private Set<FedoraObject> listObjects() {
        Set<FedoraObject> set = new HashSet<FedoraObject>();
        for (FedoraObject object : fedoraSession) {
            set.add(object);
        }
        return set;
    }
    
    private Datastream getManagedDatastreamWithOneVersion(String id) {
        Datastream ds = new Datastream(id).controlGroup(ControlGroup.MANAGED);
        ds.versions().add(new DatastreamVersion(id + ".0", null));
        return ds;
    }

    private void addObjectWithDS1(boolean managed) {
        Datastream ds = new Datastream("DS1");
        if (managed) {
            ds.controlGroup(ControlGroup.MANAGED);
        } else {
            ds.controlGroup(ControlGroup.EXTERNAL);
        }
        ds.versions().add(new DatastreamVersion("DS1.0", null));
        FedoraObject object = new FedoraObject()
                .pid(EXISTING_PID)
                .putDatastream(ds);
        fedoraSession.addObject(object);
    }

    private void addObjectWithDS1andDS2() {
        FedoraObject object = new FedoraObject()
                .pid(EXISTING_PID)
                .putDatastream(getManagedDatastreamWithOneVersion("DS1"))
                .putDatastream(getManagedDatastreamWithOneVersion("DS2"));
        fedoraSession.addObject(object);
    }

    private void updateObjectDropDS1() throws Exception {
        Assert.assertEquals(2, fedoraSession.getObject(EXISTING_PID)
                .datastreams().size());
        fedoraSession.updateObject(new FedoraObject()
                .pid(EXISTING_PID)
                .putDatastream(getManagedDatastreamWithOneVersion("DS2")));
        Assert.assertEquals(1, fedoraSession.getObject(EXISTING_PID)
                .datastreams().size());
    }

    private boolean blobExists(BlobStore blobStore, String id, boolean create)
            throws IOException {
        BlobStoreConnection connection = null;
        try {
            connection = blobStore.openConnection(null, null);
            Blob blob = connection.getBlob(URI.create(id), null);
            if (create) {
                OutputStream out = blob.openOutputStream(1, false);
                out.write(0);
                out.close();
            }
            return blob.exists();
        } finally {
            if (connection != null) connection.close();
        }
    }
}
