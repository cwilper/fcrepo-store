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

    private AkubraFedoraStoreSession testSession;
    private BlobStore testObjectStore;
    private BlobStore testContentStore;
    
    @Before
    public void setUp() {
        testObjectStore = new MemBlobStore(URI.create("urn:objects"));
        testContentStore = new MemBlobStore(URI.create("urn:content"));
        testSession = new AkubraFedoraStoreSession(testObjectStore,
                testContentStore, new FOXMLReader(), new FOXMLWriter());
    }
    
    @After
    public void tearDown() {
        testSession.close();
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
        testSession.addObject(null);
    }

    @Test (expected=IllegalArgumentException.class)
    public void addObjectNoPid() {
        testSession.addObject(new FedoraObject());
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
            testSession.addObject(new FedoraObject().pid(EXISTING_PID));
        } finally {
            if (connection != null) connection.close();
        }
    }

    @Test
    public void addObjectNew() throws Exception {
        testSession.addObject(new FedoraObject().pid("test:new-object"));
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
        testSession.close();
        testSession.addObject(new FedoraObject().pid("test:new-object"));
    }

    @Test (expected=NullPointerException.class)
    public void getObjectNullPid() {
        testSession.getObject(null);
    }

    @Test (expected=NotFoundException.class)
    public void getObjectNonExisting() {
        testSession.getObject("test:non-existing");
    }

    @Test
    public void getObjectExistingTwice() {
        FedoraObject object = new FedoraObject().pid(EXISTING_PID);
        testSession.addObject(object);
        Assert.assertEquals(object,
                testSession.getObject(EXISTING_PID));
        Assert.assertEquals(object,
                testSession.getObject(EXISTING_PID));
    }

    @Test (expected=IllegalStateException.class)
    public void getObjectAfterClose() throws Exception {
        testSession.close();
        testSession.getObject("test:non-existing");
    }
    
    @Test (expected=NullPointerException.class)
    public void updateObjectNull() {
        testSession.updateObject(null);
    }
    
    @Test (expected=IllegalArgumentException.class)
    public void updateObjectNoPid() {
        testSession.updateObject(new FedoraObject());
    }

    @Test (expected=NotFoundException.class)
    public void updateObjectNonExisting() {
        testSession.updateObject(
                new FedoraObject().pid("test:non-existing"));
    }

    @Test
    public void updateObjectLabel() {
        FedoraObject object = new FedoraObject().pid(EXISTING_PID);
        testSession.addObject(object);
        Assert.assertNull(testSession.getObject(EXISTING_PID).label());
        testSession.updateObject(
                new FedoraObject().pid(EXISTING_PID).label("label"));
        Assert.assertEquals("label",
                testSession.getObject(EXISTING_PID).label());
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
        testSession.close();
        updateObjectDropDS1();
    }

    @Test (expected=NullPointerException.class)
    public void deleteObjectNullPid() {
        testSession.deleteObject(null);
    }

    @Test (expected=NotFoundException.class)
    public void deleteObjectNonExisting() {
        testSession.deleteObject("test:non-existing");
    }

    @Test
    public void deleteObjectWithContent() throws Exception {
        Assert.assertTrue(blobExists(testContentStore, DS1V0_URI, true));
        Assert.assertTrue(blobExists(testContentStore, DS2V0_URI, true));
        addObjectWithDS1andDS2();
        testSession.deleteObject(EXISTING_PID);
        Assert.assertFalse(blobExists(testContentStore, DS1V0_URI, false));
        Assert.assertFalse(blobExists(testContentStore, DS2V0_URI, false));
    }

    @Test (expected=IllegalStateException.class)
    public void deleteObjectAfterClose() throws Exception {
        addObjectWithDS1andDS2();
        testSession.close();
        testSession.deleteObject(EXISTING_PID);
    }

    @Test
    public void listObjectsEmpty() {
        Assert.assertEquals(0, listObjects().size());
    }

    @Test
    public void listObjectsAfterAdd() {
        FedoraObject o1 = new FedoraObject().pid("test:o1");
        FedoraObject o2 = new FedoraObject().pid("test:o2");
        testSession.addObject(o1);
        testSession.addObject(o2);
        Set<FedoraObject> set = listObjects();
        Assert.assertTrue(set.contains(o1));
        Assert.assertTrue(set.contains(o2));
        Assert.assertEquals(2, set.size());
    }

    @Test
    public void listObjectsAfterDelete() {
        FedoraObject o1 = new FedoraObject().pid("test:o1");
        FedoraObject o2 = new FedoraObject().pid("test:o2");
        testSession.addObject(o1);
        testSession.addObject(o2);
        testSession.deleteObject("test:o2");
        Set<FedoraObject> set = listObjects();
        Assert.assertTrue(set.contains(o1));
        Assert.assertEquals(1, set.size());
    }

    @Test (expected=IllegalStateException.class)
    public void listObjectsAfterClose() {
        testSession.close();
        listObjects();
    }

    @Test (expected=NullPointerException.class)
    public void getContentNullPid() {
        testSession.getContent(null, "DS1", "DS1.0");
    }

    @Test (expected=NullPointerException.class)
    public void getContentNullDatastreamId() {
        testSession.getContent(EXISTING_PID, null, "DS1.0");
    }

    @Test (expected=NullPointerException.class)
    public void getContentNullDatastreamVersionId() {
        testSession.getContent(EXISTING_PID, "DS1", null);
    }

    @Test (expected=NotFoundException.class)
    public void getContentObjectNotFound() {
        testSession.getContent(EXISTING_PID, "DS1", "DS1.0");
    }

    @Test (expected=NotFoundException.class)
    public void getContentDatastreamNotFound() {
        testSession.addObject(new FedoraObject().pid(EXISTING_PID));
        testSession.getContent(EXISTING_PID, "DS1", "DS1.0");
    }

    @Test (expected=NotFoundException.class)
    public void getContentDatastreamExistsContentNotFound() {
        addObjectWithDS1(true);
        testSession.getContent(EXISTING_PID, "DS1", "DS1.0");
    }

    @Test
    public void getContentDatastreamExistsContentExistsTwice() throws Exception {
        addObjectWithDS1(true);
        testSession.setContent(EXISTING_PID, "DS1", "DS1.0",
                IOUtils.toInputStream("value"));
        Assert.assertEquals("value", IOUtils.toString(
                testSession.getContent(EXISTING_PID, "DS1", "DS1.0")));
        Assert.assertEquals("value", IOUtils.toString(
                testSession.getContent(EXISTING_PID, "DS1", "DS1.0")));
    }

    @Test (expected=IllegalStateException.class)
    public void getContentAfterClose() {
        testSession.close();
        testSession.getContent(EXISTING_PID, "DS1", "DS1.0");
    }

    @Test (expected=NullPointerException.class)
    public void getContentLengthNullPid() {
        testSession.getContentLength(null, "DS1", "DS1.0");
    }

    @Test (expected=NullPointerException.class)
    public void getContentLengthNullDatastreamId() {
        testSession.getContentLength(EXISTING_PID, null, "DS1.0");
    }

    @Test (expected=NullPointerException.class)
    public void getContentLengthNullDatastreamVersionId() {
        testSession.getContentLength(EXISTING_PID, "DS1", null);
    }

    @Test (expected=NotFoundException.class)
    public void getContentLengthObjectNotFound() {
        testSession.getContentLength(EXISTING_PID, "DS1", "DS1.0");
    }

    @Test (expected=NotFoundException.class)
    public void getContentLengthDatastreamNotFound() {
        testSession.addObject(new FedoraObject().pid(EXISTING_PID));
        testSession.getContentLength(EXISTING_PID, "DS1", "DS1.0");
    }

    @Test (expected=NotFoundException.class)
    public void getContentLengthDatastreamExistsContentNotFound() {
        addObjectWithDS1(true);
        testSession.getContentLength(EXISTING_PID, "DS1", "DS1.0");
    }

    @Test
    public void getContentLengthDatastreamExistsContentFoundTwice() throws Exception {
        addObjectWithDS1(true);
        testSession.setContent(EXISTING_PID, "DS1", "DS1.0",
                IOUtils.toInputStream("value"));
        Assert.assertEquals(5L, testSession.getContentLength(EXISTING_PID,
                "DS1", "DS1.0"));
        Assert.assertEquals(5L, testSession.getContentLength(EXISTING_PID,
                "DS1", "DS1.0"));
    }

    @Test (expected=IllegalStateException.class)
    public void getContentLengthAfterClose() {
        testSession.close();
        testSession.getContentLength(EXISTING_PID, "DS1", "DS1.0");
    }

    @Test (expected=NullPointerException.class)
    public void setContentNullPid() {
        testSession.setContent(null, "DS1", "DS1.0",
                IOUtils.toInputStream("value"));
    }

    @Test (expected=NullPointerException.class)
    public void setContentNullDatastreamId() {
        testSession.setContent(EXISTING_PID, null, "DS1.0",
                IOUtils.toInputStream("value"));
    }

    @Test (expected=NullPointerException.class)
    public void setContentNullDatastreamVersionId() {
        testSession.setContent(EXISTING_PID, "DS1", null,
                IOUtils.toInputStream("value"));
    }

    @Test (expected=NullPointerException.class)
    public void setContentNullInputStream() {
        testSession.setContent(EXISTING_PID, null, "DS1.0", null);
    }

    @Test (expected=NotFoundException.class)
    public void setContentObjectNotFound() {
        testSession.setContent(EXISTING_PID, "DS1", "DS1.0",
                IOUtils.toInputStream("value"));
    }

    @Test (expected=NotFoundException.class)
    public void setContentDatastreamNotFound() {
        testSession.addObject(new FedoraObject().pid(EXISTING_PID));
        testSession.setContent(EXISTING_PID, "DS1", "DS1.0",
                IOUtils.toInputStream("value"));
    }

    @Test (expected=NotFoundException.class)
    public void setContentDatastreamNotManaged() {
        addObjectWithDS1(false);
        testSession.setContent(EXISTING_PID, "DS1", "DS1.0",
                IOUtils.toInputStream("value"));
    }

    @Test
    public void setContentDatastreamManagedTwice() throws Exception {
        addObjectWithDS1(true);
        testSession.setContent(EXISTING_PID, "DS1", "DS1.0",
                IOUtils.toInputStream("value1"));
        Assert.assertEquals("value1", IOUtils.toString(
                testSession.getContent(EXISTING_PID, "DS1", "DS1.0")));
        testSession.setContent(EXISTING_PID, "DS1", "DS1.0",
                IOUtils.toInputStream("value2"));
        Assert.assertEquals("value2", IOUtils.toString(
                testSession.getContent(EXISTING_PID, "DS1", "DS1.0")));
    }

    @Test (expected=IllegalStateException.class)
    public void setContentAfterClose() throws Exception {
        testSession.close();
        testSession.setContent(EXISTING_PID, "DS1", "DS1.0",
                IOUtils.toInputStream("value1"));
    }

    private Set<FedoraObject> listObjects() {
        Set<FedoraObject> set = new HashSet<FedoraObject>();
        for (FedoraObject object : testSession) {
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
        testSession.addObject(object);
    }

    private void addObjectWithDS1andDS2() {
        FedoraObject object = new FedoraObject()
                .pid(EXISTING_PID)
                .putDatastream(getManagedDatastreamWithOneVersion("DS1"))
                .putDatastream(getManagedDatastreamWithOneVersion("DS2"));
        testSession.addObject(object);
    }

    private void updateObjectDropDS1() throws Exception {
        Assert.assertEquals(2, testSession.getObject(EXISTING_PID)
                .datastreams().size());
        testSession.updateObject(new FedoraObject()
                .pid(EXISTING_PID)
                .putDatastream(getManagedDatastreamWithOneVersion("DS2")));
        Assert.assertEquals(1, testSession.getObject(EXISTING_PID)
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
