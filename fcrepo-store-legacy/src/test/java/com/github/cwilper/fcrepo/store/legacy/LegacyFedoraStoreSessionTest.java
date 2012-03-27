package com.github.cwilper.fcrepo.store.legacy;

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
import org.apache.commons.io.IOUtils;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;

/**
 * Unit tests for {@link LegacyFedoraStoreSession}.
 */
public class LegacyFedoraStoreSessionTest {
    private static final String EXISTING_PATH = "path/to/test_existing";
    private static final String EXISTING_PID = "test:existing";
    private static final String DS1V0_ID = EXISTING_PID + "+DS1+DS1.0";
    private static final String DS2V0_ID = EXISTING_PID + "+DS2+DS2.0";

    private LegacyFedoraStoreSession store;
    private FileStore testObjectStore;
    private FileStore testContentStore;
    
    @Before
    public void setUp() {
        PathAlgorithm alg = new TimestampPathAlgorithm();
        testObjectStore = new MemoryFileStore(new MemoryPathRegistry(), alg);
        testContentStore = new MemoryFileStore(new MemoryPathRegistry(), alg);
        store = new LegacyFedoraStoreSession(testObjectStore,
                testContentStore, new FOXMLReader(), new FOXMLWriter());
    }
    
    @After
    public void tearDown() {
        store.close();
    }
    
    @Test (expected=NullPointerException.class)
    public void initWithNullObjectStore() {
        new LegacyFedoraStoreSession(null,
                EasyMock.createMock(FileStore.class),
                EasyMock.createMock(DTOReader.class),
                EasyMock.createMock(DTOWriter.class));
    }

    @Test (expected=NullPointerException.class)
    public void initWithNullContentStore() {
        new LegacyFedoraStoreSession(EasyMock.createMock(FileStore.class),
                null,
                EasyMock.createMock(DTOReader.class),
                EasyMock.createMock(DTOWriter.class));
    }

    @Test (expected=NullPointerException.class)
    public void initWithNullReaderFactory() {
        new LegacyFedoraStoreSession(EasyMock.createMock(FileStore.class),
                EasyMock.createMock(FileStore.class),
                null,
                EasyMock.createMock(DTOWriter.class));
    }

    @Test (expected=NullPointerException.class)
    public void initWithNullWriterFactory() {
        new LegacyFedoraStoreSession(EasyMock.createMock(FileStore.class),
                EasyMock.createMock(FileStore.class),
                EasyMock.createMock(DTOReader.class),
                null);
    }

    @Test (expected=NullPointerException.class)
    public void addObjectNull() {
        store.addObject(null);
    }

    @Test (expected=IllegalArgumentException.class)
    public void addObjectNoPid() {
        store.addObject(new FedoraObject());
    }

    @Test (expected=ExistsException.class)
    public void addObjectExisting() throws Exception {
        testObjectStore.setPath(EXISTING_PID, EXISTING_PATH);
        OutputStream out = testObjectStore.getFileOutputStream(EXISTING_PATH);
        out.write(0);
        out.close();
        store.addObject(new FedoraObject().pid(EXISTING_PID));
    }
    
    @Test
    public void addObjectNew() throws Exception {
        store.addObject(new FedoraObject().pid(EXISTING_PID));
        String path = testObjectStore.getPath(EXISTING_PID);
        Assert.assertNotNull(path);
        Assert.assertNotNull(testObjectStore.getFileInputStream(path));
    }
    
    @Test (expected=IllegalStateException.class)
    public void addObjectAfterClose() throws Exception {
        store.close();
        store.addObject(new FedoraObject().pid(EXISTING_PID));
    }

    @Test (expected=NullPointerException.class)
    public void getObjectNullPid() {
        store.getObject(null);
    }

    @Test (expected=NotFoundException.class)
    public void getObjectNonExisting() {
        store.getObject("test:non-existing");
    }

    @Test
    public void getObjectExistingTwice() {
        FedoraObject object = new FedoraObject().pid(EXISTING_PID);
        store.addObject(object);
        Assert.assertEquals(object,
                store.getObject(EXISTING_PID));
        Assert.assertEquals(object,
                store.getObject(EXISTING_PID));
    }

    @Test (expected=IllegalStateException.class)
    public void getObjectAfterClose() {
        store.close();
        store.getObject("test:non-existing");
    }

    @Test (expected=NullPointerException.class)
    public void updateObjectNull() {
        store.updateObject(null);
    }
    
    @Test (expected=IllegalArgumentException.class)
    public void updateObjectNoPid() {
        store.updateObject(new FedoraObject());
    }

    @Test (expected=NotFoundException.class)
    public void updateObjectNonExisting() {
        store.updateObject(
                new FedoraObject().pid("test:non-existing"));
    }

    @Test
    public void updateObjectLabel() {
        FedoraObject object = new FedoraObject().pid(EXISTING_PID);
        store.addObject(object);
        Assert.assertNull(store.getObject(EXISTING_PID).label());
        store.updateObject(
                new FedoraObject().pid(EXISTING_PID).label("label"));
        Assert.assertEquals("label",
                store.getObject(EXISTING_PID).label());
    }

    @Test
    public void updateObjectDropExistingContent() throws Exception {
        Assert.assertTrue(fileExists(testContentStore, DS1V0_ID, true));
        Assert.assertTrue(fileExists(testContentStore, DS2V0_ID, true));
        addObjectWithDS1andDS2();
        updateObjectDropDS1();
        Assert.assertFalse(fileExists(testContentStore, DS1V0_ID, false));
        Assert.assertTrue(fileExists(testContentStore, DS2V0_ID, false));
    }

    @Test
    public void updateObjectDropNonExistingContent() throws Exception {
        Assert.assertFalse(fileExists(testContentStore, DS1V0_ID, false));
        Assert.assertTrue(fileExists(testContentStore, DS2V0_ID, true));
        addObjectWithDS1andDS2();
        updateObjectDropDS1();
        Assert.assertFalse(fileExists(testContentStore, DS1V0_ID, false));
        Assert.assertTrue(fileExists(testContentStore, DS2V0_ID, false));
    }

    @Test (expected=IllegalStateException.class)
    public void updateObjectAfterClose() throws Exception {
        store.close();
        updateObjectDropDS1();
    }

    @Test (expected=NullPointerException.class)
    public void deleteObjectNullPid() {
        store.deleteObject(null);
    }

    @Test (expected=NotFoundException.class)
    public void deleteObjectNonExisting() {
        store.deleteObject("test:non-existing");
    }

    @Test
    public void deleteObjectWithContent() throws Exception {
        Assert.assertTrue(fileExists(testContentStore, DS1V0_ID, true));
        Assert.assertTrue(fileExists(testContentStore, DS2V0_ID, true));
        addObjectWithDS1andDS2();
        store.deleteObject(EXISTING_PID);
        Assert.assertFalse(fileExists(testContentStore, DS1V0_ID, false));
        Assert.assertFalse(fileExists(testContentStore, DS2V0_ID, false));
    }

    @Test (expected=IllegalStateException.class)
    public void deleteObjectAfterClose() throws Exception {
        addObjectWithDS1andDS2();
        store.close();
        store.deleteObject(EXISTING_PID);
    }

    @Test
    public void listObjectsEmpty() {
        Assert.assertEquals(0, listObjects().size());
    }

    @Test
    public void listObjectsAfterAdd() {
        FedoraObject o1 = new FedoraObject().pid("test:o1");
        FedoraObject o2 = new FedoraObject().pid("test:o2");
        store.addObject(o1);
        store.addObject(o2);
        Set<FedoraObject> set = listObjects();
        Assert.assertTrue(set.contains(o1));
        Assert.assertTrue(set.contains(o2));
        Assert.assertEquals(2, set.size());
    }

    @Test
    public void listObjectsAfterDelete() {
        FedoraObject o1 = new FedoraObject().pid("test:o1");
        FedoraObject o2 = new FedoraObject().pid("test:o2");
        store.addObject(o1);
        store.addObject(o2);
        store.deleteObject("test:o2");
        Set<FedoraObject> set = listObjects();
        Assert.assertTrue(set.contains(o1));
        Assert.assertEquals(1, set.size());
    }

    @Test (expected=IllegalStateException.class)
    public void listObjectsAfterClose() throws Exception {
        store.close();
        listObjects();
    }

    @Test (expected=NullPointerException.class)
    public void getContentNullPid() {
        store.getContent(null, "DS1", "DS1.0");
    }

    @Test (expected=NullPointerException.class)
    public void getContentNullDatastreamId() {
        store.getContent(EXISTING_PID, null, "DS1.0");
    }

    @Test (expected=NullPointerException.class)
    public void getContentNullDatastreamVersionId() {
        store.getContent(EXISTING_PID, "DS1", null);
    }

    @Test (expected=NotFoundException.class)
    public void getContentObjectNotFound() {
        store.getContent(EXISTING_PID, "DS1", "DS1.0");
    }

    @Test (expected=NotFoundException.class)
    public void getContentDatastreamNotFound() {
        store.addObject(new FedoraObject().pid(EXISTING_PID));
        store.getContent(EXISTING_PID, "DS1", "DS1.0");
    }

    @Test (expected=NotFoundException.class)
    public void getContentDatastreamExistsContentNotFound() {
        addObjectWithDS1(true);
        store.getContent(EXISTING_PID, "DS1", "DS1.0");
    }

    @Test
    public void getContentDatastreamExistsContentExistsTwice() throws Exception {
        addObjectWithDS1(true);
        store.setContent(EXISTING_PID, "DS1", "DS1.0",
                IOUtils.toInputStream("value"));
        Assert.assertEquals("value", IOUtils.toString(
                store.getContent(EXISTING_PID, "DS1", "DS1.0")));
        Assert.assertEquals("value", IOUtils.toString(
                store.getContent(EXISTING_PID, "DS1", "DS1.0")));
    }

    @Test (expected=IllegalStateException.class)
    public void getContentAfterClose() throws Exception {
        store.close();
        store.getContent(EXISTING_PID, "DS1", "DS1.0");
    }

    @Test (expected=NullPointerException.class)
    public void getContentLengthNullPid() {
        store.getContentLength(null, "DS1", "DS1.0");
    }

    @Test (expected=NullPointerException.class)
    public void getContentLengthNullDatastreamId() {
        store.getContentLength(EXISTING_PID, null, "DS1.0");
    }

    @Test (expected=NullPointerException.class)
    public void getContentLengthNullDatastreamVersionId() {
        store.getContentLength(EXISTING_PID, "DS1", null);
    }

    @Test (expected=NotFoundException.class)
    public void getContentLengthObjectNotFound() {
        store.getContentLength(EXISTING_PID, "DS1", "DS1.0");
    }

    @Test (expected=NotFoundException.class)
    public void getContentLengthDatastreamNotFound() {
        store.addObject(new FedoraObject().pid(EXISTING_PID));
        store.getContentLength(EXISTING_PID, "DS1", "DS1.0");
    }

    @Test (expected=NotFoundException.class)
    public void getContentLengthDatastreamExistsContentNotFound() {
        addObjectWithDS1(true);
        store.getContentLength(EXISTING_PID, "DS1", "DS1.0");
    }

    @Test
    public void getContentLengthDatastreamExistsContentFoundTwice() throws Exception {
        addObjectWithDS1(true);
        store.setContent(EXISTING_PID, "DS1", "DS1.0",
                IOUtils.toInputStream("value"));
        Assert.assertEquals(5L, store.getContentLength(EXISTING_PID,
                "DS1", "DS1.0"));
        Assert.assertEquals(5L, store.getContentLength(EXISTING_PID,
                "DS1", "DS1.0"));
    }

    @Test (expected=IllegalStateException.class)
    public void getContentLengthAfterClose() throws Exception {
        store.close();
        store.getContentLength(EXISTING_PID, "DS1", "DS1.0");
    }

    @Test (expected=NullPointerException.class)
    public void setContentNullPid() {
        store.setContent(null, "DS1", "DS1.0",
                IOUtils.toInputStream("value"));
    }

    @Test (expected=NullPointerException.class)
    public void setContentNullDatastreamId() {
        store.setContent(EXISTING_PID, null, "DS1.0",
                IOUtils.toInputStream("value"));
    }

    @Test (expected=NullPointerException.class)
    public void setContentNullDatastreamVersionId() {
        store.setContent(EXISTING_PID, "DS1", null,
                IOUtils.toInputStream("value"));
    }

    @Test (expected=NullPointerException.class)
    public void setContentNullInputStream() {
        store.setContent(EXISTING_PID, null, "DS1.0", null);
    }

    @Test (expected=NotFoundException.class)
    public void setContentObjectNotFound() {
        store.setContent(EXISTING_PID, "DS1", "DS1.0",
                IOUtils.toInputStream("value"));
    }

    @Test (expected=NotFoundException.class)
    public void setContentDatastreamNotFound() {
        store.addObject(new FedoraObject().pid(EXISTING_PID));
        store.setContent(EXISTING_PID, "DS1", "DS1.0",
                IOUtils.toInputStream("value"));
    }

    @Test (expected=NotFoundException.class)
    public void setContentDatastreamNotManaged() {
        addObjectWithDS1(false);
        store.setContent(EXISTING_PID, "DS1", "DS1.0",
                IOUtils.toInputStream("value"));
    }

    @Test
    public void setContentDatastreamManagedTwice() throws Exception {
        addObjectWithDS1(true);
        store.setContent(EXISTING_PID, "DS1", "DS1.0",
                IOUtils.toInputStream("value1"));
        Assert.assertEquals("value1", IOUtils.toString(
                store.getContent(EXISTING_PID, "DS1", "DS1.0")));
        store.setContent(EXISTING_PID, "DS1", "DS1.0",
                IOUtils.toInputStream("value2"));
        Assert.assertEquals("value2", IOUtils.toString(
                store.getContent(EXISTING_PID, "DS1", "DS1.0")));
    }

    @Test (expected=IllegalStateException.class)
    public void setContentAfterClose() throws Exception {
        store.close();
        store.setContent(EXISTING_PID, "DS1", "DS1.0",
                IOUtils.toInputStream("value1"));
    }

    private Set<FedoraObject> listObjects() {
        Set<FedoraObject> set = new HashSet<FedoraObject>();
        for (FedoraObject object : store) {
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
        store.addObject(object);
    }

    private void addObjectWithDS1andDS2() {
        FedoraObject object = new FedoraObject()
                .pid(EXISTING_PID)
                .putDatastream(getManagedDatastreamWithOneVersion("DS1"))
                .putDatastream(getManagedDatastreamWithOneVersion("DS2"));
        store.addObject(object);
    }

    private void updateObjectDropDS1() throws Exception {
        Assert.assertEquals(2, store.getObject(EXISTING_PID)
                .datastreams().size());
        store.updateObject(new FedoraObject()
                .pid(EXISTING_PID)
                .putDatastream(getManagedDatastreamWithOneVersion("DS2")));
        Assert.assertEquals(1, store.getObject(EXISTING_PID)
                .datastreams().size());
    }

    private boolean fileExists(FileStore fileStore, String id, boolean create)
            throws IOException {
        if (create) {
            String path = fileStore.generatePath(id);
            fileStore.setPath(id, path);
            OutputStream out = fileStore.getFileOutputStream(path);
            out.write(0);
            out.close();
        }
        String path = fileStore.getPath(id);
        return path != null && hasInputStream(fileStore, path);
    }
    
    private boolean hasInputStream(FileStore fileStore, String path) {
        try {
            fileStore.getFileInputStream(path);
            return true;
        } catch (NotFoundException e) {
            return false;
        }
    }
}
