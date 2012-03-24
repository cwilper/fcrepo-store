package com.github.cwilper.fcrepo.store.jcr;

import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.dto.core.io.DTOReader;
import com.github.cwilper.fcrepo.dto.core.io.DTOWriter;
import com.github.cwilper.fcrepo.dto.foxml.FOXMLReader;
import com.github.cwilper.fcrepo.dto.foxml.FOXMLWriter;
import com.github.cwilper.fcrepo.store.core.ExistsException;
import com.github.cwilper.fcrepo.store.core.NotFoundException;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.TransientRepository;
import org.easymock.EasyMock;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.jcr.Credentials;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import java.io.File;

/**
 * Unit tests for {@link JCRFedoraStoreSession}.
 */
public class JCRFedoraStoreSessionTest {
    private static final String EXISTING_PID = "test:existing";

    private static final String JCR_CONFIG_PATH = "src/test/resources/repository.xml";
    private static final String JCR_REPO_PATH = "target/testdata/jackrabbit";

    private static File tempDir;
    private static TransientRepository repository;
    private static Session jcr;
    private static JCRFedoraStoreSession store;

    @BeforeClass
    public static void setUpClass() throws Exception {
        tempDir = File.createTempFile("fcrepo-store-jcr", null);
        tempDir.delete();
        repository = new TransientRepository(tempDir);
        Credentials credentials = new SimpleCredentials(
                "admin", "admin".toCharArray());
        jcr = repository.login(credentials);
        store = new JCRFedoraStoreSession(jcr, new FOXMLReader(),
                new FOXMLWriter());
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        store.close();
        repository.shutdown();
        FileUtils.deleteDirectory(tempDir);
    }
    
    @Test (expected=NullPointerException.class)
    public void initWithNullSession() {
        new JCRFedoraStoreSession(null,
                EasyMock.createMock(DTOReader.class),
                EasyMock.createMock(DTOWriter.class));
    }

    @Test (expected=NullPointerException.class)
    public void initWithNullReaderFactory() {
        new JCRFedoraStoreSession(EasyMock.createMock(Session.class),
                null, EasyMock.createMock(DTOWriter.class));
    }

    @Test (expected=NullPointerException.class)
    public void initWithNullWriterFactory() {
        new JCRFedoraStoreSession(EasyMock.createMock(Session.class),
                EasyMock.createMock(DTOReader.class), null);
    }

    @Test (expected=NullPointerException.class)
    public void addObjectNull() {
        store.addObject(null);
    }

    @Test (expected=IllegalArgumentException.class)
    public void addObjectNoPid() {
        store.addObject(new FedoraObject());
    }

    @Test (expected= ExistsException.class)
    public void addObjectExisting() throws Exception {
        store.addObject(new FedoraObject().pid("test:new-object"));
        try {
            store.addObject(new FedoraObject().pid("test:new-object"));
        } finally {
            jcr.removeItem("/ca/88/test_new-object");
            jcr.save();
        }
    }

    @Test
    public void addObjectNew() throws Exception {
        store.addObject(new FedoraObject().pid("test:new-object"));
        Assert.assertTrue(jcr.nodeExists("/ca/88/test_new-object"));
        jcr.removeItem("/ca/88/test_new-object");
        jcr.save();
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
        Assert.assertEquals(object, store.getObject(EXISTING_PID));
        Assert.assertEquals(object, store.getObject(EXISTING_PID));
    }
}
