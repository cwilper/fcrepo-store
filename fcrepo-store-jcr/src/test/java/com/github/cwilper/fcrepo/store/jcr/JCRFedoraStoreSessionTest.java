package com.github.cwilper.fcrepo.store.jcr;

import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.dto.core.io.DTOReader;
import com.github.cwilper.fcrepo.dto.core.io.DTOWriter;
import com.github.cwilper.fcrepo.dto.foxml.FOXMLReader;
import com.github.cwilper.fcrepo.dto.foxml.FOXMLWriter;
import com.github.cwilper.fcrepo.store.core.ExistsException;
import com.github.cwilper.fcrepo.store.core.NotFoundException;
import org.apache.jackrabbit.core.TransientRepository;
import org.easymock.EasyMock;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
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
    private static Credentials credentials;

    private JCRFedoraStoreSession testFedoraStore;

    @BeforeClass
    public static void setUpClass() throws Exception {
        System.out.println(new File(".").getAbsolutePath());
        repository = new TransientRepository(new File(JCR_CONFIG_PATH),
                new File(JCR_REPO_PATH));
        credentials = new SimpleCredentials("admin", "admin".toCharArray());
    }
    
    @Before
    public void setUp() throws Exception {
        testFedoraStore = new JCRFedoraStoreSession(repository, credentials,
                new FOXMLReader(), new FOXMLWriter());
    }

    @Test (expected=NullPointerException.class)
    public void initWithNullRepository() {
        new JCRFedoraStoreSession(null,
                EasyMock.createMock(Credentials.class),
                EasyMock.createMock(DTOReader.class),
                EasyMock.createMock(DTOWriter.class));
    }

    @Test (expected=NullPointerException.class)
    public void initWithNullCredentials() {
        new JCRFedoraStoreSession(repository,
                null,
                EasyMock.createMock(DTOReader.class),
                EasyMock.createMock(DTOWriter.class));
    }

    @Test (expected=NullPointerException.class)
    public void initWithNullReaderFactory() {
        new JCRFedoraStoreSession(repository,
                EasyMock.createMock(Credentials.class),
                null,
                EasyMock.createMock(DTOWriter.class));
    }

    @Test (expected=NullPointerException.class)
    public void initWithNullWriterFactory() {
        new JCRFedoraStoreSession(repository,
                EasyMock.createMock(Credentials.class),
                EasyMock.createMock(DTOReader.class),
                null);
    }

    @Test (expected=NullPointerException.class)
    public void addObjectNull() {
        testFedoraStore.addObject(null);
    }

    @Test (expected=IllegalArgumentException.class)
    public void addObjectNoPid() {
        testFedoraStore.addObject(new FedoraObject());
    }

    @Test (expected= ExistsException.class)
    public void addObjectExisting() throws Exception {
        testFedoraStore.addObject(new FedoraObject().pid("test:new-object"));
        Session session = repository.login(credentials);
        try {
            testFedoraStore.addObject(new FedoraObject().pid("test:new-object"));
        } finally {
            session.removeItem("/ca/88/test_new-object");
            session.save();
            session.logout();
        }
    }

    @Test
    public void addObjectNew() throws Exception {
        testFedoraStore.addObject(new FedoraObject().pid("test:new-object"));
        Session session = repository.login(credentials);
        try {
            Assert.assertTrue(session.nodeExists("/ca/88/test_new-object"));
            session.removeItem("/ca/88/test_new-object");
            session.save();
        } finally {
            session.logout();
        }
    }

    @Test (expected=NullPointerException.class)
    public void getObjectNullPid() {
        testFedoraStore.getObject(null);
    }

    @Test (expected=NotFoundException.class)
    public void getObjectNonExisting() {
        testFedoraStore.getObject("test:non-existing");
    }

    @Test
    public void getObjectExistingTwice() {
        FedoraObject object = new FedoraObject().pid(EXISTING_PID);
        testFedoraStore.addObject(object);
        Assert.assertEquals(object,
                testFedoraStore.getObject(EXISTING_PID));
        Assert.assertEquals(object,
                testFedoraStore.getObject(EXISTING_PID));
    }
    
    @AfterClass
    public static void tearDownClass() throws Exception {
        repository.shutdown();
//        FileUtils.deleteDirectory(tempDir);
    }
}
