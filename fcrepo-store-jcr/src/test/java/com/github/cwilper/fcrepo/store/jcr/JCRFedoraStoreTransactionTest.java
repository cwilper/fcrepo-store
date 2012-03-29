package com.github.cwilper.fcrepo.store.jcr;

import com.atomikos.icatch.jta.UserTransactionManager;
import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.dto.foxml.FOXMLReader;
import com.github.cwilper.fcrepo.dto.foxml.FOXMLWriter;
import com.github.cwilper.fcrepo.store.core.ExistsException;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.TransientRepository;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.transaction.support.TransactionTemplate;

import javax.jcr.Credentials;
import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.transaction.Transaction;
import java.io.File;
import java.util.HashSet;
import java.util.Set;

/**
 * Transaction tests for <code>JCRFedoraStore/Session</code>.
 */
public class JCRFedoraStoreTransactionTest {
    private static final String EXISTING_PID = "test:existing";

    private static File tempDir;
    private static TransientRepository repository;
    private static Credentials credentials;
    private static Session jcrSession;
    private static JCRFedoraStoreSession fedoraSession;
    private static TransactionTemplate tt;
    private static UserTransactionManager txManager;
    
    @BeforeClass
    public static void setUpClass() throws Exception {
        tempDir = File.createTempFile("fcrepo-session-jcr", null);
        tempDir.delete();
        repository = new TransientRepository(tempDir);
        credentials = new SimpleCredentials(
                "admin", "admin".toCharArray());
        txManager = new UserTransactionManager();
        txManager.init();
        jcrSession = repository.login(credentials);
        fedoraSession = new JCRFedoraStoreSession(jcrSession,
                new FOXMLReader(), new FOXMLWriter());
    }
    
    @After
    public void tearDown() throws Exception {
        try { jcrSession.removeItem("/08/cf/test_o1"); } catch (Exception e) { }
        try { jcrSession.removeItem("/26/b6/test_o2"); } catch (Exception e) { }
        jcrSession.save();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        fedoraSession.close();
        repository.shutdown();
        FileUtils.deleteDirectory(tempDir);
        txManager.close();
    }

    @Test
    public void jcrImplSupportsTransactions() {
        Assert.assertEquals("true", repository.getDescriptor(
                Repository.OPTION_TRANSACTIONS_SUPPORTED));
    }

    @Test
    public void addMultipleObjectsInTransaction() throws Exception {
        Assert.assertEquals(0, listObjects().size());
        txManager.begin();
        Transaction tx = txManager.getTransaction();
        boolean success = false;
        try {
            fedoraSession.addObject(new FedoraObject().pid("test:o1"));
            fedoraSession.addObject(new FedoraObject().pid("test:o2"));
            // should get here
            tx.commit();
            success = true;
            // both adds succeeded and committed
            Assert.assertEquals(2, listObjects().size());
        } finally {
            if (!success) {
                tx.rollback();
                Assert.fail("Should have succeeded");
            }
        }
    }

    @Test (expected=ExistsException.class)
    public void addSameObjectTwiceInTransaction()
            throws Exception {
        Assert.assertEquals(0, listObjects().size());
        txManager.begin();
        Transaction tx = txManager.getTransaction();
        tx.enlistResource(fedoraSession.getXAResource());
        boolean success = false;
        try {
            fedoraSession.addObject(new FedoraObject().pid("test:o1"));
            fedoraSession.addObject(new FedoraObject().pid("test:o1"));
            // shouldn't get here
            tx.commit();
            success = true;
        } finally {
            Assert.assertFalse(success);
            // first add should have succeeded
            Assert.assertEquals(1, listObjects().size());
            tx.rollback();
            // rollback restores the initial state
            Assert.assertEquals(0, listObjects().size());
        }
    }

    @Test (expected=ExistsException.class)
    public void addSameObjectTwiceNoTransaction() {
        Assert.assertFalse(fedoraSession.iterator().hasNext());
        fedoraSession.addObject(new FedoraObject().pid("test:o1"));
        try {
            fedoraSession.addObject(new FedoraObject().pid("test:o1"));
        } finally {
            // first add should have succeeded and saved the object
            Assert.assertTrue(fedoraSession.iterator().hasNext());
        }
    }

    private Set<FedoraObject> listObjects() {
        Set<FedoraObject> set = new HashSet<FedoraObject>();
        for (FedoraObject object : fedoraSession) {
            set.add(object);
        }
        return set;
    }
}
