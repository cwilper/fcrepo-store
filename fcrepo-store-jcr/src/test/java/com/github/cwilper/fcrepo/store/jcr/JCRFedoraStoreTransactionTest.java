package com.github.cwilper.fcrepo.store.jcr;

import com.atomikos.icatch.jta.UserTransactionImp;
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
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.jta.JtaTransactionManager;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

import javax.jcr.Credentials;
import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import java.io.File;

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
        jcrSession = repository.login(credentials);
        fedoraSession = new JCRFedoraStoreSession(jcrSession,
                new FOXMLReader(), new FOXMLWriter());
        tt = new TransactionTemplate(getTxManager());
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
    }

    @Test
    public void supportsTransactions() {
        Assert.assertEquals("true", repository.getDescriptor(
                Repository.OPTION_TRANSACTIONS_SUPPORTED));
    }

    @Test (expected=ExistsException.class)
    public void addSameObjectTwiceInTransactionWithAtomikos()
            throws Exception {
        Assert.assertFalse(fedoraSession.iterator().hasNext());
        txManager.init();
        txManager.begin();
        boolean success = false;
        fedoraSession.addObject(new FedoraObject().pid("test:o1"));
        try {
            fedoraSession.addObject(new FedoraObject().pid("test:o1"));
            txManager.commit();
            success = true;
        } finally {
            Assert.assertFalse(success);
            Assert.assertTrue(fedoraSession.iterator().hasNext());
            txManager.rollback();
            Assert.assertFalse(fedoraSession.iterator().hasNext());
        }
    }
    
    @Test
    public void addSameObjectTwiceInTransactionWithSpring() {
        Assert.assertFalse(fedoraSession.iterator().hasNext());
        try {
            tt.execute(new TransactionCallbackWithoutResult() {
                @Override
                protected void doInTransactionWithoutResult(
                        TransactionStatus transactionStatus) {
                    boolean success = false;
                    try {
                        fedoraSession.addObject(new FedoraObject().pid("test:o1"));
                        fedoraSession.addObject(new FedoraObject().pid("test:o1"));
                        success = true;
                    } finally {
                        if (!success) {
                            System.out.println("Setting rollback only");
                            transactionStatus.setRollbackOnly();
                        }
                    }
                }
            });
            Assert.fail("Should have thrown ExistsException");
        } catch (ExistsException e) {
            Assert.assertFalse("Transaction should have been rolled back!",
                    fedoraSession.iterator().hasNext());
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

    private static JtaTransactionManager getTxManager() {
        // Normally configured via Spring, and often provided by the appserver.
        // But for testing we'll manually configure Atomikos.
        return new JtaTransactionManager(new UserTransactionImp(),
                new UserTransactionManager());
    }
}
