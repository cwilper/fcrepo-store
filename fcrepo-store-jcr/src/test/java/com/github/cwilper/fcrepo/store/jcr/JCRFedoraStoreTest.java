package com.github.cwilper.fcrepo.store.jcr;

import com.github.cwilper.fcrepo.dto.core.io.DTOReader;
import com.github.cwilper.fcrepo.dto.core.io.DTOWriter;
import com.github.cwilper.fcrepo.dto.foxml.FOXMLReader;
import com.github.cwilper.fcrepo.dto.foxml.FOXMLWriter;
import com.github.cwilper.fcrepo.store.core.FedoraStore;
import org.easymock.EasyMock;
import org.junit.Test;

import javax.jcr.Credentials;
import javax.jcr.Repository;
import javax.jcr.Session;

/**
 * Unit tests for {@link JCRFedoraStore}.
 */
public class JCRFedoraStoreTest {
    @Test (expected=NullPointerException.class)
    public void initWithNullRepository() throws Exception {
        new JCRFedoraStore(null, EasyMock.createMock(Credentials.class),
                EasyMock.createMock(DTOReader.class),
                EasyMock.createMock(DTOWriter.class));
    }

    @Test (expected=NullPointerException.class)
    public void initWithNullCredentials() throws Exception {
        new JCRFedoraStore(EasyMock.createMock(Repository.class), null,
                EasyMock.createMock(DTOReader.class),
                EasyMock.createMock(DTOWriter.class));
    }

    @Test (expected=NullPointerException.class)
    public void initWithNullReaderFactory() throws Exception {
        new JCRFedoraStore(EasyMock.createMock(Repository.class),
                EasyMock.createMock(Credentials.class),
                null,
                EasyMock.createMock(DTOWriter.class));
    }

    @Test (expected=NullPointerException.class)
    public void initWithNullWriterFactory() throws Exception {
        new JCRFedoraStore(EasyMock.createMock(Repository.class),
                EasyMock.createMock(Credentials.class),
                EasyMock.createMock(DTOReader.class),
                null);
    }
    
    @Test
    public void getSession() throws Exception {
        Repository repository = EasyMock.createMock(Repository.class);
        Credentials credentials = EasyMock.createMock(Credentials.class);
        Session session = EasyMock.createMock(Session.class);
        EasyMock.expect(repository.login(credentials)).andReturn(session);
        EasyMock.replay(repository);
        FedoraStore store = new JCRFedoraStore(repository, credentials,
                new FOXMLReader(), new FOXMLWriter());
        store.getSession().close();
        EasyMock.verify(repository);
    }
}
