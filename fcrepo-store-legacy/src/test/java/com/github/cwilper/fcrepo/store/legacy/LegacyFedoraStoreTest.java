package com.github.cwilper.fcrepo.store.legacy;

import com.github.cwilper.fcrepo.dto.core.io.DTOReader;
import com.github.cwilper.fcrepo.dto.core.io.DTOWriter;
import com.github.cwilper.fcrepo.dto.foxml.FOXMLReader;
import com.github.cwilper.fcrepo.dto.foxml.FOXMLWriter;
import com.github.cwilper.fcrepo.store.core.FedoraStore;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link LegacyFedoraStore}.
 */
public class LegacyFedoraStoreTest {
    private FileStore testObjectStore;
    private FileStore testContentStore;

    @Before
    public void setUp() {
        PathAlgorithm alg = new TimestampPathAlgorithm();
        testObjectStore = new MemoryFileStore(new MemoryPathRegistry(), alg);
        testContentStore = new MemoryFileStore(new MemoryPathRegistry(), alg);
    }

    @Test(expected=NullPointerException.class)
    public void initWithNullObjectStore() {
        new LegacyFedoraStore(null,
                EasyMock.createMock(FileStore.class),
                EasyMock.createMock(DTOReader.class),
                EasyMock.createMock(DTOWriter.class));
    }

    @Test (expected=NullPointerException.class)
    public void initWithNullContentStore() {
        new LegacyFedoraStore(EasyMock.createMock(FileStore.class),
                null,
                EasyMock.createMock(DTOReader.class),
                EasyMock.createMock(DTOWriter.class));
    }

    @Test (expected=NullPointerException.class)
    public void initWithNullReaderFactory() {
        new LegacyFedoraStore(EasyMock.createMock(FileStore.class),
                EasyMock.createMock(FileStore.class),
                null,
                EasyMock.createMock(DTOWriter.class));
    }

    @Test (expected=NullPointerException.class)
    public void initWithNullWriterFactory() {
        new LegacyFedoraStore(EasyMock.createMock(FileStore.class),
                EasyMock.createMock(FileStore.class),
                EasyMock.createMock(DTOReader.class),
                null);
    }
    
    @Test
    public void getSession() {
        FedoraStore store = new LegacyFedoraStore(testObjectStore,
                testContentStore, new FOXMLReader(), new FOXMLWriter());
        store.getSession().close();
    }
}
