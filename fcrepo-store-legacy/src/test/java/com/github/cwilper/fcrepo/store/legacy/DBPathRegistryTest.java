package com.github.cwilper.fcrepo.store.legacy;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for {@DBPathRegistry}.
 */
public class DBPathRegistryTest {
    private static final String TABLE = "testTable";
    private static final String ID1 = "id1";
    private static final String ID2 = "id2";
    private static final String PATH1 = "path1";
    private static final String PATH2 = "path2";

    private static TemporaryDerbyDB db;
    private static DBPathRegistry registry;

    @BeforeClass
    public static void setUpClass() {
      db = new TemporaryDerbyDB();
      registry = new DBPathRegistry(db, TABLE);
    }
    
    @Before
    public void setUp() {
        registry.clear();
    }

    @Test (expected=NullPointerException.class)
    public void initWithNullDB() {
        new DBPathRegistry(null, TABLE);
    }

    @Test (expected=NullPointerException.class)
    public void initWithNullTable() {
        new DBPathRegistry(db, null);
    }

    @Test
    public void getInitialPathCount() {
        Assert.assertEquals(0L, registry.getPathCount());
    }

    @Test
    public void setPathExisting() {
        registry.setPath(ID1, PATH1);
        registry.setPath(ID1, PATH2);
        Assert.assertEquals(1L, registry.getPathCount());
        Assert.assertEquals(PATH2, registry.getPath(ID1));
    }

    @Test
    public void setPathNonExisting() {
        registry.setPath(ID1, PATH1);
        Assert.assertEquals(1L, registry.getPathCount());
        Assert.assertEquals(PATH1, registry.getPath(ID1));
    }

    @Test
    public void setPathTwoNonExisting() {
        registry.setPath(ID1, PATH1);
        registry.setPath(ID2, PATH2);
        Assert.assertEquals(2L, registry.getPathCount());
        Assert.assertEquals(PATH1, registry.getPath(ID1));
        Assert.assertEquals(PATH2, registry.getPath(ID2));
    }

    @Test
    public void setPathNullExisting() {
        registry.setPath(ID1, PATH1);
        Assert.assertEquals(1L, registry.getPathCount());
        registry.setPath(ID1, null);
        Assert.assertEquals(0L, registry.getPathCount());
    }

    @Test
    public void setPathNullNonExisting() {
        registry.setPath(ID1, null);
        Assert.assertEquals(0L, registry.getPathCount());
    }

    @Test
    public void getPathNonExisting() {
        Assert.assertNull(registry.getPath(ID1));
    }

    @Test
    public void getPathExisting() {
        registry.setPath(ID1, PATH1);
        Assert.assertEquals(PATH1, registry.getPath(ID1));
    }

    @AfterClass
    public static void tearDownClass() {
        db.delete();
    }
}
