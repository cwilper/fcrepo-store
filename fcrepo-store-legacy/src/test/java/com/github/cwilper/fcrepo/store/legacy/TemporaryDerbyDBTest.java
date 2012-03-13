package com.github.cwilper.fcrepo.store.legacy;

import org.junit.Test;

/**
 * Units tests for {@link TemporaryDerbyDB}.
 */
public class TemporaryDerbyDBTest {
    @Test
    public void createAndDelete() {
        TemporaryDerbyDB db = new TemporaryDerbyDB();
        db.delete();
    }
}
