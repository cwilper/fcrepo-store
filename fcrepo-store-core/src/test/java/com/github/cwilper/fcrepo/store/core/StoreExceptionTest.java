package com.github.cwilper.fcrepo.store.core;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link StoreException}.
 */
public class StoreExceptionTest {
    @Test
    public void initWithMessage() {
        StoreException e = new StoreException("test");
        Assert.assertEquals("test", e.getMessage());
        Assert.assertNull(e.getCause());
    }

    @Test
    public void initWithMessageAndCause() {
        Exception cause = new Exception();
        StoreException e = new StoreException("test", cause);
        Assert.assertEquals("test", e.getMessage());
        Assert.assertEquals(cause, e.getCause());
    }
}
