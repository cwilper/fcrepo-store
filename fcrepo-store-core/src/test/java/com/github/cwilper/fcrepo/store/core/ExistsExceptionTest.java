package com.github.cwilper.fcrepo.store.core;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link ExistsException}.
 */
public class ExistsExceptionTest {
    @Test
    public void initWithMessage() {
        ExistsException e = new ExistsException("test");
        Assert.assertEquals("test", e.getMessage());
        Assert.assertNull(e.getCause());
    }

    @Test
    public void initWithMessageAndCause() {
        Exception cause = new Exception();
        ExistsException e = new ExistsException("test", cause);
        Assert.assertEquals("test", e.getMessage());
        Assert.assertEquals(cause, e.getCause());
    }
}
