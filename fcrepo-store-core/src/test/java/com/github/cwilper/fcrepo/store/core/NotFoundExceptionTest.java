package com.github.cwilper.fcrepo.store.core;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link NotFoundException}.
 */
public class NotFoundExceptionTest {
    @Test
    public void initWithMessage() {
        NotFoundException e = new NotFoundException("test");
        Assert.assertEquals("test", e.getMessage());
        Assert.assertNull(e.getCause());
    }

    @Test
    public void initWithMessageAndCause() {
        Exception cause = new Exception();
        NotFoundException e = new NotFoundException("test", cause);
        Assert.assertEquals("test", e.getMessage());
        Assert.assertEquals(cause, e.getCause());
    }
}
