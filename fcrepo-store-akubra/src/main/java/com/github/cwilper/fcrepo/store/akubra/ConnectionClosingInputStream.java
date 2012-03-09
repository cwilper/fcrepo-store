package com.github.cwilper.fcrepo.store.akubra;

import org.akubraproject.BlobStoreConnection;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * An {@link InputStream} that automatically closes a given
 * {@link BlobStoreConnection} when finished.
 */
class ConnectionClosingInputStream extends FilterInputStream {
    private final BlobStoreConnection connection;

    /**
     * Creates an instance.
     *
     * @param connection the connection.
     * @param inputStream the stream to wrap.
     */
    ConnectionClosingInputStream(BlobStoreConnection connection,
            InputStream inputStream) {
        super(inputStream);
        this.connection = connection;
    }

    @Override
    public void close() throws IOException {
        if (!connection.isClosed()) {
            try {
                super.close();
            } finally {
                connection.close();
            }
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            super.finalize();
        } finally {
            close();
        }
    }
}

