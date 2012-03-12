package com.github.cwilper.fcrepo.store.legacy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Provides read/write/iterate access to the filesystem.
 */
public interface FileStore extends Iterable<String>, PathAlgorithm,
        PathRegistry {
    OutputStream getFileOutputStream(String path) throws IOException;
    long getFileSize(String path) throws IOException;
    InputStream getFileInputStream(String path) throws IOException;
    void deleteFile(String path) throws IOException;
    void populateRegistry() throws IOException;
}
