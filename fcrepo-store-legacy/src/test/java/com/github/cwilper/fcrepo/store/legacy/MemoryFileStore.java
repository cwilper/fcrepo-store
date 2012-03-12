package com.github.cwilper.fcrepo.store.legacy;

import com.github.cwilper.fcrepo.store.core.NotFoundException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Memory-based {@link FileStore} implementation.
 */
public class MemoryFileStore extends AbstractFileStore {
    private final Map<String, byte[]> files = new HashMap<String, byte[]>();

    public MemoryFileStore(PathRegistry pathRegistry,
            PathAlgorithm pathAlgorithm) {
        super(pathRegistry, pathAlgorithm);
    }

    @Override
    public OutputStream getFileOutputStream(final String path) {
        return new ByteArrayOutputStream() {
            @Override
            public void close() {
                files.put(path, toByteArray());
            }
        };
    }

    @Override
    public long getFileSize(String path) {
        return getBytes(path).length;
    }

    @Override
    public InputStream getFileInputStream(String path) {
        return new ByteArrayInputStream(getBytes(path));
    }

    @Override
    public void deleteFile(String path) {
        getBytes(path);
        files.remove(path);
    }

    @Override
    public Iterator<String> iterator() {
        return files.keySet().iterator();
    }
    
    private byte[] getBytes(String path) {
        byte[] bytes = files.get(path);
        if (bytes == null) throw new NotFoundException("No such file: " + path);
        return bytes;
    }
}
