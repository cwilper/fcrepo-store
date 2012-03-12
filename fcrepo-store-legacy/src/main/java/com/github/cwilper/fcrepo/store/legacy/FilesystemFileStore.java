package com.github.cwilper.fcrepo.store.legacy;

import com.github.cwilper.fcrepo.store.core.StoreException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

/**
 * A {@link FileStore} that works on the local filesystem.
 */
public class FilesystemFileStore extends AbstractFileStore {
    private final File baseDir;
    
    public FilesystemFileStore(PathRegistry pathRegistry,
            PathAlgorithm pathAlgorithm, String basePath) {
        super(pathRegistry, pathAlgorithm);
        baseDir = new File(basePath);
        if (!baseDir.exists() && !baseDir.mkdirs()) {
            throw new StoreException("Unable to create directory: " + baseDir);
        }
    }

    @Override
    public OutputStream getFileOutputStream(String path) throws IOException {
        return new FileOutputStream(getFile(path));
    }

    @Override
    public long getFileSize(String path) throws IOException {
        return getFile(path).length();
    }

    @Override
    public InputStream getFileInputStream(String path) throws IOException {
        return new FileInputStream(getFile(path));
    }

    @Override
    public void deleteFile(String path) throws IOException {
        File file = getFile(path);
        if (!file.exists()) {
            throw new FileNotFoundException("No such file: " + file);
        }
        if (!file.delete()) {
            throw new IOException("Unable to delete file: " + file);
        }
    }

    @Override
    public Iterator<String> iterator() {
        return new FilesystemPathIterator(baseDir);
    }
    
    private File getFile(String path) {
        return new File(baseDir, path);
    }
}
