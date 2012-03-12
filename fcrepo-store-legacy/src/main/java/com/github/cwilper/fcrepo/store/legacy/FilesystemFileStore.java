package com.github.cwilper.fcrepo.store.legacy;

import com.github.cwilper.fcrepo.store.core.NotFoundException;
import com.github.cwilper.fcrepo.store.core.StoreException;

import java.io.File;
import java.io.FileInputStream;
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

    /**
     * Creates an instance.
     *
     * @param pathRegistry the path registry to use.
     * @param pathAlgorithm the path algorithm to use.
     * @param basePath the base path of the store, which will be created
     *                 if it doesn't exist yet.
     */
    public FilesystemFileStore(PathRegistry pathRegistry,
            PathAlgorithm pathAlgorithm, String basePath) {
        super(pathRegistry, pathAlgorithm);
        baseDir = new File(basePath);
        if (!baseDir.exists() && !baseDir.mkdirs()) {
            throw new StoreException("Unable to create directory: " + baseDir);
        }
    }

    @Override
    public OutputStream getFileOutputStream(String path) {
        try {
            return new FileOutputStream(getFile(path, false));
        } catch (IOException e) {
            throw new StoreException("Error getting output stream", e);
        }
    }

    @Override
    public long getFileSize(String path) {
        return getFile(path, true).length();
    }

    @Override
    public InputStream getFileInputStream(String path) {
        try {
            return new FileInputStream(getFile(path, true));
        } catch (IOException e) {
            throw new StoreException("Error getting input stream", e);
        }
    }

    @Override
    public void deleteFile(String path) {
        if (!getFile(path, true).delete()) {
            throw new StoreException("Unable to delete file: " + path);
        }
    }

    @Override
    public Iterator<String> iterator() {
        return new FilesystemPathIterator(baseDir);
    }
    
    private File getFile(String path, boolean mustExist) {
        File file = new File(baseDir, path);
        if (mustExist && !file.exists()) {
            throw new NotFoundException("No such file: " + path);
        }
        return file;
    }
}
