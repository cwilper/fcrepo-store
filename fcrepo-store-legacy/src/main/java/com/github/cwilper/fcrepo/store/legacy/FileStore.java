package com.github.cwilper.fcrepo.store.legacy;

import com.github.cwilper.fcrepo.store.core.NotFoundException;
import com.github.cwilper.fcrepo.store.core.StoreException;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Provides read/write/iterate access to a set of files organized in
 * directories. All paths are of the form <code>dir/subdir/filename</code>.
 */
public interface FileStore
        extends Iterable<String>, PathAlgorithm, PathRegistry {
    /**
     * Gets an output stream to which content for the given path can be
     * written.
     *
     * @param path the path of the file which may or may not exist yet.
     * @return the output stream.
     * @throws StoreException if any problem occurs.
     */
    OutputStream getFileOutputStream(String path) throws StoreException;

    /**
     * Gets the size of the file at the given path.
     *
     * @param path the path of the file, which must exist.
     * @return the size in bytes.
     * @throws NotFoundException if the file is not found.
     * @throws StoreException if any other problem occurs.
     */
    long getFileSize(String path) throws NotFoundException, StoreException;

    /**
     * Gets an input stream for reading the file at the given path.
     *
     * @param path the path of the file, which must exist.
     * @return the input stream.
     * @throws NotFoundException if the file is not found.
     * @throws StoreException if any other problem occurs.
     */
    InputStream getFileInputStream(String path) throws NotFoundException,
            StoreException;

    /**
     * Deletes the file at the given path.
     *
     * @param path the path of the file, which must exist.
     * @throws NotFoundException if the file is not found.
     * @throws StoreException if any other problem occurs.
     */
    void deleteFile(String path) throws NotFoundException, StoreException;

    /**
     * Iterates the paths of all fiels in this store and adds the id, path
     * pairs to the registry.
     *
     * @throws StoreException if any problem occurs.
     */
    void populateRegistry() throws StoreException;
}
