package com.github.cwilper.fcrepo.store.legacy;

import com.github.cwilper.fcrepo.store.core.StoreException;

/**
 * Keeps track of the file paths of serialized objects or managed datastream
 * content.
 */
public interface PathRegistry {
    /**
     * Gets the number of paths currently in the registry.
     *
     * @return the number of paths.
     * @throws StoreException if any problem occurs.
     */
    long getPathCount();

    /**
     * Gets the path for the given id.
     *
     * @param id the pid or pid "+" datastreamId "+" datastreamVersionId.
     * @return the path, or <code>null</code> if no such mapping exists.
     * @throws StoreException if any problem occurs.
     */
    String getPath(String id);

    /**
     * Sets the path for the given id.
     *
     * @param id the pid or pid "+" datastreamId "+" datastreamVersionId.
     * @param path the path or <code>null</code> to delete the mapping.
     * @throws StoreException if any problem occurs.
     */
    void setPath(String id, String path);
}
