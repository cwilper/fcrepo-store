package com.github.cwilper.fcrepo.store.legacy;

/**
 * Enter Description.
 */
public interface PathRegistry {
    long getPathCount();

    // return null if not found
    String getPath(String id);

    // if path is null, delete
    void setPath(String id, String path);
}
