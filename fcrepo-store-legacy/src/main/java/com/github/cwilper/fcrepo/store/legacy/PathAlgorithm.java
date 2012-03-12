package com.github.cwilper.fcrepo.store.legacy;

/**
 * Allocates paths for storing objects and managed datastreams.
 */
public interface PathAlgorithm {
    String generatePath(String id);
    String getId(String path);
}
