package com.github.cwilper.fcrepo.store.legacy;

/**
 * Allocates relative paths for storing objects and managed datastreams.
 */
public interface PathAlgorithm {
    /**
     * Gets the path to use for storing an object or datastream.
     *
     * @param id the pid or pid "+" datastreamId "+" datastreamVersionId.
     * @return the path to use for storing it
     */
    String generatePath(String id);

    /**
     * Gets the original id from a given path.
     *
     * @param path the path previously provided by generatePath.
     * @return the original pid or pid "+" datastreamId "+" datastreamVersionId
     */
    String getId(String path);
}
