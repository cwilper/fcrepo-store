package com.github.cwilper.fcrepo.store.legacy;

/**
 * Base implementation of {@link FileStore}.
 */
abstract class AbstractFileStore implements FileStore {
    private final PathRegistry pathRegistry;
    private final PathAlgorithm pathAlgorithm;

    AbstractFileStore(PathRegistry pathRegistry, PathAlgorithm pathAlgorithm) {
        this.pathRegistry = pathRegistry;
        this.pathAlgorithm = pathAlgorithm;
    }

    @Override
    public void populateRegistry() {
        for (String path : this) {
            setPath(getId(path), path);
        }
    }

    @Override
    public long getPathCount() {
        return pathRegistry.getPathCount();
    }

    @Override
    public String getPath(String id) {
        return pathRegistry.getPath(id);
    }

    @Override
    public void setPath(String id, String path) {
        pathRegistry.setPath(id, path);
    }

    @Override
    public String generatePath(String id) {
        return pathAlgorithm.generatePath(id);
    }

    @Override
    public String getId(String path) {
        return pathAlgorithm.getId(path);
    }
}
