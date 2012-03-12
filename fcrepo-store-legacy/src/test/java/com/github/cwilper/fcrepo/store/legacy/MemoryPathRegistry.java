package com.github.cwilper.fcrepo.store.legacy;

import java.util.HashMap;
import java.util.Map;

/**
 * Memory-based implementation of {@link PathRegistry}.
 */
public class MemoryPathRegistry implements PathRegistry {
    private final Map<String, String> map = new HashMap<String, String>();

    @Override
    public long getPathCount() {
        return map.size();
    }

    @Override
    public String getPath(String id) {
        return map.get(id);
    }

    @Override
    public void setPath(String id, String path) {
        if (path == null) {
            map.remove(path);
        } else {
            map.put(id, path);
        }
    }
}
