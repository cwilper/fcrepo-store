package com.github.cwilper.fcrepo.store.util.filters;

import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.store.util.PIDSpec;
import com.github.cwilper.ttff.AbstractFilter;

import java.io.IOException;
import java.util.Set;

/**
 * Omits a given set of objects.
 */
public class ExcludeByPid extends AbstractFilter<FedoraObject> {
    private final Set<String> pids;

    public ExcludeByPid(PIDSpec pids) {
        this(pids.toSet());
    }
    
    public ExcludeByPid(Set<String> pids) {
        this.pids = pids;
    }

    @Override
    public FedoraObject accept(FedoraObject object) throws IOException {
        if (pids.contains(object.pid())) {
            return object;
        } else {
            return null;
        }
    }
}
