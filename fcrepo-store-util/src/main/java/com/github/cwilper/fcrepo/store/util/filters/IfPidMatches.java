package com.github.cwilper.fcrepo.store.util.filters;

import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.store.util.IdSpec;
import com.github.cwilper.ttff.AbstractFilter;

import java.io.IOException;

public class IfPidMatches extends AbstractFilter<FedoraObject> {
    private final IdSpec pids;

    public IfPidMatches(IdSpec pids) {
        this.pids = pids;
    }

    @Override
    public FedoraObject accept(FedoraObject object) throws IOException {
        if (pids.matches(object.pid())) {
            return object;
        }
        return null;
    }
}
