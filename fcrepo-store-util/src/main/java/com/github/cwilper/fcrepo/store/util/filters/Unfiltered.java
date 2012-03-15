package com.github.cwilper.fcrepo.store.util.filters;

import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.ttff.AbstractFilter;

/**
 * A no-op filter that just passes everything through.
 */
public class Unfiltered extends AbstractFilter<FedoraObject> {
    @Override
    public FedoraObject accept(FedoraObject object) {
        return object;
    }
}
