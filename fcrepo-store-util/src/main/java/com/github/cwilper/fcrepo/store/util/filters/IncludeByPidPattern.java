package com.github.cwilper.fcrepo.store.util.filters;

import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.ttff.AbstractFilter;

import java.io.IOException;

/**
 * Includes objects whose pids match the given regular expression.
 */
public class IncludeByPidPattern extends AbstractFilter<FedoraObject> {
    private final String regex;

    public IncludeByPidPattern(String regex) {
        this.regex = regex;
    }
    @Override
    public FedoraObject accept(FedoraObject object) throws IOException {
        if (object.pid().matches(regex)) {
            return object;
        }
        return null;
    }
}
