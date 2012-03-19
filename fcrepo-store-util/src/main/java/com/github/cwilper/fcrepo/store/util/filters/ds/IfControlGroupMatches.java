package com.github.cwilper.fcrepo.store.util.filters.ds;

import com.github.cwilper.fcrepo.dto.core.Datastream;
import com.github.cwilper.fcrepo.store.util.IdSpec;
import com.github.cwilper.ttff.AbstractFilter;

/**
 * Includes datastreams whose control groups match the given idspec.
 */
public class IfControlGroupMatches extends AbstractFilter<Datastream> {
    private final IdSpec ids;

    public IfControlGroupMatches(IdSpec ids) {
        this.ids = ids;
    }

    @Override
    public Datastream accept(Datastream datastream) {
        if (ids.matches(datastream.controlGroup().shortName())) {
            return datastream;
        }
        return null;
    }
}
