package com.github.cwilper.fcrepo.store.util.filters.ds;

import com.github.cwilper.fcrepo.dto.core.Datastream;
import com.github.cwilper.fcrepo.dto.core.DatastreamVersion;
import com.github.cwilper.ttff.AbstractFilter;

/**
 * Drops all versions of each datastream except the most recent.
 */
public class RemoveOldVersions extends AbstractFilter<Datastream> {
    @Override
    public Datastream accept(Datastream datastream) {
        DatastreamVersion latest = datastream.versions().first();
        datastream.versions().clear();
        datastream.versions().add(latest);
        return datastream;
    }
}
