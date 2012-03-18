package com.github.cwilper.fcrepo.store.util.filters;

import com.github.cwilper.fcrepo.dto.core.Datastream;
import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.store.util.commands.CommandContext;
import com.github.cwilper.ttff.AbstractFilter;
import com.github.cwilper.ttff.Filter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Accepts all objects, filtering datastreams with the given datastream filter.
 */
public class FilterDatastreams extends AbstractFilter<FedoraObject> {
    private final Filter<Datastream> filter;
    
    public FilterDatastreams(Filter<Datastream> filter) {
        this.filter = filter;
    }

    @Override
    public FedoraObject accept(FedoraObject object) throws IOException {
        CommandContext.setObject(object); // make available to filter
        Iterator<Datastream> iterator = object.datastreams().values().iterator();
        Set<Datastream> updatedDatastreams = new HashSet<Datastream>();
        while (iterator.hasNext()) {
            Datastream result = filter.accept(iterator.next());
            if (result == null) {
                iterator.remove();
            } else {
                updatedDatastreams.add(result);
            }
        }
        for (Datastream datastream : updatedDatastreams) {
            object.putDatastream(datastream);
        }
        return object;
    }
}
