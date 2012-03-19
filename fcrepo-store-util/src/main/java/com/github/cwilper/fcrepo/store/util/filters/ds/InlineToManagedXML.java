package com.github.cwilper.fcrepo.store.util.filters.ds;

import com.github.cwilper.fcrepo.dto.core.ControlGroup;
import com.github.cwilper.fcrepo.dto.core.Datastream;
import com.github.cwilper.fcrepo.dto.core.DatastreamVersion;
import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.store.core.StoreException;
import com.github.cwilper.fcrepo.store.util.commands.CommandContext;
import com.github.cwilper.ttff.AbstractFilter;

import java.io.IOException;

/**
 * A datastream filter that accepts all versions, but may examine or change
 * any of them.
 */
public abstract class InlineToManagedXML extends AbstractFilter<Datastream> {
    @Override
    public Datastream accept(Datastream datastream) throws IOException {
        FedoraObject object = CommandContext.getObject();
        try {
            if (datastream.controlGroup() == ControlGroup.INLINE_XML) {
                for (DatastreamVersion datastreamVersion :
                        datastream.versions()) {
                    handleVersion(object, datastream, datastreamVersion);
                }
            }
        } catch (StoreException e) {
            throw new IOException(e);
        }
        return datastream;
    }

    protected abstract void handleVersion(FedoraObject object,
            Datastream datastream, DatastreamVersion datastreamVersion)
            throws IOException;
}
