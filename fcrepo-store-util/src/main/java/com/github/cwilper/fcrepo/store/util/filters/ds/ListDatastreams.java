package com.github.cwilper.fcrepo.store.util.filters.ds;

import com.github.cwilper.fcrepo.dto.core.Datastream;
import com.github.cwilper.fcrepo.dto.core.DatastreamVersion;
import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.store.util.commands.CommandContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ListDatastreams extends MultiVersionFilter {
    private static final Logger logger =
            LoggerFactory.getLogger(ListDatastreams.class);

    private final boolean includeVersions;

    public ListDatastreams(boolean includeVersions,
            boolean allDatastreamVersions) {
        super(allDatastreamVersions);
        this.includeVersions = includeVersions;
    }

    @Override
    public Datastream accept(Datastream ds) throws IOException {
        String info = CommandContext.getObject().pid() + "/" + ds.id();
        if (includeVersions) {
            super.accept(ds);
        }
        logger.info(info);
        logger.debug(ds.toString());
        return ds;
    }

    @Override
    protected void handleVersion(FedoraObject object, Datastream ds,
            DatastreamVersion dsv) {
        String info = object.pid() + "/" + ds.id() + "/" + dsv.id();
        logger.info(info);
        logger.debug(dsv.toString());
    }
}
