package com.github.cwilper.fcrepo.store.util.commands;

import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.store.core.FedoraStore;
import com.github.cwilper.fcrepo.store.util.PIDSpec;
import com.github.cwilper.ttff.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Base class for {@link BatchObjectCommand}s that also perform filtering.
 */
public abstract class FilteringBatchObjectCommand
        extends BatchObjectCommand {
    private static final Logger logger =
            LoggerFactory.getLogger(FilteringBatchObjectCommand.class);
    protected final Filter<FedoraObject> filter;
    
    public FilteringBatchObjectCommand(FedoraStore source,
            PIDSpec pids, Filter<FedoraObject> filter) {
        super(source, pids);
        this.filter = filter;
    }
    
    @Override
    public void handleObject(FedoraObject object) {
        String pid = object.pid();
        try {
            object = filter.accept(object);
            if (object == null) {
                logger.info("Skipped object {} (filtered out)", pid);
            } else {
                handleFilteredObject(object);
            }
        } catch (IOException e) {
            logger.warn("Skipped object {} (error filtering)" + pid, e);
        }
    }

    protected abstract void handleFilteredObject(FedoraObject object);
}
