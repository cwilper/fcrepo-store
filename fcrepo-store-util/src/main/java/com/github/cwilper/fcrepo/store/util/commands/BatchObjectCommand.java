package com.github.cwilper.fcrepo.store.util.commands;

import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.store.core.FedoraStore;
import com.github.cwilper.fcrepo.store.core.NotFoundException;
import com.github.cwilper.fcrepo.store.util.IdSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for {@link Command}s that work with multiple
 * {@link FedoraObject}s in a {@link FedoraStore}.
 */
public abstract class BatchObjectCommand implements Command {
    private static final Logger logger =
            LoggerFactory.getLogger(BatchObjectCommand.class);

    protected final FedoraStore source;
    protected final IdSpec pids;

    public BatchObjectCommand(FedoraStore source, IdSpec pids) {
        this.source = source;
        this.pids = pids;
    }
    
    @Override
    public void execute() {
        if (pids.isDynamic()) {
            for (FedoraObject object : source) {
                if (pids.matches(object.pid())) {
                    handleObject(object);
                } else {
                    logger.debug("Skipped {} (pid filtered out)",
                            object.pid());
                }
            }
        } else {
            for (String pid : pids) {
                try {
                    handleObject(source.getObject(pid));
                } catch (NotFoundException e) {
                    logger.warn("Skipped {} (does not exist in source)",
                            pid);
                }
            }
        }
    }

    protected abstract void handleObject(FedoraObject object);
}
