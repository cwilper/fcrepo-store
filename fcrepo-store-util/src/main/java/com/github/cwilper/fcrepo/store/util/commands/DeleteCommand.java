package com.github.cwilper.fcrepo.store.util.commands;

import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.store.core.FedoraStoreSession;
import com.github.cwilper.fcrepo.store.util.IdSpec;
import com.github.cwilper.ttff.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deletes {@link FedoraObject}s in a given {@link com.github.cwilper.fcrepo.store.core.FedoraStoreSession}.
 */
public class DeleteCommand extends FilteringBatchObjectCommand {
    private static final Logger logger =
            LoggerFactory.getLogger(DeleteCommand.class);

    public DeleteCommand(FedoraStoreSession source, IdSpec pids,
            Filter<FedoraObject> filter) {
        super(source, pids, filter);
    }

    @Override
    protected void handleFilteredObject(FedoraObject object) {
        source.deleteObject(object.pid());
        logger.info("Deleted {}", object.pid());
    }
}
