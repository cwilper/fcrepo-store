package com.github.cwilper.fcrepo.store.util.commands;

import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.store.core.FedoraStore;
import com.github.cwilper.fcrepo.store.util.IdSpec;
import com.github.cwilper.ttff.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lists each {@link FedoraObject}s in the given {@link FedoraStore}.
 */
public class ListCommand extends FilteringBatchObjectCommand {
    private static final Logger logger =
            LoggerFactory.getLogger(ListCommand.class);

    public ListCommand(FedoraStore source, IdSpec pids,
            Filter<FedoraObject> filter) {
        super(source, pids, filter);
        // if a content-modifying filter is accidently used with this
        // command, prevent it from writing
        CommandContext.setDestination(null);
    }

    @Override
    protected void handleFilteredObject(FedoraObject object) {
        logger.info("Processed {}", object.pid());
    }
}
