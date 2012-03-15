package com.github.cwilper.fcrepo.store.util.commands;

import com.github.cwilper.fcrepo.dto.core.ControlGroup;
import com.github.cwilper.fcrepo.dto.core.Datastream;
import com.github.cwilper.fcrepo.dto.core.DatastreamVersion;
import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.store.core.ExistsException;
import com.github.cwilper.fcrepo.store.core.FedoraStore;
import com.github.cwilper.fcrepo.store.util.PIDSpec;
import com.github.cwilper.ttff.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * Copies {@link FedoraObject}s from one {@link FedoraStore} to another.
 */
public class CopyCommand extends FilteringBatchObjectCommand {
    private static final Logger logger =
            LoggerFactory.getLogger(CopyCommand.class);

    private final FedoraStore destination;
    private final boolean withContent;
    private final boolean overwrite;
    
    public CopyCommand(FedoraStore source, PIDSpec pids,
            Filter<FedoraObject> filter, boolean withContent,
            boolean overwrite, FedoraStore destination) {
        super(source, pids, filter);
        this.withContent = withContent;
        this.overwrite = overwrite;
        this.destination = destination;
    }

    @Override
    protected void handleFilteredObject(FedoraObject object) {
        try {
            destination.addObject(object);
            logger.info("Copied object {}", object.pid());
        } catch (ExistsException e) {
            if (overwrite) {
                destination.updateObject(object);
                logger.info("Replaced object {}",
                        object.pid());
            } else {
                logger.info("Skipped object {} (exists in destination)",
                        object.pid());
                return;
            }
        }
        if (withContent) {
            for (Datastream datastream : object.datastreams().values()) {
                if (datastream.controlGroup() == ControlGroup.MANAGED) {
                    for (DatastreamVersion version : datastream.versions()) {
                        InputStream content = source.getContent(
                                object.pid(), datastream.id(), version.id());
                        String info = object.pid() + "/" + datastream.id() +
                                "/" + version.id();
                        if (content != null) {
                            destination.setContent(
                                    object.pid(), datastream.id(),
                                    version.id(), content);
                            logger.info("Copied content {}", info);
                        }
                    }
                }
            }
        }
    }
}
