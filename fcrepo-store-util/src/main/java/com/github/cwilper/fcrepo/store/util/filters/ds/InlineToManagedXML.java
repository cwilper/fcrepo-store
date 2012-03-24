package com.github.cwilper.fcrepo.store.util.filters.ds;

import com.github.cwilper.fcrepo.dto.core.ContentDigest;
import com.github.cwilper.fcrepo.dto.core.ControlGroup;
import com.github.cwilper.fcrepo.dto.core.Datastream;
import com.github.cwilper.fcrepo.dto.core.DatastreamVersion;
import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.store.core.FedoraStoreSession;
import com.github.cwilper.fcrepo.store.core.StoreException;
import com.github.cwilper.fcrepo.store.util.commands.CommandContext;
import com.github.cwilper.ttff.AbstractFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;

// also sets size attribute. digest will be re-set if one is already defined.
public class InlineToManagedXML extends AbstractFilter<Datastream> {
    private final static Logger logger =
            LoggerFactory.getLogger(InlineToManagedXML.class);

    @Override
    public Datastream accept(Datastream datastream) throws IOException {
        FedoraStoreSession destination = CommandContext.getDestination();
        if (destination == null) {
            throw new UnsupportedOperationException("Filter requires content "
                    + "write access, but this is a read-only command");
        }
        FedoraObject object = CommandContext.getObject();
        try {
            if (datastream.controlGroup() == ControlGroup.INLINE_XML) {
                datastream.controlGroup(ControlGroup.MANAGED);
                // put object first so destination can accept managed content
                Util.putObjectIfNoSuchManagedDatastream(object, destination,
                        datastream.id());
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

    private void handleVersion(FedoraObject object, Datastream datastream,
            DatastreamVersion datastreamVersion) throws IOException {
        byte[] bytes = datastreamVersion.inlineXML().bytes();
        CommandContext.getDestination().setContent(object.pid(),
                datastream.id(), datastreamVersion.id(),
                new ByteArrayInputStream(bytes));
        datastreamVersion.inlineXML(null);
        datastreamVersion.contentLocation(URI.create(object.pid() + "+" +
                datastream.id() + "+" + datastreamVersion.id()));
        datastreamVersion.size((long) bytes.length);
        ContentDigest digest = datastreamVersion.contentDigest();
        if (digest != null) {
            digest.hexValue(Util.computeFixity(new ByteArrayInputStream(bytes),
                    digest.type())[1]);
        }
        logger.info("Converted {} to managed content", object.pid() + "/" +
                datastream.id() + "/" + datastreamVersion.id());
    }
}
