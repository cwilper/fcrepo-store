package com.github.cwilper.fcrepo.store.util.filters.ds;

import com.github.cwilper.fcrepo.dto.core.ControlGroup;
import com.github.cwilper.fcrepo.dto.core.Datastream;
import com.github.cwilper.fcrepo.dto.core.DatastreamVersion;
import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.dto.core.io.XMLUtil;
import com.github.cwilper.fcrepo.store.util.commands.CommandContext;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class CanonicalizeManagedXML extends MultiVersionFilter {
    private static final Logger logger =
            LoggerFactory.getLogger(CanonicalizeManagedXML.class);

    public CanonicalizeManagedXML(boolean allDatastreamVersions) {
        super(allDatastreamVersions);
    }

    @Override
    protected void handleVersion(FedoraObject object, Datastream ds,
            DatastreamVersion dsv) throws IOException {
        if (CommandContext.getDestination() == null) {
            throw new UnsupportedOperationException("Filter requires content "
                    + "write access, but this is a read-only command");
        }
        String info = object.pid() + "/" + ds.id() + "/" + dsv.id();
        if (ds.controlGroup() == ControlGroup.MANAGED
                && Util.isXML(dsv.mimeType())) {
            InputStream inputStream = CommandContext.getSource().getContent(
                    object.pid(), ds.id(), dsv.id());
            byte[] cBytes;
            try {
                byte[] oBytes = IOUtils.toByteArray(inputStream);
                logger.info("Orig:\n" + new String(oBytes, "UTF-8"));
                cBytes = XMLUtil.canonicalize(oBytes);
                logger.info("Canonicalized:\n" + new String(cBytes, "UTF-8"));
            } finally {
                IOUtils.closeQuietly(inputStream);
            }
            CommandContext.getDestination().setContent(object.pid(), ds.id(),
                    dsv.id(), new ByteArrayInputStream(cBytes));
            logger.info("Canonicalized {}", info);
        }
    }
}
