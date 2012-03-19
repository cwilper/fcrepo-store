package com.github.cwilper.fcrepo.store.util.filters.ds;

import com.github.cwilper.fcrepo.dto.core.ContentDigest;
import com.github.cwilper.fcrepo.dto.core.ControlGroup;
import com.github.cwilper.fcrepo.dto.core.Datastream;
import com.github.cwilper.fcrepo.dto.core.DatastreamVersion;
import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.dto.core.io.XMLUtil;
import com.github.cwilper.fcrepo.store.core.FedoraStore;
import com.github.cwilper.fcrepo.store.util.commands.CommandContext;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

// also sets size attribute. digest will be re-set if one is already defined.
public class CanonicalizeManagedXML extends MultiVersionFilter {
    private static final Logger logger =
            LoggerFactory.getLogger(CanonicalizeManagedXML.class);

    public CanonicalizeManagedXML(boolean allDatastreamVersions) {
        super(allDatastreamVersions);
    }
    
    @Override
    protected void handleVersion(FedoraObject object, Datastream ds,
            DatastreamVersion dsv) throws IOException {
        FedoraStore destination = CommandContext.getDestination();
        if (destination == null) {
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
                cBytes = XMLUtil.canonicalize(oBytes);
                dsv.size((long) cBytes.length);
                ContentDigest digest = dsv.contentDigest();
                if (digest != null) {
                    digest.hexValue(Util.computeFixity(
                            new ByteArrayInputStream(cBytes),
                            digest.type())[1]);
                }
            } finally {
                IOUtils.closeQuietly(inputStream);
            }
            Util.putObjectIfNoSuchManagedDatastream(object, destination,
                    ds.id());
            destination.setContent(object.pid(), ds.id(),
                    dsv.id(), new ByteArrayInputStream(cBytes));
            logger.info("Canonicalized {}", info);
        }
    }
}
