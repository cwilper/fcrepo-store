package com.github.cwilper.fcrepo.store.util.filters.ds;

import com.github.cwilper.fcrepo.dto.core.Datastream;
import com.github.cwilper.fcrepo.dto.core.DatastreamVersion;
import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.dto.core.io.ContentResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Computes the fixity (size and/or digest) and compares it to the stored
 * values, reporting on any differences.
 */
public class CheckFixity extends MultiVersionFilter {
    private static final Logger logger =
            LoggerFactory.getLogger(CheckFixity.class);

    private final ContentResolver contentResolver;
    private final String localFedoraServer;

    public CheckFixity(boolean allDatastreamVersions,
            ContentResolver contentResolver, String localFedoraServer) {
        super(allDatastreamVersions);
        this.contentResolver = contentResolver;
        this.localFedoraServer = localFedoraServer;
    }

    @Override
    protected void handleVersion(FedoraObject object, Datastream ds,
            DatastreamVersion dsv) {
        String info = object.pid() + "/" + ds.id() + "/" + dsv.id();
        if (dsv.contentDigest() != null || dsv.size() != null) {
            try {
                // some fixity info exists, so compute and compare
                InputStream inputStream = Util.getInputStream(info,
                        object.pid(), ds, dsv, contentResolver,
                        localFedoraServer);
                String[] result;
                if (dsv.contentDigest() != null) {
                    result = Util.computeFixity(inputStream,
                            dsv.contentDigest().type());
                } else {
                    result = new String[] {
                            "" + Util.computeSize(inputStream), null };
                }
                boolean mismatch = false;
                StringBuilder msg = new StringBuilder();
                if (dsv.size() != null) {
                    msg.append("stated");
                    if (result[0].equals("" + dsv.size())) {
                        msg.append("/actual size=");
                        msg.append(result[0]);
                    } else {
                        mismatch = true;
                        msg.append(" size=");
                        msg.append(dsv.size());
                        msg.append("/actual=");
                        msg.append(result[0]);
                    }
                    if (dsv.contentDigest() != null) msg.append(",");
                }
                if (dsv.contentDigest() != null) {
                    msg.append("stated");
                    if (result[1].equalsIgnoreCase(dsv.contentDigest().hexValue())) {
                        msg.append("/actual ");
                        msg.append(dsv.contentDigest().type());
                        msg.append("=");
                        msg.append(result[1]);
                    } else {
                        mismatch = true;
                        msg.append(" ");
                        msg.append(dsv.contentDigest().type());
                        msg.append("=");
                        msg.append(dsv.size());
                        msg.append("/actual=");
                        msg.append(result[1]);
                    }
                }
                if (mismatch) {
                    logger.warn("Fixity mismatch ({}) for {}", msg.toString(),
                            info);
                } else {
                    logger.info("Fixity match ({}) for {}", msg.toString(), info);
                }
            } catch (IOException e) {
                logger.warn("Error getting content for fixity check of "
                        + info + "; location=" + dsv.contentLocation(), e);
            }
        } else {
            logger.warn("No fixity info (size or digest) for {}", info);
        }
    }
}
