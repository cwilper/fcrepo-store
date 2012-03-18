package com.github.cwilper.fcrepo.store.util.filters.ds;

import com.github.cwilper.fcrepo.dto.core.ControlGroup;
import com.github.cwilper.fcrepo.dto.core.Datastream;
import com.github.cwilper.fcrepo.dto.core.DatastreamVersion;
import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.dto.core.io.ContentResolver;
import com.github.cwilper.fcrepo.dto.core.io.XMLUtil;
import com.github.cwilper.fcrepo.store.core.StoreException;
import com.github.cwilper.fcrepo.store.util.commands.CommandContext;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class CheckFixity extends MultiVersionFilter {
    private static final Logger logger =
            LoggerFactory.getLogger(CheckFixity.class);

    private final ContentResolver contentResolver;

    public CheckFixity(boolean allDatastreamVersions,
            ContentResolver contentResolver) {
        super(allDatastreamVersions);
        this.contentResolver = contentResolver;
    }

    @Override
    protected void handleVersion(FedoraObject object, Datastream ds,
            DatastreamVersion dsv) {
        String info = object.pid() + "/" + ds.id() + "/" + dsv.id();
        if (dsv.contentDigest() != null || dsv.size() != null) {
            try {
                // some fixity info exists, so compute and compare
                String[] result = computeFixity(object.pid(), ds, dsv);
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
                logger.warn("Error getting content for fixity check of {}; location=" + dsv.contentLocation());
            }
        } else {
            logger.warn("No fixity info (size or digest) for {}", info);
        }
    }
    
    private String[] computeFixity(String pid, Datastream ds,
            DatastreamVersion dsv)
            throws IOException {
        try {
            InputStream inputStream;
            if (ds.controlGroup() == ControlGroup.INLINE_XML) {
                if (!dsv.inlineXML().canonical()) {
                    try {
                        XMLUtil.canonicalize(dsv.inlineXML().bytes());
                    } catch (IOException e) {
                        logger.warn("Unable to canonicalize using C14N11;"
                                + " using non-standard method ("
                                + e.getCause().getMessage() + ")");
                    }
                }
                inputStream = new ByteArrayInputStream(
                        dsv.inlineXML().bytes());
            } else if (ds.controlGroup() == ControlGroup.MANAGED) {
                inputStream = CommandContext.getSource().getContent(pid,
                        ds.id(), dsv.id());
            } else {
                inputStream = contentResolver.resolveContent(null,
                        dsv.contentLocation());
            }
            if (dsv.contentDigest() != null) {
                return computeFixity(inputStream, dsv.contentDigest().type());
            } else {
                return new String[] { "" + computeSize(inputStream), null };
            }
        } catch (StoreException e) {
            throw new IOException(e);
        }
    }
    
    private String[] computeFixity(InputStream inputStream,
            String digestType) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance(digestType);
            byte[] buffer = new byte[4096];
            int count;
            long size = 0;
            while ((count = inputStream.read(buffer)) > 0) {
                size += count;
                digest.update(buffer, 0, count);
            }
            inputStream.close();
            return new String[] { "" + size, hexString(digest.digest()) };
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        }
    }

    static final String HEXES = "0123456789ABCDEF";
    private static String hexString(byte[] bytes) {
        StringBuilder hex = new StringBuilder(2 * bytes.length);
        for (byte b : bytes) {
            hex.append(HEXES.charAt((b & 0xF0) >> 4))
                    .append(HEXES.charAt((b & 0x0F)));
        }
        return hex.toString();
    }
    
    private long computeSize(InputStream inputStream) throws IOException {
        try {
            return IOUtils.skip(inputStream, Long.MAX_VALUE);
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
    }
}
