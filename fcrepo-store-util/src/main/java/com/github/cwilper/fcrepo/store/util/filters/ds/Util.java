package com.github.cwilper.fcrepo.store.util.filters.ds;

import com.github.cwilper.fcrepo.dto.core.ControlGroup;
import com.github.cwilper.fcrepo.dto.core.Datastream;
import com.github.cwilper.fcrepo.dto.core.DatastreamVersion;
import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.dto.core.io.ContentResolver;
import com.github.cwilper.fcrepo.dto.core.io.XMLUtil;
import com.github.cwilper.fcrepo.store.core.FedoraStoreSession;
import com.github.cwilper.fcrepo.store.core.NotFoundException;
import com.github.cwilper.fcrepo.store.core.StoreException;
import com.github.cwilper.fcrepo.store.util.commands.CommandContext;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

class Util {
    private static final Logger logger = LoggerFactory.getLogger(Util.class);

    private Util() { }
    
    static boolean isXML(String mimeType) {
        return mimeType != null &&
                (mimeType.endsWith("/xml") || mimeType.endsWith("+xml"));
    }

    static InputStream getInputStream(String info, String pid, Datastream ds,
            DatastreamVersion dsv, ContentResolver contentResolver,
            String localFedoraServer) throws IOException {
        try {
            if (ds.controlGroup() == ControlGroup.INLINE_XML) {
                if (!dsv.inlineXML().canonical()) {
                    try {
                        XMLUtil.canonicalize(dsv.inlineXML().bytes());
                    } catch (IOException e) {
                        logger.warn("Unable to canonicalize {} using C14N11;"
                                + " using non-standard method ("
                                + e.getCause().getMessage() + ")", info);
                    }
                }
                return new ByteArrayInputStream(dsv.inlineXML().bytes());
            } else if (ds.controlGroup() == ControlGroup.MANAGED) {
                return CommandContext.getSource().getContent(pid,
                        ds.id(), dsv.id());
            } else {
                String location = dsv.contentLocation().toString();
                location = location.replace("local.fedora.server",
                        localFedoraServer);
                return contentResolver.resolveContent(null,
                        URI.create(location));
            }
        } catch (StoreException e) {
            throw new IOException(e);
        }
    }

    static String[] computeFixity(InputStream inputStream,
            String algorithm) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance(algorithm);
            byte[] buffer = new byte[4096];
            int count;
            long size = 0;
            while ((count = inputStream.read(buffer)) > 0) {
                size += count;
                digest.update(buffer, 0, count);
            }
            return new String[] { "" + size, hexString(digest.digest()) };
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
    }

    static final String HEXES = "0123456789abcdef";
    static String hexString(byte[] bytes) {
        StringBuilder hex = new StringBuilder(2 * bytes.length);
        for (byte b : bytes) {
            hex.append(HEXES.charAt((b & 0xF0) >> 4))
                    .append(HEXES.charAt((b & 0x0F)));
        }
        return hex.toString();
    }

    static long computeSize(InputStream inputStream)
            throws IOException {
        try {
            return IOUtils.skip(inputStream, Long.MAX_VALUE);
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
    }

    // create or update an existing object so it refers to the given
    // managed datastream -- a prerequisite to putting the content into
    // the store.
    static void putObjectIfNoSuchManagedDatastream(FedoraObject object,
            FedoraStoreSession store, String datastreamId) {
        try {
            FedoraObject existing = store.getObject(object.pid());
            Datastream ds = existing.datastreams().get(datastreamId);
            if (ds == null || ds.controlGroup() != ControlGroup.MANAGED) {
                store.updateObject(object);
            }
        } catch (NotFoundException e) {
            store.addObject(object);
        }
    }
}
