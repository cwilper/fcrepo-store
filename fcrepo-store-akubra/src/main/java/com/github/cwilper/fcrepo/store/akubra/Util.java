package com.github.cwilper.fcrepo.store.akubra;

import com.github.cwilper.fcrepo.dto.core.ControlGroup;
import com.github.cwilper.fcrepo.dto.core.Datastream;
import com.github.cwilper.fcrepo.dto.core.DatastreamVersion;
import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.dto.core.io.DTOReader;
import com.github.cwilper.fcrepo.dto.core.io.DTOWriter;
import com.github.cwilper.fcrepo.store.core.StoreException;
import org.akubraproject.Blob;
import org.akubraproject.BlobStore;
import org.akubraproject.BlobStoreConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLEncoder;

/** Package-private utility methods. */
final class Util {
    private static final Logger logger =
            LoggerFactory.getLogger(Util.class);

    private Util() { }

    static BlobStoreConnection getConnection(BlobStore blobStore) {
        try {
            return blobStore.openConnection(null, null);
        } catch (IOException e) {
            throw new StoreException(Constants.ERR_OPENING_CONN, e);
        }
    }

    static void writeObject(DTOWriter writerFactory,
            FedoraObject object, Blob blob) throws IOException {
        DTOWriter writer = writerFactory.getInstance();
        OutputStream out = blob.openOutputStream(-1, true);
        boolean success = false;
        try {
            writer.writeObject(object, out);
            out.close();
            success = true;
        } finally {
            writer.close();
            if (!success) closeOrWarn(out);
        }
    }

    static FedoraObject readObject(DTOReader readerFactory,
            Blob blob) throws IOException {
        DTOReader reader = readerFactory.getInstance();
        try {
            return reader.readObject(blob.openInputStream());
        } finally {
            reader.close();
        }
    }

    static String getDetails(String pid,
            String datastreamId, String datastreamVersionId) {
        return "(pid=" + pid + ", datastreamId=" + datastreamId
                + ", datastreamVersionId=" + datastreamVersionId + ")";
    }

    static boolean hasManagedDatastreamVersion(FedoraObject object,
            String datastreamId, String datastreamVersionId) {
        Datastream datastream = object.datastreams().get(datastreamId);
        return datastream != null &&
                datastream.controlGroup() == ControlGroup.MANAGED &&
                hasDatastreamVersion(datastream, datastreamVersionId);
    }

    static boolean hasDatastreamVersion(Datastream datastream,
            String datastreamVersionId) {
        for (DatastreamVersion version : datastream.versions()) {
            if (version.id().equals(datastreamVersionId)) return true;
        }
        return false;
    }

    static void closeOrWarn(Closeable stream) {
        try {
            if (stream != null) stream.close();
        } catch (IOException e) {
            logger.warn(Constants.ERR_CLOSING_STREAM, e);
        }
    }

    static Blob getBlob(BlobStoreConnection connection, String pid)
            throws IOException {
        return connection.getBlob(
                URI.create(Constants.URI_PREFIX + pid), null);
    }

    static Blob getBlob(BlobStoreConnection connection, String pid,
            String datastreamId, String datastreamVersionId)
            throws IOException {
        return connection.getBlob(
                URI.create(Constants.URI_PREFIX + pid + "/"
                + URLEncoder.encode(datastreamId, Constants.CHAR_ENCODING)
                + "/" + URLEncoder.encode(datastreamVersionId,
                Constants.CHAR_ENCODING)), null);
    }
}
