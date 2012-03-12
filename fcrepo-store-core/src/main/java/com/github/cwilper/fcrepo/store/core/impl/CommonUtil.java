package com.github.cwilper.fcrepo.store.core.impl;

import com.github.cwilper.fcrepo.dto.core.ControlGroup;
import com.github.cwilper.fcrepo.dto.core.Datastream;
import com.github.cwilper.fcrepo.dto.core.DatastreamVersion;
import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Utility methods useful to implementations.
 */
public class CommonUtil {
    private static final Logger logger =
            LoggerFactory.getLogger(CommonUtil.class);

    public static String getDetails(String pid,
            String datastreamId, String datastreamVersionId) {
        return "(pid=" + pid + ", datastreamId=" + datastreamId
                + ", datastreamVersionId=" + datastreamVersionId + ")";
    }

    public static boolean hasManagedDatastreamVersion(FedoraObject object,
            String datastreamId, String datastreamVersionId) {
        Datastream datastream = object.datastreams().get(datastreamId);
        return datastream != null &&
                datastream.controlGroup() == ControlGroup.MANAGED &&
                hasDatastreamVersion(datastream, datastreamVersionId);
    }

    public static boolean hasDatastreamVersion(Datastream datastream,
            String datastreamVersionId) {
        for (DatastreamVersion version : datastream.versions()) {
            if (version.id().equals(datastreamVersionId)) return true;
        }
        return false;
    }

    public static void closeOrWarn(Closeable stream) {
        try {
            if (stream != null) stream.close();
        } catch (IOException e) {
            logger.warn(CommonConstants.ERR_CLOSING_STREAM, e);
        }
    }
}
