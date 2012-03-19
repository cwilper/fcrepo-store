package com.github.cwilper.fcrepo.store.util.filters.ds;

import com.github.cwilper.fcrepo.dto.core.ContentDigest;
import com.github.cwilper.fcrepo.dto.core.Datastream;
import com.github.cwilper.fcrepo.dto.core.DatastreamVersion;
import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.dto.core.io.ContentResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SetFixity extends MultiVersionFilter {
    private static final Logger logger =
            LoggerFactory.getLogger(SetFixity.class);

    private final ContentResolver contentResolver;
    private final boolean force;
    private final String algorithm;
    private final String localFedoraServer;

    public SetFixity(boolean allDatastreamVersions,
            ContentResolver contentResolver, String algorithm,
            boolean force, String localFedoraServer) {
        super(allDatastreamVersions);
        this.contentResolver = contentResolver;
        this.algorithm = algorithm.toUpperCase();
        this.force = force;
        this.localFedoraServer = localFedoraServer;
    }

    @Override
    protected void handleVersion(FedoraObject object, Datastream ds,
            DatastreamVersion dsv) {
        String info = object.pid() + "/" + ds.id() + "/" + dsv.id();
        String[] result = null;
        try {
            if (dsv.contentDigest() == null || force) {
                result = Util.computeFixity(Util.getInputStream(
                        info, object.pid(), ds,
                        dsv, contentResolver, localFedoraServer), algorithm);
                if (force || dsv.size() == null) {
                    dsv.size(Long.valueOf(result[0]));
                    logger.debug("Set {} size=" + result[0], info);
                }
                dsv.contentDigest(new ContentDigest()
                        .type(algorithm)
                        .hexValue(result[1]));
                logger.debug("Set {} {}=" + result[1], info, algorithm);
            } else if (dsv.size() == null) {
                dsv.size(Util.computeSize(Util.getInputStream(info,
                        object.pid(), ds, dsv, contentResolver,
                        localFedoraServer)));
                logger.debug("Set {} size=" + dsv.size(), info);
            }
        } catch (IOException e) {
            logger.warn("Error determining fixity of " + info, e);
        }
    }
}
