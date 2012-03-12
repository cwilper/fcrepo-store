package com.github.cwilper.fcrepo.store.legacy;

import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.dto.core.io.DTOReader;
import com.github.cwilper.fcrepo.dto.core.io.DTOWriter;
import com.github.cwilper.fcrepo.store.core.impl.CommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Utility methods useful to this implementation.
 */
final class Util extends CommonUtil {
    private static final Logger logger =
            LoggerFactory.getLogger(Util.class);

    private Util() { }

    static void writeObject(DTOWriter writerFactory,
            FedoraObject object, OutputStream outputStream)
            throws IOException {
        DTOWriter writer = writerFactory.getInstance();
        boolean success = false;
        try {
            writer.writeObject(object, outputStream);
            outputStream.close();
            success = true;
        } finally {
            writer.close();
            if (!success) closeOrWarn(outputStream);
        }
    }

    static FedoraObject readObject(DTOReader readerFactory,
            InputStream inputStream) throws IOException {
        DTOReader reader = readerFactory.getInstance();
        try {
            return reader.readObject(inputStream);
        } finally {
            reader.close();
        }
    }
    
    static String getId(String pid, String datastreamId,
            String datastreamVersionId) {
        return pid + "+" + datastreamId + "+" + datastreamVersionId;
    }
}
