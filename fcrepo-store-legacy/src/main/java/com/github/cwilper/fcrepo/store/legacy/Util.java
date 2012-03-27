package com.github.cwilper.fcrepo.store.legacy;

import com.github.cwilper.fcrepo.store.core.impl.CommonUtil;

/**
 * Utility methods useful to this implementation.
 */
final class Util extends CommonUtil {
    private Util() { }

    static String getId(String pid, String datastreamId,
            String datastreamVersionId) {
        return pid + "+" + datastreamId + "+" + datastreamVersionId;
    }
}
