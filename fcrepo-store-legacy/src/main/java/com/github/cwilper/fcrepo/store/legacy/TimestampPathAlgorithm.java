package com.github.cwilper.fcrepo.store.legacy;

import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * Timestamp-based {@link PathAlgorithm} compatible with Fedora's legacy
 * lowlevel storage path algorithm.
 */
public class TimestampPathAlgorithm implements PathAlgorithm {

    @Override
    public String generatePath(String id) {
        return dateBasedPath(new GregorianCalendar()) + encode(id);
    }

    @Override
    public String getId(String path) {
        return decode(path.substring(path.lastIndexOf("/") + 1));
    }
    
    private static String dateBasedPath(GregorianCalendar date) {
        return date.get(Calendar.YEAR) + "/"
                + pad(2, date.get(Calendar.MONTH))
                + pad(2, date.get(Calendar.DAY_OF_MONTH)) + "/"
                + pad(2, date.get(Calendar.HOUR_OF_DAY)) + "/"
                + pad(2, date.get(Calendar.MINUTE)) + "/";
    }
    
    private static String encode(String in) {
        int i = in.indexOf("+");
        if (i == -1) return pidToFilename(in);
        return pidToFilename(in.substring(0, i)) + in.substring(i);
    }
    
    private static String decode(String in) {
        return null;
    }

    private static String pad(int requiredLength, int in) {
        String out = Integer.toString(in);
        while (out.length() < requiredLength) {
            out = "0" + out;
        }
        return out;
    }
    
    private static String pidToFilename(String pid) {
        String filename = pid.replaceFirst(":", "_");
        if (filename.endsWith(".")) {
            return filename.substring(0, filename.length() - 1) + "%";
        }
        return filename;
    }
    
    private static String filenameToPid(String filename) {
        String pid = filename.replaceFirst("_", ":");
        if (pid.endsWith("%")) {
            return pid.substring(0, pid.length() - 1) + ".";
        }
        return pid;
    }
}
