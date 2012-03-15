package com.github.cwilper.fcrepo.store.util;

import com.github.cwilper.fcrepo.store.core.NotFoundException;
import com.github.cwilper.fcrepo.store.core.StoreException;
import com.google.common.collect.AbstractIterator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Specifies "all" or a finite set of Fedora objects.
 */
public class PIDSpec implements Iterable<String> {
    private final String stringValue;

    /**
     * Creates an instance.
     *
     * @param stringValue a comma-separated list of pids, the path to a file
     *                    containing a list of pids, one per line, the
     *                    string "all" or <code>null</code>, meaning that
     *                    this PIDSpec matches all pids.
     */
    public PIDSpec(String stringValue) {
        this.stringValue = stringValue;
    }

    /**
     * Tells whether this PIDSpec specifies all pids. If <code>true</code>,
     * calls to {@link #iterator()} will fail with an
     * <code>UnsupportedOperationException</code>.
     *
     * @return whether it specifies all pids.
     */
    public boolean all() {
        return stringValue == null || stringValue.equals("all");
    }

    /**
     * Gets the pids as a set (memory-bound), or <code>null</code> if
     * this PIDSpec matches all pids.
     *
     * @return the set, or <code>null</code> if this PIDSpec matches all pids.
     */
    public Set<String> toSet() {
        if (all()) return null;
        Set<String> set = new HashSet<String>();
        for (String pid : this) {
            set.add(pid);
        }
        return set;
    }

    @Override
    public Iterator<String> iterator() {
        if (all()) throw new UnsupportedOperationException();
        if (stringValue.contains(":")) {
            String[] pids = stringValue.split(",");
            return Arrays.asList(pids).iterator();
        } else {
            File file = new File(stringValue);
            final BufferedReader reader;
            try {
                reader = new BufferedReader(new InputStreamReader(
                        new FileInputStream(file), "UTF-8"));
                return new AbstractIterator<String>() {
                    @Override
                    protected String computeNext() {
                        try {
                            String line = reader.readLine();
                            while (line != null) {
                                line = line.trim();
                                if (line.contains(":")) {
                                    return line;
                                }
                                line = reader.readLine();
                            }
                            reader.close();
                            return endOfData();
                        } catch (IOException e) {
                            throw new StoreException(
                                    "Error reading pid file", e);
                        }
                    }
                };
            } catch (UnsupportedEncodingException wontHappen) {
                throw new RuntimeException(wontHappen);
            } catch (FileNotFoundException e) {
                throw new NotFoundException("No such pid file: " + file);
            }
        }
    }
}
