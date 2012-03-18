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
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Specifies "all" or a finite set identifiers (ids, datastream ids, etc.)
 * <p>
 * Ids can be specified as a comma-separated list, a file uri pointing
 * to a file containing a list of ids, one per line (blank lines and those
 * beginning with '#' are ignored), the string 'all', or a regular expression
 * if the string starts with '^'.
 */
public class IdSpec implements Iterable<String> {
    private final String stringValue;
    private Set<String> set;

    /**
     * Creates an instance.
     *
     * @param stringValue a comma-separated list, a file uri, the
     *                    string 'all' (or null), or a regular expression.
     */
    public IdSpec(String stringValue) {
        this.stringValue = stringValue;
    }

    /**
     * Tells whether the given id matches this idspec.
     *
     * @param id the id.
     * @return whether it matches.
     */
    public boolean matches(String id) {
        if (stringValue == null || stringValue.equals("all")) {
            return true;
        } else if (stringValue.startsWith("^")) {
            return id.matches(stringValue);
        } else {
            return toSet().contains(id);
        }
    }

    /**
     * Gets the ids as a set in memory, or <code>null</code> if
     * this IdSpec is 'all' or based on a regular expression.
     *
     * @return the set, or <code>null</code> if this IdSpec matches all ids.
     */
    public Set<String> toSet() {
        if (isDynamic()) return null;
        if (set != null) return set;
        set = new HashSet<String>();
        for (String id : this) {
            set.add(id);
        }
        return set;
    }

    public boolean isDynamic() {
        return stringValue == null || stringValue.equals("all") ||
                stringValue.startsWith("^");

    }

    @Override
    public Iterator<String> iterator() {
        if (isDynamic()) throw new UnsupportedOperationException();
        if (!stringValue.startsWith("file:")) {
            String[] ids = stringValue.split(",");
            return Arrays.asList(ids).iterator();
        } else {
            final BufferedReader reader;
            File file = new File(
                    URI.create(stringValue).getRawSchemeSpecificPart());
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
                                if (line.length() > 0 &&
                                        !line.startsWith("#")) {
                                    return line;
                                }
                                line = reader.readLine();
                            }
                            reader.close();
                            return endOfData();
                        } catch (IOException e) {
                            throw new StoreException(
                                    "Error reading file", e);
                        }
                    }
                };
            } catch (UnsupportedEncodingException wontHappen) {
                throw new RuntimeException(wontHappen);
            } catch (FileNotFoundException e) {
                throw new NotFoundException("No such file: " + file);
            }
        }
    }
}
