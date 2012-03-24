package com.github.cwilper.fcrepo.store.jcr;

import com.github.cwilper.fcrepo.store.core.StoreException;
import com.github.cwilper.fcrepo.store.core.impl.CommonUtil;

import javax.jcr.Credentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

/**
 * Utility methods useful to this implementation.
 */
final class Util extends CommonUtil {
    private Util() { }
    static Session getSession(Repository repository, Credentials credentials) {
        try {
            return repository.login(credentials);
        } catch (RepositoryException e) {
            throw new StoreException("Error connecting to JCR repository", e);
        }
    }
    

}
