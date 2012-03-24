package com.github.cwilper.fcrepo.store.core;

import com.github.cwilper.fcrepo.dto.core.FedoraObject;

import java.io.InputStream;

/**
 * A place to store Fedora objects and managed content.
 * <p>
 * Implementations should be presumed non-threadsafe unless explicitly stated.
 */
public interface FedoraStoreSession extends Iterable<FedoraObject> {
    /**
     * Adds an object.
     *
     * @param object the object to add, never <code>null</code>.
     * @throws IllegalArgumentException if the object does not specify a pid.
     * @throws NullPointerException if the object is null.
     * @throws ExistsException if an object with that pid already exists.
     * @throws StoreException if there is any other problem.
     */
    void addObject(FedoraObject object);

    /**
     * Gets an object.
     *
     * @param pid the pid of the object to get, never <code>null</code>.
     * @throws NullPointerException if the pid is null.
     * @throws NotFoundException if the object does not exist.
     * @throws StoreException if there is any other problem.
     * @return the object, never <code>null</code>.
     */
    FedoraObject getObject(String pid);

    /**
     * Updates an object. If any formerly-existing managed datastreams are
     * no longer present, their associated content will be automatically
     * deleted.
     *
     * @param object the object to update, never <code>null</code>.
     * @throws IllegalArgumentException if the object does not specify a pid.
     * @throws NullPointerException if the object is null.
     * @throws NotFoundException if the object does not exist.
     * @throws StoreException if there is any other problem.
     */
    void updateObject(FedoraObject object);

    /**
     * Deletes an object. Any formerly-existing managed datastream content
     * will be automatically deleted.
     *
     * @param pid the pid of the object to delete, never <code>null</code>.
     * @throws NullPointerException if the pid is null.
     * @throws NotFoundException if the object does not exist.
     * @throws StoreException if there is any other problem.
     */
    void deleteObject(String pid);

    /**
     * Gets the content of an existing managed datastream.
     *
     * @param pid the pid of the object in which the datastream resides,
     *        never <code>null</code>.
     * @param datastreamId the id of the datastream, never <code>null</code>.
     * @param datastreamVersionId the id of the datastream version,
     *        never <code>null</code>.
     * @throws NullPointerException if any argument is null.
     * @throws NotFoundException if the object, managed datastream, or content
     *         does not exist.
     * @throws StoreException if there is any other problem.
     * @return the content, never <code>null</code>.
     */
    InputStream getContent(String pid, String datastreamId,
            String datastreamVersionId);

    /**
     * Gets the length of the content of an existing managed datastream,
     * in bytes.
     *
     * @param pid the pid of the object in which the datastream resides,
     *        never <code>null</code>.
     * @param datastreamId the id of the datastream, never <code>null</code>.
     * @param datastreamVersionId the id of the datastream version,
     *        never <code>null</code>.
     * @throws NullPointerException if any argument is null.
     * @throws NotFoundException if the object, managed datastream, or content
     *         does not exist.
     * @throws StoreException if there is any other problem.
     * @return the content length in bytes.
     */
    long getContentLength(String pid, String datastreamId,
            String datastreamVersionId);

    /**
     * Sets the content of an existing managed datastream.
     *
     * @param pid the pid of the object in which the datastream resides,
     *        never <code>null</code>.
     * @param datastreamId the id of the datastream, never <code>null</code>.
     * @param datastreamVersionId the id of the datastream version,
     *        never <code>null</code>.
     * @param inputStream the content stream. Guaranteed to be closed by the
     *        time this method returns, regardless of success.
     * @throws NullPointerException if any argument is null.
     * @throws NotFoundException if the object or managed datastream does not
     *         exist.
     * @throws StoreException if there is any other problem.
     */
    void setContent(String pid, String datastreamId,
            String datastreamVersionId, InputStream inputStream);

    /**
     * Closes the session, releasing any resources held. It is safe to
     * call this method multiple times. If an error occurs while closing, it
     * will be logged rather than thrown.
     *
     * After this method is called, subsequent requests to other
     * <code>FedoraStoreSession</code> methods will fail with an
     * {@link IllegalStateException}.
     */
    void close();
}
