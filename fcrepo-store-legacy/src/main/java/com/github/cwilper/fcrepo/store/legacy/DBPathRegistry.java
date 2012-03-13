package com.github.cwilper.fcrepo.store.legacy;

import com.github.cwilper.fcrepo.store.core.StoreException;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

/**
 * Database-backed {@link PathRegistry} implementation.
 */
public class DBPathRegistry implements PathRegistry {
    private static final String CREATE_TABLE_DDL =
            "CREATE TABLE ? (\n"
            + "id VARCHAR(256) PRIMARY KEY NOT NULL,\n"
            + "path VARCHAR(4096) NOT NULL)";
    private static final String SELECT_COUNT_SQL =
            "SELECT COUNT(*) FROM ?";
    private static final String SELECT_PATH_SQL =
            "SELECT PATH FROM ? WHERE ID = ?";
    private static final String INSERT_PATH_SQL =
            "INSERT INTO ? (id, path) VALUES (?, ?)";
    private static final String UPDATE_PATH_SQL =
            "UPDATE ? SET path = ? WHERE id = ?";
    private static final String DELETE_BY_ID_SQL =
            "DELETE FROM ? WHERE id = ?";
    private static final String DELETE_ALL_SQL =
            "DELETE FROM ?";
    

    private final JdbcTemplate db;
    private final String table;

    /**
     * Creates an instance.
     *
     * @param db the database to use.
     * @param table the name of the table, which will be created if it doesn't
     *              exist.
     * @throws NullPointerException if either argument is null.
     */
    public DBPathRegistry(JdbcTemplate db, String table) {
        if (db == null || table == null) throw new NullPointerException();
        this.db = db;
        this.table = table;
        createTableIfNeeded();
    }

    private void createTableIfNeeded() {
        try {
            getPathCount();
        } catch (StoreException e) {
            try {
                db.execute(CREATE_TABLE_DDL.replace("?", table));
                getPathCount();
            } catch (Exception e2) {
                throw new StoreException("Error creating table", e2);
            }
        }
    }

    /**
     * Deletes all rows from the table, for testing.
     */
    public void clear() {
        try {
            db.update(DELETE_ALL_SQL.replace("?", table));
        } catch (DataAccessException e) {
            throw new StoreException("Error clearing table", e);
        }
    }

    @Override
    public long getPathCount() {
        try {
            return db.queryForLong(
                    SELECT_COUNT_SQL.replaceFirst("\\?", table));
        } catch (DataAccessException e) {
            throw new StoreException("Error getting path count", e);
        }
    }

    @Override
    public String getPath(String id) {
        try {
            List<String> result = db.queryForList(
                    SELECT_PATH_SQL.replaceFirst("\\?", table),
                    new String[] { id }, String.class);
            if (result.size() == 0) return null;
            return result.get(0);
        } catch (DataAccessException e) {
            throw new StoreException("Error getting path", e);
        }
    }

    @Override
    public void setPath(String id, String path) {
        boolean exists = getPath(id) != null;
        try {
            if (!exists && path != null) {
                db.update(INSERT_PATH_SQL.replaceFirst("\\?", table),
                        id, path);
            } else if (exists) {
                if (path != null) {
                    db.update(UPDATE_PATH_SQL.replaceFirst("\\?", table),
                            path, id);
                } else {
                    db.update(DELETE_BY_ID_SQL.replaceFirst("\\?", table),
                            id);
                }
            }
        } catch (DataAccessException e) {
            throw new StoreException("Error setting path", e);
        }
    }
}
